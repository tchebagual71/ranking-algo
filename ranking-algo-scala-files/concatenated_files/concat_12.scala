package com.twitter.cr_mixer.logging

import com.twitter.cr_mixer.model.AdsCandidateGeneratorQuery
import com.twitter.cr_mixer.model.CrCandidateGeneratorQuery
import com.twitter.cr_mixer.model.RelatedTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.UtegTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.thriftscala.Product
import com.twitter.product_mixer.core.thriftscala.ClientContext
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.InternalId

case class ScribeMetadata(
  requestUUID: Long,
  userId: UserId,
  product: Product)

object ScribeMetadata {
  def from(query: CrCandidateGeneratorQuery): ScribeMetadata = {
    ScribeMetadata(query.requestUUID, query.userId, query.product)
  }

  def from(query: UtegTweetCandidateGeneratorQuery): ScribeMetadata = {
    ScribeMetadata(query.requestUUID, query.userId, query.product)
  }

  def from(query: AdsCandidateGeneratorQuery): ScribeMetadata = {
    ScribeMetadata(query.requestUUID, query.userId, query.product)
  }
}

case class RelatedTweetScribeMetadata(
  requestUUID: Long,
  internalId: InternalId,
  clientContext: ClientContext,
  product: Product)

object RelatedTweetScribeMetadata {
  def from(query: RelatedTweetCandidateGeneratorQuery): RelatedTweetScribeMetadata = {
    RelatedTweetScribeMetadata(
      query.requestUUID,
      query.internalId,
      query.clientContext,
      query.product)
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.GlobalRequestTimeoutException
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.mux.ServerApplicationError
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.hashing.KeyHasher
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.relevance_platform.common.injection.LZ4Injection
import com.twitter.relevance_platform.common.injection.SeqObjectInjection
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.Params
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.TimeoutException
import com.twitter.util.logging.Logging
import org.apache.thrift.TApplicationException

/**
 * A SimilarityEngine is a wrapper which, given a [[Query]], returns a list of [[Candidate]]
 * The main purposes of a SimilarityEngine is to provide a consistent interface for candidate
 * generation logic, and provides default functions, including:
 * - Identification
 * - Observability
 * - Timeout settings
 * - Exception Handling
 * - Gating by Deciders & FeatureSwitch settings
 * - (coming soon): Dark traffic
 *
 * Note:
 * A SimilarityEngine by itself is NOT meant to be cacheable.
 * Caching should be implemented in the underlying ReadableStore that provides the [[Candidate]]s
 *
 * Please keep extension of this class local this directory only
 *
 */
trait SimilarityEngine[Query, Candidate] {

  /**
   * Uniquely identifies a similarity engine.
   * Avoid using the same engine type for more than one engine, it will cause stats to double count
   */
  private[similarity_engine] def identifier: SimilarityEngineType

  def getCandidates(query: Query): Future[Option[Seq[Candidate]]]

}

object SimilarityEngine extends Logging {
  case class SimilarityEngineConfig(
    timeout: Duration,
    gatingConfig: GatingConfig)

  /**
   * Controls for whether or not this Engine is enabled.
   * In our previous design, we were expecting a Sim Engine will only take one set of Params,
   * and that’s why we decided to have GatingConfig and the EnableFeatureSwitch in the trait.
   * However, we now have two candidate generation pipelines: Tweet Rec, Related Tweets
   * and they are now having their own set of Params, but EnableFeatureSwitch can only put in 1 fixed value.
   * We need some further refactor work to make it more flexible.
   *
   * @param deciderConfig Gate the Engine by a decider. If specified,
   * @param enableFeatureSwitch. DO NOT USE IT FOR NOW. It needs some refactorting. Please set it to None (SD-20268)
   */
  case class GatingConfig(
    deciderConfig: Option[DeciderConfig],
    enableFeatureSwitch: Option[
      FSParam[Boolean]
    ]) // Do NOT use the enableFeatureSwitch. It needs some refactoring.

  case class DeciderConfig(
    decider: CrMixerDecider,
    deciderString: String)

  case class MemCacheConfig[K](
    cacheClient: Client,
    ttl: Duration,
    asyncUpdate: Boolean = false,
    keyToString: K => String)

  private[similarity_engine] def isEnabled(
    params: Params,
    gatingConfig: GatingConfig
  ): Boolean = {
    val enabledByDecider =
      gatingConfig.deciderConfig.forall { config =>
        config.decider.isAvailable(config.deciderString)
      }

    val enabledByFS = gatingConfig.enableFeatureSwitch.forall(params.apply)

    enabledByDecider && enabledByFS
  }

  // Default key hasher for memcache keys
  val keyHasher: KeyHasher = KeyHasher.FNV1A_64

  /**
   * Add a MemCache wrapper to a ReadableStore with a preset key and value injection functions
   * Note: The [[Query]] object needs to be cacheable,
   * i.e. it cannot be a runtime objects or complex objects, for example, configapi.Params
   *
   * @param underlyingStore un-cached store implementation
   * @param keyPrefix       a prefix differentiates 2 stores if they share the same key space.
   *                        e.x. 2 implementations of ReadableStore[UserId, Seq[Candidiate] ]
   *                        can use prefix "store_v1", "store_v2"
   * @return                A ReadableStore with a MemCache wrapper
   */
  private[similarity_engine] def addMemCache[Query, Candidate <: Serializable](
    underlyingStore: ReadableStore[Query, Seq[Candidate]],
    memCacheConfig: MemCacheConfig[Query],
    keyPrefix: Option[String] = None,
    statsReceiver: StatsReceiver
  ): ReadableStore[Query, Seq[Candidate]] = {
    val prefix = keyPrefix.getOrElse("")

    ObservedMemcachedReadableStore.fromCacheClient[Query, Seq[Candidate]](
      backingStore = underlyingStore,
      cacheClient = memCacheConfig.cacheClient,
      ttl = memCacheConfig.ttl,
      asyncUpdate = memCacheConfig.asyncUpdate,
    )(
      valueInjection = LZ4Injection.compose(SeqObjectInjection[Candidate]()),
      keyToString = { k: Query => s"CRMixer:$prefix${memCacheConfig.keyToString(k)}" },
      statsReceiver = statsReceiver
    )
  }

  private val timer = com.twitter.finagle.util.DefaultTimer

  /**
   * Applies runtime configs, like stats, timeouts, exception handling, onto fn
   */
  private[similarity_engine] def getFromFn[Query, Candidate](
    fn: Query => Future[Option[Seq[Candidate]]],
    storeQuery: Query,
    engineConfig: SimilarityEngineConfig,
    params: Params,
    scopedStats: StatsReceiver
  ): Future[Option[Seq[Candidate]]] = {
    if (isEnabled(params, engineConfig.gatingConfig)) {
      scopedStats.counter("gate_enabled").incr()

      StatsUtil
        .trackOptionItemsStats(scopedStats) {
          fn.apply(storeQuery).raiseWithin(engineConfig.timeout)(timer)
        }
        .rescue {
          case _: TimeoutException | _: GlobalRequestTimeoutException | _: TApplicationException |
              _: ClientDiscardedRequestException |
              _: ServerApplicationError // TApplicationException inside
              =>
            debug("Failed to fetch. request aborted or timed out")
            Future.None
          case e =>
            error("Failed to fetch. request aborted or timed out", e)
            Future.None
        }
    } else {
      scopedStats.counter("gate_disabled").incr()
      Future.None
    }
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.base.Stats
import com.twitter.product_mixer.core.thriftscala.ClientContext
import com.twitter.qig_ranker.thriftscala.Product
import com.twitter.qig_ranker.thriftscala.ProductContext
import com.twitter.qig_ranker.thriftscala.QigRanker
import com.twitter.qig_ranker.thriftscala.QigRankerProductResponse
import com.twitter.qig_ranker.thriftscala.QigRankerRequest
import com.twitter.qig_ranker.thriftscala.QigRankerResponse
import com.twitter.qig_ranker.thriftscala.TwistlySimilarTweetsProductContext
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Singleton

/**
 * This store looks for similar tweets from QueryInteractionGraph (QIG) for a source tweet id.
 * For a given query tweet, QIG returns us the similar tweets that have an overlap of engagements
 * (with the query tweet) on different search queries
 */
@Singleton
case class TweetBasedQigSimilarityEngine(
  qigRanker: QigRanker.MethodPerEndpoint,
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      TweetBasedQigSimilarityEngine.Query,
      Seq[TweetWithScore]
    ] {

  private val stats = statsReceiver.scope(this.getClass.getSimpleName)
  private val fetchCandidatesStat = stats.scope("fetchCandidates")

  override def get(
    query: TweetBasedQigSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    query.sourceId match {
      case InternalId.TweetId(tweetId) =>
        val qigSimilarTweetsRequest = getQigSimilarTweetsRequest(tweetId)

        Stats.trackOption(fetchCandidatesStat) {
          qigRanker
            .getSimilarCandidates(qigSimilarTweetsRequest)
            .map { qigSimilarTweetsResponse =>
              getCandidatesFromQigResponse(qigSimilarTweetsResponse)
            }
        }
      case _ =>
        Future.value(None)
    }
  }

  private def getQigSimilarTweetsRequest(
    tweetId: Long
  ): QigRankerRequest = {
    // Note: QigRanker needs a non-empty userId to be passed to return results.
    // We are passing in a dummy userId until we fix this on QigRanker side
    val clientContext = ClientContext(userId = Some(0L))
    val productContext = ProductContext.TwistlySimilarTweetsProductContext(
      TwistlySimilarTweetsProductContext(tweetId = tweetId))

    QigRankerRequest(
      clientContext = clientContext,
      product = Product.TwistlySimilarTweets,
      productContext = Some(productContext),
    )
  }

  private def getCandidatesFromQigResponse(
    qigSimilarTweetsResponse: QigRankerResponse
  ): Option[Seq[TweetWithScore]] = {
    qigSimilarTweetsResponse.productResponse match {
      case QigRankerProductResponse
            .TwistlySimilarTweetCandidatesResponse(response) =>
        val tweetsWithScore = response.similarTweets
          .map { similarTweetResult =>
            TweetWithScore(
              similarTweetResult.tweetResult.tweetId,
              similarTweetResult.tweetResult.score.getOrElse(0L))
          }
        Some(tweetsWithScore)

      case _ => None
    }
  }
}

object TweetBasedQigSimilarityEngine {

  def toSimilarityEngineInfo(score: Double): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.Qig,
      modelId = None,
      score = Some(score))
  }

  case class Query(sourceId: InternalId)

  def fromParams(
    sourceId: InternalId,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    EngineQuery(
      Query(sourceId = sourceId),
      params
    )
  }

}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.simclusters_v2.common.TweetId
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SimilaritySourceOrderingUtil {
  /**
   * This function flatten and dedup input candidates according to the order in the input Seq
   * [[candidate10, candidate11], [candidate20, candidate21]] => [candidate10, candidate11, candidate20, candidate21]
   */
  def keepGivenOrder(
    candidates: Seq[Seq[TweetWithCandidateGenerationInfo]],
  ): Seq[TweetWithCandidateGenerationInfo] = {

    val seen = mutable.Set[TweetId]()
    val combinedCandidates = candidates.flatten
    val result = ArrayBuffer[TweetWithCandidateGenerationInfo]()

    combinedCandidates.foreach { candidate =>
      val candidateTweetId = candidate.tweetId
      val seenCandidate = seen.contains(candidateTweetId) // de-dup
      if (!seenCandidate) {
        result += candidate
        seen.add(candidate.tweetId)
      }
    }
    //convert result to immutable seq
    result.toList
  }
}
package com.twitter.cr_mixer.model

import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId

case class TweetWithAuthor(tweetId: TweetId, authorId: UserId)
package com.twitter.cr_mixer.source_signal

import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.source_signal.SourceFetcher.FetcherQuery
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.util.Future

/***
 * A SourceSignalFetcher is a trait that extends from `SourceFetcher`
 * and is specialized in tackling Signals (eg., USS, FRS) fetch.
 * Currently, we define Signals as (but not limited to) a set of past engagements that
 * the user makes, such as RecentFav, RecentFollow, etc.
 *
 * The [[ResultType]] of a SourceSignalFetcher is `Seq[SourceInfo]`. When we pass in userId,
 * the underlying store returns a list of signals.
 */
trait SourceSignalFetcher extends SourceFetcher[Seq[SourceInfo]] {

  protected type SignalConvertType

  def trackStats(
    query: FetcherQuery
  )(
    func: => Future[Option[Seq[SourceInfo]]]
  ): Future[Option[Seq[SourceInfo]]] = {
    val productScopedStats = stats.scope(query.product.originalName)
    val productUserStateScopedStats = productScopedStats.scope(query.userState.toString)
    StatsUtil
      .trackOptionItemsStats(productScopedStats) {
        StatsUtil
          .trackOptionItemsStats(productUserStateScopedStats) {
            func
          }
      }
  }

  /***
   * Convert a list of Signals of type [[SignalConvertType]] into SourceInfo
   */
  def convertSourceInfo(
    sourceType: SourceType,
    signals: Seq[SignalConvertType]
  ): Seq[SourceInfo]
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType_ModelBased
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType_RecencyBased
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType_TensorflowBased
import com.twitter.cr_mixer.model.TweetWithAuthor
import com.twitter.cr_mixer.param.EarlybirdFrsBasedCandidateGenerationParams
import com.twitter.cr_mixer.param.EarlybirdFrsBasedCandidateGenerationParams.FrsBasedCandidateGenerationEarlybirdSimilarityEngineTypeParam
import com.twitter.cr_mixer.param.FrsParams.FrsBasedCandidateGenerationMaxCandidatesNumParam
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
case class EarlybirdSimilarityEngineRouter @Inject() (
  earlybirdRecencyBasedSimilarityEngine: EarlybirdSimilarityEngine[
    EarlybirdRecencyBasedSimilarityEngine.EarlybirdRecencyBasedSearchQuery,
    EarlybirdRecencyBasedSimilarityEngine
  ],
  earlybirdModelBasedSimilarityEngine: EarlybirdSimilarityEngine[
    EarlybirdModelBasedSimilarityEngine.EarlybirdModelBasedSearchQuery,
    EarlybirdModelBasedSimilarityEngine
  ],
  earlybirdTensorflowBasedSimilarityEngine: EarlybirdSimilarityEngine[
    EarlybirdTensorflowBasedSimilarityEngine.EarlybirdTensorflowBasedSearchQuery,
    EarlybirdTensorflowBasedSimilarityEngine
  ],
  timeoutConfig: TimeoutConfig,
  statsReceiver: StatsReceiver)
    extends ReadableStore[EarlybirdSimilarityEngineRouter.Query, Seq[TweetWithAuthor]] {
  import EarlybirdSimilarityEngineRouter._

  override def get(
    k: EarlybirdSimilarityEngineRouter.Query
  ): Future[Option[Seq[TweetWithAuthor]]] = {
    k.rankingMode match {
      case EarlybirdSimilarityEngineType_RecencyBased =>
        earlybirdRecencyBasedSimilarityEngine.getCandidates(recencyBasedQueryFromParams(k))
      case EarlybirdSimilarityEngineType_ModelBased =>
        earlybirdModelBasedSimilarityEngine.getCandidates(modelBasedQueryFromParams(k))
      case EarlybirdSimilarityEngineType_TensorflowBased =>
        earlybirdTensorflowBasedSimilarityEngine.getCandidates(tensorflowBasedQueryFromParams(k))
    }
  }
}

object EarlybirdSimilarityEngineRouter {
  case class Query(
    searcherUserId: Option[UserId],
    seedUserIds: Seq[UserId],
    maxNumTweets: Int,
    excludedTweetIds: Set[TweetId],
    rankingMode: EarlybirdSimilarityEngineType,
    frsUserToScoresForScoreAdjustment: Option[Map[UserId, Double]],
    maxTweetAge: Duration,
    filterOutRetweetsAndReplies: Boolean,
    params: configapi.Params)

  def queryFromParams(
    searcherUserId: Option[UserId],
    seedUserIds: Seq[UserId],
    excludedTweetIds: Set[TweetId],
    frsUserToScoresForScoreAdjustment: Option[Map[UserId, Double]],
    params: configapi.Params
  ): Query =
    Query(
      searcherUserId,
      seedUserIds,
      maxNumTweets = params(FrsBasedCandidateGenerationMaxCandidatesNumParam),
      excludedTweetIds,
      rankingMode =
        params(FrsBasedCandidateGenerationEarlybirdSimilarityEngineTypeParam).rankingMode,
      frsUserToScoresForScoreAdjustment,
      maxTweetAge = params(
        EarlybirdFrsBasedCandidateGenerationParams.FrsBasedCandidateGenerationEarlybirdMaxTweetAge),
      filterOutRetweetsAndReplies = params(
        EarlybirdFrsBasedCandidateGenerationParams.FrsBasedCandidateGenerationEarlybirdFilterOutRetweetsAndReplies),
      params
    )

  private def recencyBasedQueryFromParams(
    query: Query
  ): EngineQuery[EarlybirdRecencyBasedSimilarityEngine.EarlybirdRecencyBasedSearchQuery] =
    EngineQuery(
      EarlybirdRecencyBasedSimilarityEngine.EarlybirdRecencyBasedSearchQuery(
        seedUserIds = query.seedUserIds,
        maxNumTweets = query.maxNumTweets,
        excludedTweetIds = query.excludedTweetIds,
        maxTweetAge = query.maxTweetAge,
        filterOutRetweetsAndReplies = query.filterOutRetweetsAndReplies
      ),
      query.params
    )

  private def tensorflowBasedQueryFromParams(
    query: Query,
  ): EngineQuery[EarlybirdTensorflowBasedSimilarityEngine.EarlybirdTensorflowBasedSearchQuery] =
    EngineQuery(
      EarlybirdTensorflowBasedSimilarityEngine.EarlybirdTensorflowBasedSearchQuery(
        searcherUserId = query.searcherUserId,
        seedUserIds = query.seedUserIds,
        maxNumTweets = query.maxNumTweets,
        // hard code the params below for now. Will move to FS after shipping the ddg
        beforeTweetIdExclusive = None,
        afterTweetIdExclusive =
          Some(SnowflakeId.firstIdFor((Time.now - query.maxTweetAge).inMilliseconds)),
        filterOutRetweetsAndReplies = query.filterOutRetweetsAndReplies,
        useTensorflowRanking = true,
        excludedTweetIds = query.excludedTweetIds,
        maxNumHitsPerShard = 1000
      ),
      query.params
    )
  private def modelBasedQueryFromParams(
    query: Query,
  ): EngineQuery[EarlybirdModelBasedSimilarityEngine.EarlybirdModelBasedSearchQuery] =
    EngineQuery(
      EarlybirdModelBasedSimilarityEngine.EarlybirdModelBasedSearchQuery(
        seedUserIds = query.seedUserIds,
        maxNumTweets = query.maxNumTweets,
        oldestTweetTimestampInSec = Some(query.maxTweetAge.ago.inSeconds),
        frsUserToScoresForScoreAdjustment = query.frsUserToScoresForScoreAdjustment
      ),
      query.params
    )
}
package com.twitter.cr_mixer.similarity_engine

import com.google.inject.Inject
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.twitter.contentrecommender.thriftscala.AlgorithmType
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TopicTweetWithScore
import com.twitter.cr_mixer.param.TopicTweetParams
import com.twitter.cr_mixer.similarity_engine.SkitTopicTweetSimilarityEngine._
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.thriftscala.EmbeddingType
import com.twitter.simclusters_v2.thriftscala.TopicId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.topic_recos.thriftscala.TopicTweet
import com.twitter.topic_recos.thriftscala.TopicTweetPartitionFlatKey
import com.twitter.util.Future

@Singleton
case class SkitHighPrecisionTopicTweetSimilarityEngine @Inject() (
  @Named(ModuleNames.SkitStratoStoreName) skitStratoStore: ReadableStore[
    TopicTweetPartitionFlatKey,
    Seq[TopicTweet]
  ],
  statsReceiver: StatsReceiver)
    extends ReadableStore[EngineQuery[Query], Seq[TopicTweetWithScore]] {

  private val name: String = this.getClass.getSimpleName
  private val stats = statsReceiver.scope(name)

  override def get(query: EngineQuery[Query]): Future[Option[Seq[TopicTweetWithScore]]] = {
    StatsUtil.trackOptionItemsStats(stats) {
      fetch(query).map { tweets =>
        val topTweets =
          tweets
            .sortBy(-_.favCount)
            .take(query.storeQuery.maxCandidates)
            .map { tweet =>
              TopicTweetWithScore(
                tweetId = tweet.tweetId,
                score = tweet.favCount,
                similarityEngineType = SimilarityEngineType.SkitHighPrecisionTopicTweet
              )
            }
        Some(topTweets)
      }
    }
  }

  private def fetch(query: EngineQuery[Query]): Future[Seq[SkitTopicTweet]] = {
    val latestTweetTimeInHour = System.currentTimeMillis() / 1000 / 60 / 60

    val earliestTweetTimeInHour = latestTweetTimeInHour -
      math.min(MaxTweetAgeInHours, query.storeQuery.maxTweetAge.inHours)
    val timedKeys = for (timePartition <- earliestTweetTimeInHour to latestTweetTimeInHour) yield {

      TopicTweetPartitionFlatKey(
        entityId = query.storeQuery.topicId.entityId,
        timePartition = timePartition,
        algorithmType = Some(AlgorithmType.SemanticCoreTweet),
        tweetEmbeddingType = Some(EmbeddingType.LogFavBasedTweet),
        language = query.storeQuery.topicId.language.getOrElse("").toLowerCase,
        country = None, // Disable country. It is not used.
        semanticCoreAnnotationVersionId = Some(query.storeQuery.semanticCoreVersionId)
      )
    }

    getTweetsForKeys(
      timedKeys,
      query.storeQuery.topicId
    )
  }

  /**
   * Given a set of keys, multiget the underlying Strato store, combine and flatten the results.
   */
  private def getTweetsForKeys(
    keys: Seq[TopicTweetPartitionFlatKey],
    sourceTopic: TopicId
  ): Future[Seq[SkitTopicTweet]] = {
    Future
      .collect { skitStratoStore.multiGet(keys.toSet).values.toSeq }
      .map { combinedResults =>
        val topTweets = combinedResults.flatten.flatten
        topTweets.map { tweet =>
          SkitTopicTweet(
            tweetId = tweet.tweetId,
            favCount = tweet.scores.favCount.getOrElse(0L),
            cosineSimilarityScore = tweet.scores.cosineSimilarity.getOrElse(0.0),
            sourceTopic = sourceTopic
          )
        }
      }
  }
}

object SkitHighPrecisionTopicTweetSimilarityEngine {

  def fromParams(
    topicId: TopicId,
    isVideoOnly: Boolean,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    val maxCandidates = if (isVideoOnly) {
      params(TopicTweetParams.MaxSkitHighPrecisionCandidatesParam) * 2
    } else {
      params(TopicTweetParams.MaxSkitHighPrecisionCandidatesParam)
    }

    EngineQuery(
      Query(
        topicId = topicId,
        maxCandidates = maxCandidates,
        maxTweetAge = params(TopicTweetParams.MaxTweetAge),
        semanticCoreVersionId = params(TopicTweetParams.SemanticCoreVersionIdParam)
      ),
      params
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.MemCacheConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi.Params
import com.twitter.util.Future

case class LookupEngineQuery[Query](
  storeQuery: Query, // the actual Query type of the underlying store
  lookupKey: String,
  params: Params,
)

/**
 * This Engine provides a map interface for looking up different model implementations.
 * It provides modelId level monitoring for free.
 *
 * Example use cases include OfflineSimClusters lookup
 *
 *
 * @param versionedStoreMap   A mapping from a modelId to a corresponding implementation
 * @param memCacheConfigOpt   If specified, it will wrap the underlying store with a MemCache layer
 *                            You should only enable this for cacheable queries, e.x. TweetIds.
 *                            consumer based UserIds are generally not possible to cache.
 */
class LookupSimilarityEngine[Query, Candidate <: Serializable](
  versionedStoreMap: Map[String, ReadableStore[Query, Seq[Candidate]]], // key = modelId
  override val identifier: SimilarityEngineType,
  globalStats: StatsReceiver,
  engineConfig: SimilarityEngineConfig,
  memCacheConfigOpt: Option[MemCacheConfig[Query]] = None)
    extends SimilarityEngine[LookupEngineQuery[Query], Candidate] {

  private val scopedStats = globalStats.scope("similarityEngine", identifier.toString)

  private val underlyingLookupMap = {
    memCacheConfigOpt match {
      case Some(config) =>
        versionedStoreMap.map {
          case (modelId, store) =>
            (
              modelId,
              SimilarityEngine.addMemCache(
                underlyingStore = store,
                memCacheConfig = config,
                keyPrefix = Some(modelId),
                statsReceiver = scopedStats
              )
            )
        }
      case _ => versionedStoreMap
    }
  }

  override def getCandidates(
    engineQuery: LookupEngineQuery[Query]
  ): Future[Option[Seq[Candidate]]] = {
    val versionedStore =
      underlyingLookupMap
        .getOrElse(
          engineQuery.lookupKey,
          throw new IllegalArgumentException(
            s"${this.getClass.getSimpleName} ${identifier.toString}: ModelId ${engineQuery.lookupKey} does not exist"
          )
        )

    SimilarityEngine.getFromFn(
      fn = versionedStore.get,
      storeQuery = engineQuery.storeQuery,
      engineConfig = engineConfig,
      params = engineQuery.params,
      scopedStats = scopedStats.scope(engineQuery.lookupKey)
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.param.ConsumerEmbeddingBasedTwoTowerParams
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.timelines.configapi

object ConsumerEmbeddingBasedTwoTowerSimilarityEngine {
  def fromParams(
    sourceId: InternalId,
    params: configapi.Params,
  ): HnswANNEngineQuery = {
    HnswANNEngineQuery(
      sourceId = sourceId,
      modelId = params(ConsumerEmbeddingBasedTwoTowerParams.ModelIdParam),
      params = params
    )
  }
}
