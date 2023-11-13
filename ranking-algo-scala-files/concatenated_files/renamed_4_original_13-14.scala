package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.util.Duration
import com.twitter.util.Time

object FilterUtil {

  /** Returns a list of tweets that are generated less than `maxTweetAgeHours` hours ago */
  def tweetAgeFilter(
    candidates: Seq[TweetWithScore],
    maxTweetAgeHours: Duration
  ): Seq[TweetWithScore] = {
    // Tweet IDs are approximately chronological (see http://go/snowflake),
    // so we are building the earliest tweet id once
    // The per-candidate logic here then be candidate.tweetId > earliestPermittedTweetId, which is far cheaper.
    // See @cyao's phab on CrMixer generic age filter for reference https://phabricator.twitter.biz/D903188
    val earliestTweetId = SnowflakeId.firstIdFor(Time.now - maxTweetAgeHours)
    candidates.filter { candidate => candidate.tweetId >= earliestTweetId }
  }

  /** Returns a list of tweet sources that are generated less than `maxTweetAgeHours` hours ago */
  def tweetSourceAgeFilter(
    candidates: Seq[SourceInfo],
    maxTweetSignalAgeHoursParam: Duration
  ): Seq[SourceInfo] = {
    // Tweet IDs are approximately chronological (see http://go/snowflake),
    // so we are building the earliest tweet id once
    // This filter applies to source signals. Some candidate source calls can be avoided if source signals
    // can be filtered.
    val earliestTweetId = SnowflakeId.firstIdFor(Time.now - maxTweetSignalAgeHoursParam)
    candidates.filter { candidate =>
      candidate.internalId match {
        case InternalId.TweetId(tweetId) => tweetId >= earliestTweetId
        case _ => false
      }
    }
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.similarity_engine.EarlybirdModelBasedSimilarityEngine.EarlybirdModelBasedSearchQuery
import com.twitter.cr_mixer.similarity_engine.EarlybirdSimilarityEngineBase._
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.EarlybirdClientId
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.FacetsToFetch
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.MetadataOptions
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Trace
import com.twitter.search.common.ranking.thriftscala.ThriftRankingParams
import com.twitter.search.common.ranking.thriftscala.ThriftScoringFunctionType
import com.twitter.search.earlybird.thriftscala.EarlybirdRequest
import com.twitter.search.earlybird.thriftscala.EarlybirdService
import com.twitter.search.earlybird.thriftscala.ThriftSearchQuery
import com.twitter.search.earlybird.thriftscala.ThriftSearchRankingMode
import com.twitter.search.earlybird.thriftscala.ThriftSearchRelevanceOptions
import com.twitter.simclusters_v2.common.UserId
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
case class EarlybirdModelBasedSimilarityEngine @Inject() (
  earlybirdSearchClient: EarlybirdService.MethodPerEndpoint,
  timeoutConfig: TimeoutConfig,
  stats: StatsReceiver)
    extends EarlybirdSimilarityEngineBase[EarlybirdModelBasedSearchQuery] {
  import EarlybirdModelBasedSimilarityEngine._
  override val statsReceiver: StatsReceiver = stats.scope(this.getClass.getSimpleName)
  override def getEarlybirdRequest(
    query: EarlybirdModelBasedSearchQuery
  ): Option[EarlybirdRequest] =
    if (query.seedUserIds.nonEmpty)
      Some(
        EarlybirdRequest(
          searchQuery = getThriftSearchQuery(query),
          clientId = Some(EarlybirdClientId),
          timeoutMs = timeoutConfig.earlybirdServerTimeout.inMilliseconds.intValue(),
          clientRequestID = Some(s"${Trace.id.traceId}"),
        ))
    else None
}

object EarlybirdModelBasedSimilarityEngine {
  case class EarlybirdModelBasedSearchQuery(
    seedUserIds: Seq[UserId],
    maxNumTweets: Int,
    oldestTweetTimestampInSec: Option[UserId],
    frsUserToScoresForScoreAdjustment: Option[Map[UserId, Double]])
      extends EarlybirdSearchQuery

  /**
   * Used by Push Service
   */
  val RealGraphScoringModel = "frigate_unified_engagement_rg"
  val MaxHitsToProcess = 1000
  val MaxConsecutiveSameUser = 1

  private def getModelBasedRankingParams(
    authorSpecificScoreAdjustments: Map[Long, Double]
  ): ThriftRankingParams = ThriftRankingParams(
    `type` = Some(ThriftScoringFunctionType.ModelBased),
    selectedModels = Some(Map(RealGraphScoringModel -> 1.0)),
    applyBoosts = false,
    authorSpecificScoreAdjustments = Some(authorSpecificScoreAdjustments)
  )

  private def getRelevanceOptions(
    authorSpecificScoreAdjustments: Map[Long, Double],
  ): ThriftSearchRelevanceOptions = {
    ThriftSearchRelevanceOptions(
      maxConsecutiveSameUser = Some(MaxConsecutiveSameUser),
      rankingParams = Some(getModelBasedRankingParams(authorSpecificScoreAdjustments)),
      maxHitsToProcess = Some(MaxHitsToProcess),
      orderByRelevance = true
    )
  }

  private def getThriftSearchQuery(query: EarlybirdModelBasedSearchQuery): ThriftSearchQuery =
    ThriftSearchQuery(
      serializedQuery = Some(f"(* [since_time ${query.oldestTweetTimestampInSec.getOrElse(0)}])"),
      fromUserIDFilter64 = Some(query.seedUserIds),
      numResults = query.maxNumTweets,
      maxHitsToProcess = MaxHitsToProcess,
      rankingMode = ThriftSearchRankingMode.Relevance,
      relevanceOptions =
        Some(getRelevanceOptions(query.frsUserToScoresForScoreAdjustment.getOrElse(Map.empty))),
      facetFieldNames = Some(FacetsToFetch),
      resultMetadataOptions = Some(MetadataOptions),
      searcherId = None
    )
}
package com.twitter.cr_mixer.similarity_engine

import com.google.inject.Inject
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.twitter.contentrecommender.thriftscala.AlgorithmType
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TopicTweetWithScore
import com.twitter.cr_mixer.param.TopicTweetParams
import com.twitter.cr_mixer.similarity_engine.SkitTopicTweetSimilarityEngine._
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.thriftscala.EmbeddingType
import com.twitter.simclusters_v2.thriftscala.ModelVersion
import com.twitter.simclusters_v2.thriftscala.TopicId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.topic_recos.thriftscala.TopicTweet
import com.twitter.topic_recos.thriftscala.TopicTweetPartitionFlatKey
import com.twitter.util.Duration
import com.twitter.util.Future

@Singleton
case class SkitTopicTweetSimilarityEngine @Inject() (
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
            .sortBy(-_.cosineSimilarityScore)
            .take(query.storeQuery.maxCandidates)
            .map { tweet =>
              TopicTweetWithScore(
                tweetId = tweet.tweetId,
                score = tweet.cosineSimilarityScore,
                similarityEngineType = SimilarityEngineType.SkitTfgTopicTweet
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
        algorithmType = Some(AlgorithmType.TfgTweet),
        tweetEmbeddingType = Some(EmbeddingType.LogFavBasedTweet),
        language = query.storeQuery.topicId.language.getOrElse("").toLowerCase,
        country = None, // Disable country. It is not used.
        semanticCoreAnnotationVersionId = Some(query.storeQuery.semanticCoreVersionId),
        simclustersModelVersion = Some(ModelVersion.Model20m145k2020)
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

object SkitTopicTweetSimilarityEngine {

  val MaxTweetAgeInHours: Int = 7.days.inHours // Simple guard to prevent overloading

  // Query is used as a cache key. Do not add any user level information in this.
  case class Query(
    topicId: TopicId,
    maxCandidates: Int,
    maxTweetAge: Duration,
    semanticCoreVersionId: Long)

  case class SkitTopicTweet(
    sourceTopic: TopicId,
    tweetId: TweetId,
    favCount: Long,
    cosineSimilarityScore: Double)

  def fromParams(
    topicId: TopicId,
    isVideoOnly: Boolean,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    val maxCandidates = if (isVideoOnly) {
      params(TopicTweetParams.MaxSkitTfgCandidatesParam) * 2
    } else {
      params(TopicTweetParams.MaxSkitTfgCandidatesParam)
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

import com.twitter.cr_mixer.model.TripTweetWithScore
import com.twitter.cr_mixer.param.ConsumerEmbeddingBasedTripParams
import com.twitter.cr_mixer.util.InterleaveUtil
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.ClusterId
import com.twitter.simclusters_v2.common.SimClustersEmbedding
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.timelines.configapi.Params
import com.twitter.trends.trip_v1.trip_tweets.thriftscala.Cluster
import com.twitter.trends.trip_v1.trip_tweets.thriftscala.ClusterDomain
import com.twitter.trends.trip_v1.trip_tweets.thriftscala.TripTweet
import com.twitter.trends.trip_v1.trip_tweets.thriftscala.TripDomain
import com.twitter.util.Future

case class TripEngineQuery(
  modelId: String,
  sourceId: InternalId,
  tripSourceId: String,
  maxResult: Int,
  params: Params)

case class ConsumerEmbeddingBasedTripSimilarityEngine(
  embeddingStoreLookUpMap: Map[String, ReadableStore[UserId, SimClustersEmbedding]],
  tripCandidateSource: ReadableStore[TripDomain, Seq[TripTweet]],
  statsReceiver: StatsReceiver,
) extends ReadableStore[TripEngineQuery, Seq[TripTweetWithScore]] {
  import ConsumerEmbeddingBasedTripSimilarityEngine._

  private val scopedStats = statsReceiver.scope(name)
  private def fetchTopClusters(query: TripEngineQuery): Future[Option[Seq[ClusterId]]] = {
    query.sourceId match {
      case InternalId.UserId(userId) =>
        val embeddingStore = embeddingStoreLookUpMap.getOrElse(
          query.modelId,
          throw new IllegalArgumentException(
            s"${this.getClass.getSimpleName}: " +
              s"ModelId ${query.modelId} does not exist for embeddingStore"
          )
        )
        embeddingStore.get(userId).map(_.map(_.topClusterIds(MaxClusters)))
      case _ =>
        Future.None
    }
  }
  private def fetchCandidates(
    topClusters: Seq[ClusterId],
    tripSourceId: String
  ): Future[Seq[Seq[TripTweetWithScore]]] = {
    Future
      .collect {
        topClusters.map { clusterId =>
          tripCandidateSource
            .get(
              TripDomain(
                sourceId = tripSourceId,
                clusterDomain = Some(
                  ClusterDomain(simCluster = Some(Cluster(clusterIntId = Some(clusterId))))))).map {
              _.map {
                _.collect {
                  case TripTweet(tweetId, score) =>
                    TripTweetWithScore(tweetId, score)
                }
              }.getOrElse(Seq.empty).take(MaxNumResultsPerCluster)
            }
        }
      }
  }

  override def get(engineQuery: TripEngineQuery): Future[Option[Seq[TripTweetWithScore]]] = {
    val fetchTopClustersStat = scopedStats.scope(engineQuery.modelId).scope("fetchTopClusters")
    val fetchCandidatesStat = scopedStats.scope(engineQuery.modelId).scope("fetchCandidates")

    for {
      topClustersOpt <- StatsUtil.trackOptionStats(fetchTopClustersStat) {
        fetchTopClusters(engineQuery)
      }
      candidates <- StatsUtil.trackItemsStats(fetchCandidatesStat) {
        topClustersOpt match {
          case Some(topClusters) => fetchCandidates(topClusters, engineQuery.tripSourceId)
          case None => Future.Nil
        }
      }
    } yield {
      val interleavedTweets = InterleaveUtil.interleave(candidates)
      val dedupCandidates = interleavedTweets
        .groupBy(_.tweetId).flatMap {
          case (_, tweetWithScoreSeq) => tweetWithScoreSeq.sortBy(-_.score).take(1)
        }.toSeq.take(engineQuery.maxResult)
      Some(dedupCandidates)
    }
  }
}

object ConsumerEmbeddingBasedTripSimilarityEngine {
  private val MaxClusters: Int = 8
  private val MaxNumResultsPerCluster: Int = 25
  private val name: String = this.getClass.getSimpleName

  def fromParams(
    modelId: String,
    sourceId: InternalId,
    params: configapi.Params
  ): TripEngineQuery = {
    TripEngineQuery(
      modelId = modelId,
      sourceId = sourceId,
      tripSourceId = params(ConsumerEmbeddingBasedTripParams.SourceIdParam),
      maxResult = params(ConsumerEmbeddingBasedTripParams.MaxNumCandidatesParam),
      params = params
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.search.earlybird.thriftscala.EarlybirdRequest
import com.twitter.search.earlybird.thriftscala.EarlybirdService
import com.twitter.search.earlybird.thriftscala.ThriftSearchQuery
import com.twitter.util.Time
import com.twitter.search.common.query.thriftjava.thriftscala.CollectorParams
import com.twitter.search.common.ranking.thriftscala.ThriftRankingParams
import com.twitter.search.common.ranking.thriftscala.ThriftScoringFunctionType
import com.twitter.search.earlybird.thriftscala.ThriftSearchRelevanceOptions
import javax.inject.Inject
import javax.inject.Singleton
import EarlybirdSimilarityEngineBase._
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.similarity_engine.EarlybirdTensorflowBasedSimilarityEngine.EarlybirdTensorflowBasedSearchQuery
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.EarlybirdClientId
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.FacetsToFetch
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.GetCollectorTerminationParams
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.GetEarlybirdQuery
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.MetadataOptions
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.GetNamedDisjunctions
import com.twitter.search.earlybird.thriftscala.ThriftSearchRankingMode
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.util.Duration

@Singleton
case class EarlybirdTensorflowBasedSimilarityEngine @Inject() (
  earlybirdSearchClient: EarlybirdService.MethodPerEndpoint,
  timeoutConfig: TimeoutConfig,
  stats: StatsReceiver)
    extends EarlybirdSimilarityEngineBase[EarlybirdTensorflowBasedSearchQuery] {
  import EarlybirdTensorflowBasedSimilarityEngine._
  override val statsReceiver: StatsReceiver = stats.scope(this.getClass.getSimpleName)
  override def getEarlybirdRequest(
    query: EarlybirdTensorflowBasedSearchQuery
  ): Option[EarlybirdRequest] = {
    if (query.seedUserIds.nonEmpty)
      Some(
        EarlybirdRequest(
          searchQuery = getThriftSearchQuery(query, timeoutConfig.earlybirdServerTimeout),
          clientHost = None,
          clientRequestID = None,
          clientId = Some(EarlybirdClientId),
          clientRequestTimeMs = Some(Time.now.inMilliseconds),
          cachingParams = None,
          timeoutMs = timeoutConfig.earlybirdServerTimeout.inMilliseconds.intValue(),
          facetRequest = None,
          termStatisticsRequest = None,
          debugMode = 0,
          debugOptions = None,
          searchSegmentId = None,
          returnStatusType = None,
          successfulResponseThreshold = None,
          querySource = None,
          getOlderResults = Some(false),
          followedUserIds = Some(query.seedUserIds),
          adjustedProtectedRequestParams = None,
          adjustedFullArchiveRequestParams = None,
          getProtectedTweetsOnly = Some(false),
          retokenizeSerializedQuery = None,
          skipVeryRecentTweets = true,
          experimentClusterToUse = None
        ))
    else None
  }
}

object EarlybirdTensorflowBasedSimilarityEngine {
  case class EarlybirdTensorflowBasedSearchQuery(
    searcherUserId: Option[UserId],
    seedUserIds: Seq[UserId],
    maxNumTweets: Int,
    beforeTweetIdExclusive: Option[TweetId],
    afterTweetIdExclusive: Option[TweetId],
    filterOutRetweetsAndReplies: Boolean,
    useTensorflowRanking: Boolean,
    excludedTweetIds: Set[TweetId],
    maxNumHitsPerShard: Int)
      extends EarlybirdSearchQuery

  private def getThriftSearchQuery(
    query: EarlybirdTensorflowBasedSearchQuery,
    processingTimeout: Duration
  ): ThriftSearchQuery =
    ThriftSearchQuery(
      serializedQuery = GetEarlybirdQuery(
        query.beforeTweetIdExclusive,
        query.afterTweetIdExclusive,
        query.excludedTweetIds,
        query.filterOutRetweetsAndReplies).map(_.serialize),
      fromUserIDFilter64 = Some(query.seedUserIds),
      numResults = query.maxNumTweets,
      // Whether to collect conversation IDs. Remove it for now.
      // collectConversationId = Gate.True(), // true for Home
      rankingMode = ThriftSearchRankingMode.Relevance,
      relevanceOptions = Some(getRelevanceOptions),
      collectorParams = Some(
        CollectorParams(
          // numResultsToReturn defines how many results each EB shard will return to search root
          numResultsToReturn = 1000,
          // terminationParams.maxHitsToProcess is used for early terminating per shard results fetching.
          terminationParams =
            GetCollectorTerminationParams(query.maxNumHitsPerShard, processingTimeout)
        )),
      facetFieldNames = Some(FacetsToFetch),
      resultMetadataOptions = Some(MetadataOptions),
      searcherId = query.searcherUserId,
      searchStatusIds = None,
      namedDisjunctionMap = GetNamedDisjunctions(query.excludedTweetIds)
    )

  // The specific values of recap relevance/reranking options correspond to
  // experiment: enable_recap_reranking_2988,timeline_internal_disable_recap_filter
  // bucket    : enable_rerank,disable_filter
  private def getRelevanceOptions: ThriftSearchRelevanceOptions = {
    ThriftSearchRelevanceOptions(
      proximityScoring = true,
      maxConsecutiveSameUser = Some(2),
      rankingParams = Some(getTensorflowBasedRankingParams),
      maxHitsToProcess = Some(500),
      maxUserBlendCount = Some(3),
      proximityPhraseWeight = 9.0,
      returnAllResults = Some(true)
    )
  }

  private def getTensorflowBasedRankingParams: ThriftRankingParams = {
    ThriftRankingParams(
      `type` = Some(ThriftScoringFunctionType.TensorflowBased),
      selectedTensorflowModel = Some("timelines_rectweet_replica"),
      minScore = -1.0e100,
      applyBoosts = false,
      authorSpecificScoreAdjustments = None
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.ProducerBasedUserTweetGraphParams
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.recos.user_tweet_graph.thriftscala.ProducerBasedRelatedTweetRequest
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import javax.inject.Singleton
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.timelines.configapi
import com.twitter.recos.user_tweet_graph.thriftscala.UserTweetGraph

/**
 * This store looks for similar tweets from UserTweetGraph for a Source ProducerId
 * For a query producerId,User Tweet Graph (UTG),
 * lets us find out which tweets the query producer's followers co-engaged
 */
@Singleton
case class ProducerBasedUserTweetGraphSimilarityEngine(
  userTweetGraphService: UserTweetGraph.MethodPerEndpoint,
  statsReceiver: StatsReceiver)
    extends ReadableStore[ProducerBasedUserTweetGraphSimilarityEngine.Query, Seq[
      TweetWithScore
    ]] {

  private val stats = statsReceiver.scope(this.getClass.getSimpleName)
  private val fetchCandidatesStat = stats.scope("fetchCandidates")

  override def get(
    query: ProducerBasedUserTweetGraphSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    query.sourceId match {
      case InternalId.UserId(producerId) =>
        StatsUtil.trackOptionItemsStats(fetchCandidatesStat) {
          val relatedTweetRequest =
            ProducerBasedRelatedTweetRequest(
              producerId,
              maxResults = Some(query.maxResults),
              minCooccurrence = Some(query.minCooccurrence),
              minScore = Some(query.minScore),
              maxNumFollowers = Some(query.maxNumFollowers),
              maxTweetAgeInHours = Some(query.maxTweetAgeInHours),
            )

          userTweetGraphService.producerBasedRelatedTweets(relatedTweetRequest).map {
            relatedTweetResponse =>
              val candidates =
                relatedTweetResponse.tweets.map(tweet => TweetWithScore(tweet.tweetId, tweet.score))
              Some(candidates)
          }
        }
      case _ =>
        Future.value(None)
    }
  }
}

object ProducerBasedUserTweetGraphSimilarityEngine {

  def toSimilarityEngineInfo(score: Double): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.ProducerBasedUserTweetGraph,
      modelId = None,
      score = Some(score))
  }

  case class Query(
    sourceId: InternalId,
    maxResults: Int,
    minCooccurrence: Int, // require at least {minCooccurrence} lhs user engaged with returned tweet
    minScore: Double,
    maxNumFollowers: Int, // max number of lhs users
    maxTweetAgeInHours: Int)

  def fromParams(
    sourceId: InternalId,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    EngineQuery(
      Query(
        sourceId = sourceId,
        maxResults = params(GlobalParams.MaxCandidateNumPerSourceKeyParam),
        minCooccurrence = params(ProducerBasedUserTweetGraphParams.MinCoOccurrenceParam),
        maxNumFollowers = params(ProducerBasedUserTweetGraphParams.MaxNumFollowersParam),
        maxTweetAgeInHours = params(GlobalParams.MaxTweetAgeHoursParam).inHours,
        minScore = params(ProducerBasedUserTweetGraphParams.MinScoreParam)
      ),
      params
    )
  }
}
package com.twitter.cr_mixer.source_signal

import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.cr_mixer.source_signal.SourceFetcher.FetcherQuery
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.storehaus.ReadableStore
import com.twitter.usersignalservice.thriftscala.{Signal => UssSignal}
import com.twitter.usersignalservice.thriftscala.SignalType
import com.twitter.frigate.common.util.StatsUtil.Size
import com.twitter.frigate.common.util.StatsUtil.Success
import com.twitter.frigate.common.util.StatsUtil.Empty
import com.twitter.util.Future
import com.twitter.util.Time
import javax.inject.Singleton
import javax.inject.Inject
import javax.inject.Named

@Singleton
case class UssSourceSignalFetcher @Inject() (
  @Named(ModuleNames.UssStore) ussStore: ReadableStore[UssStore.Query, Seq[
    (SignalType, Seq[UssSignal])
  ]],
  override val timeoutConfig: TimeoutConfig,
  globalStats: StatsReceiver)
    extends SourceSignalFetcher {

  override protected val stats: StatsReceiver = globalStats.scope(identifier)
  override type SignalConvertType = UssSignal

  // always enable USS call. We have fine-grained FS to decider which signal to fetch
  override def isEnabled(query: FetcherQuery): Boolean = true

  override def fetchAndProcess(
    query: FetcherQuery,
  ): Future[Option[Seq[SourceInfo]]] = {
    // Fetch raw signals
    val rawSignals = ussStore.get(UssStore.Query(query.userId, query.params, query.product)).map {
      _.map {
        _.map {
          case (signalType, signals) =>
            trackUssSignalStatsPerSignalType(query, signalType, signals)
            (signalType, signals)
        }
      }
    }

    /**
     * Process signals:
     * Transform a Seq of USS Signals with signalType specified to a Seq of SourceInfo
     * We do case match to make sure the SignalType can correctly map to a SourceType defined in CrMixer
     * and it should be simplified.
     */
    rawSignals.map {
      _.map { nestedSignal =>
        val sourceInfoList = nestedSignal.flatMap {
          case (signalType, ussSignals) =>
            signalType match {
              case SignalType.TweetFavorite =>
                convertSourceInfo(sourceType = SourceType.TweetFavorite, signals = ussSignals)
              case SignalType.Retweet =>
                convertSourceInfo(sourceType = SourceType.Retweet, signals = ussSignals)
              case SignalType.Reply =>
                convertSourceInfo(sourceType = SourceType.Reply, signals = ussSignals)
              case SignalType.OriginalTweet =>
                convertSourceInfo(sourceType = SourceType.OriginalTweet, signals = ussSignals)
              case SignalType.AccountFollow =>
                convertSourceInfo(sourceType = SourceType.UserFollow, signals = ussSignals)
              case SignalType.RepeatedProfileVisit180dMinVisit6V1 |
                  SignalType.RepeatedProfileVisit90dMinVisit6V1 |
                  SignalType.RepeatedProfileVisit14dMinVisit2V1 =>
                convertSourceInfo(
                  sourceType = SourceType.UserRepeatedProfileVisit,
                  signals = ussSignals)
              case SignalType.NotificationOpenAndClickV1 =>
                convertSourceInfo(sourceType = SourceType.NotificationClick, signals = ussSignals)
              case SignalType.TweetShareV1 =>
                convertSourceInfo(sourceType = SourceType.TweetShare, signals = ussSignals)
              case SignalType.RealGraphOon =>
                convertSourceInfo(sourceType = SourceType.RealGraphOon, signals = ussSignals)
              case SignalType.GoodTweetClick | SignalType.GoodTweetClick5s |
                  SignalType.GoodTweetClick10s | SignalType.GoodTweetClick30s =>
                convertSourceInfo(sourceType = SourceType.GoodTweetClick, signals = ussSignals)
              case SignalType.VideoView90dPlayback50V1 =>
                convertSourceInfo(
                  sourceType = SourceType.VideoTweetPlayback50,
                  signals = ussSignals)
              case SignalType.VideoView90dQualityV1 =>
                convertSourceInfo(
                  sourceType = SourceType.VideoTweetQualityView,
                  signals = ussSignals)
              case SignalType.GoodProfileClick | SignalType.GoodProfileClick20s |
                  SignalType.GoodProfileClick30s =>
                convertSourceInfo(sourceType = SourceType.GoodProfileClick, signals = ussSignals)
              // negative signals
              case SignalType.AccountBlock =>
                convertSourceInfo(sourceType = SourceType.AccountBlock, signals = ussSignals)
              case SignalType.AccountMute =>
                convertSourceInfo(sourceType = SourceType.AccountMute, signals = ussSignals)
              case SignalType.TweetReport =>
                convertSourceInfo(sourceType = SourceType.TweetReport, signals = ussSignals)
              case SignalType.TweetDontLike =>
                convertSourceInfo(sourceType = SourceType.TweetDontLike, signals = ussSignals)
              // Aggregated Signals
              case SignalType.TweetBasedUnifiedEngagementWeightedSignal |
                  SignalType.TweetBasedUnifiedUniformSignal =>
                convertSourceInfo(sourceType = SourceType.TweetAggregation, signals = ussSignals)
              case SignalType.ProducerBasedUnifiedEngagementWeightedSignal |
                  SignalType.ProducerBasedUnifiedUniformSignal =>
                convertSourceInfo(sourceType = SourceType.ProducerAggregation, signals = ussSignals)

              // Default
              case _ =>
                Seq.empty[SourceInfo]
            }
        }
        sourceInfoList
      }
    }
  }

  override def convertSourceInfo(
    sourceType: SourceType,
    signals: Seq[SignalConvertType]
  ): Seq[SourceInfo] = {
    signals.map { signal =>
      SourceInfo(
        sourceType = sourceType,
        internalId = signal.targetInternalId.getOrElse(
          throw new IllegalArgumentException(
            s"${sourceType.toString} Signal does not have internalId")),
        sourceEventTime =
          if (signal.timestamp == 0L) None else Some(Time.fromMilliseconds(signal.timestamp))
      )
    }
  }

  private def trackUssSignalStatsPerSignalType(
    query: FetcherQuery,
    signalType: SignalType,
    ussSignals: Seq[UssSignal]
  ): Unit = {
    val productScopedStats = stats.scope(query.product.originalName)
    val productUserStateScopedStats = productScopedStats.scope(query.userState.toString)
    val productStats = productScopedStats.scope(signalType.toString)
    val productUserStateStats = productUserStateScopedStats.scope(signalType.toString)

    productStats.counter(Success).incr()
    productUserStateStats.counter(Success).incr()
    val size = ussSignals.size
    productStats.stat(Size).add(size)
    productUserStateStats.stat(Size).add(size)
    if (size == 0) {
      productStats.counter(Empty).incr()
      productUserStateStats.counter(Empty).incr()
    }
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.ConsumersBasedUserAdGraphParams
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.recos.user_ad_graph.thriftscala.ConsumersBasedRelatedAdRequest
import com.twitter.recos.user_ad_graph.thriftscala.RelatedAdResponse
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Singleton

/**
 * This store uses the graph based input (a list of userIds)
 * to query consumersBasedUserAdGraph and get their top engaged ad tweets
 */
@Singleton
case class ConsumersBasedUserAdGraphSimilarityEngine(
  consumersBasedUserAdGraphStore: ReadableStore[
    ConsumersBasedRelatedAdRequest,
    RelatedAdResponse
  ],
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      ConsumersBasedUserAdGraphSimilarityEngine.Query,
      Seq[TweetWithScore]
    ] {

  override def get(
    query: ConsumersBasedUserAdGraphSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    val consumersBasedRelatedAdRequest =
      ConsumersBasedRelatedAdRequest(
        query.seedWithScores.keySet.toSeq,
        maxResults = Some(query.maxResults),
        minCooccurrence = Some(query.minCooccurrence),
        minScore = Some(query.minScore),
        maxTweetAgeInHours = Some(query.maxTweetAgeInHours)
      )
    consumersBasedUserAdGraphStore
      .get(consumersBasedRelatedAdRequest)
      .map { relatedAdResponseOpt =>
        relatedAdResponseOpt.map { relatedAdResponse =>
          relatedAdResponse.adTweets.map { tweet =>
            TweetWithScore(tweet.adTweetId, tweet.score)
          }
        }
      }
  }
}

object ConsumersBasedUserAdGraphSimilarityEngine {

  case class Query(
    seedWithScores: Map[UserId, Double],
    maxResults: Int,
    minCooccurrence: Int,
    minScore: Double,
    maxTweetAgeInHours: Int)

  def toSimilarityEngineInfo(
    score: Double
  ): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.ConsumersBasedUserAdGraph,
      modelId = None,
      score = Some(score))
  }

  def fromParams(
    seedWithScores: Map[UserId, Double],
    params: configapi.Params,
  ): EngineQuery[Query] = {

    EngineQuery(
      Query(
        seedWithScores = seedWithScores,
        maxResults = params(GlobalParams.MaxCandidateNumPerSourceKeyParam),
        minCooccurrence = params(ConsumersBasedUserAdGraphParams.MinCoOccurrenceParam),
        minScore = params(ConsumersBasedUserAdGraphParams.MinScoreParam),
        maxTweetAgeInHours = params(GlobalParams.MaxTweetAgeHoursParam).inHours,
      ),
      params
    )
  }
}
package com.twitter.cr_mixer.similarity_engine
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TweetWithAuthor
import com.twitter.cr_mixer.similarity_engine.EarlybirdRecencyBasedSimilarityEngine.EarlybirdRecencyBasedSearchQuery
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

@Singleton
case class EarlybirdRecencyBasedSimilarityEngine @Inject() (
  @Named(ModuleNames.EarlybirdRecencyBasedWithoutRetweetsRepliesTweetsCache)
  earlybirdRecencyBasedWithoutRetweetsRepliesTweetsCacheStore: ReadableStore[
    UserId,
    Seq[TweetId]
  ],
  @Named(ModuleNames.EarlybirdRecencyBasedWithRetweetsRepliesTweetsCache)
  earlybirdRecencyBasedWithRetweetsRepliesTweetsCacheStore: ReadableStore[
    UserId,
    Seq[TweetId]
  ],
  timeoutConfig: TimeoutConfig,
  stats: StatsReceiver)
    extends ReadableStore[EarlybirdRecencyBasedSearchQuery, Seq[TweetWithAuthor]] {
  import EarlybirdRecencyBasedSimilarityEngine._
  val statsReceiver: StatsReceiver = stats.scope(this.getClass.getSimpleName)

  override def get(
    query: EarlybirdRecencyBasedSearchQuery
  ): Future[Option[Seq[TweetWithAuthor]]] = {
    Future
      .collect {
        if (query.filterOutRetweetsAndReplies) {
          query.seedUserIds.map { seedUserId =>
            StatsUtil.trackOptionItemsStats(statsReceiver.scope("WithoutRetweetsAndReplies")) {
              earlybirdRecencyBasedWithoutRetweetsRepliesTweetsCacheStore
                .get(seedUserId).map(_.map(_.map(tweetId =>
                  TweetWithAuthor(tweetId = tweetId, authorId = seedUserId))))
            }
          }
        } else {
          query.seedUserIds.map { seedUserId =>
            StatsUtil.trackOptionItemsStats(statsReceiver.scope("WithRetweetsAndReplies")) {
              earlybirdRecencyBasedWithRetweetsRepliesTweetsCacheStore
                .get(seedUserId)
                .map(_.map(_.map(tweetId =>
                  TweetWithAuthor(tweetId = tweetId, authorId = seedUserId))))
            }
          }
        }
      }
      .map { tweetWithAuthorList =>
        val earliestTweetId = SnowflakeId.firstIdFor(Time.now - query.maxTweetAge)
        tweetWithAuthorList
          .flatMap(_.getOrElse(Seq.empty))
          .filter(tweetWithAuthor =>
            tweetWithAuthor.tweetId >= earliestTweetId // tweet age filter
              && !query.excludedTweetIds
                .contains(tweetWithAuthor.tweetId)) // excluded tweet filter
          .sortBy(tweetWithAuthor =>
            -SnowflakeId.unixTimeMillisFromId(tweetWithAuthor.tweetId)) // sort by recency
          .take(query.maxNumTweets) // take most recent N tweets
      }
      .map(result => Some(result))
  }

}

object EarlybirdRecencyBasedSimilarityEngine {
  case class EarlybirdRecencyBasedSearchQuery(
    seedUserIds: Seq[UserId],
    maxNumTweets: Int,
    excludedTweetIds: Set[TweetId],
    maxTweetAge: Duration,
    filterOutRetweetsAndReplies: Boolean)

}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.param.TweetBasedUserTweetGraphParams
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.recos.user_tweet_graph.thriftscala.RelatedTweetResponse
import com.twitter.recos.user_tweet_graph.thriftscala.TweetBasedRelatedTweetRequest
import com.twitter.recos.user_tweet_graph.thriftscala.ConsumersBasedRelatedTweetRequest
import com.twitter.recos.user_tweet_graph.thriftscala.UserTweetGraph
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.twistly.thriftscala.TweetRecentEngagedUsers
import com.twitter.util.Future
import javax.inject.Singleton
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.timelines.configapi
import com.twitter.util.Duration
import com.twitter.util.Time
import scala.concurrent.duration.HOURS

/**
 * This store looks for similar tweets from UserTweetGraph for a Source TweetId
 * For a query tweet,User Tweet Graph (UTG),
 * lets us find out which other tweets share a lot of the same engagers with the query tweet
 * one-pager: go/UTG
 */
@Singleton
case class TweetBasedUserTweetGraphSimilarityEngine(
  userTweetGraphService: UserTweetGraph.MethodPerEndpoint,
  tweetEngagedUsersStore: ReadableStore[TweetId, TweetRecentEngagedUsers],
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      TweetBasedUserTweetGraphSimilarityEngine.Query,
      Seq[TweetWithScore]
    ] {

  import TweetBasedUserTweetGraphSimilarityEngine._

  private val stats = statsReceiver.scope(this.getClass.getSimpleName)
  private val fetchCandidatesStat = stats.scope("fetchCandidates")
  private val fetchCoverageExpansionCandidatesStat = stats.scope("fetchCoverageExpansionCandidates")

  override def get(
    query: TweetBasedUserTweetGraphSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    query.sourceId match {
      case InternalId.TweetId(tweetId) if query.enableCoverageExpansionAllTweet =>
        getCoverageExpansionCandidates(tweetId, query)

      case InternalId.TweetId(tweetId) if query.enableCoverageExpansionOldTweet => // For Home
        if (isOldTweet(tweetId)) getCoverageExpansionCandidates(tweetId, query)
        else getCandidates(tweetId, query)

      case InternalId.TweetId(tweetId) => getCandidates(tweetId, query)
      case _ =>
        Future.value(None)
    }
  }

  // This is the main candidate source
  private def getCandidates(
    tweetId: TweetId,
    query: TweetBasedUserTweetGraphSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    StatsUtil.trackOptionItemsStats(fetchCandidatesStat) {
      val tweetBasedRelatedTweetRequest = {
        TweetBasedRelatedTweetRequest(
          tweetId,
          maxResults = Some(query.maxResults),
          minCooccurrence = Some(query.minCooccurrence),
          excludeTweetIds = Some(Seq(tweetId)),
          minScore = Some(query.tweetBasedMinScore),
          maxTweetAgeInHours = Some(query.maxTweetAgeInHours)
        )
      }
      toTweetWithScore(
        userTweetGraphService.tweetBasedRelatedTweets(tweetBasedRelatedTweetRequest).map {
          Some(_)
        })
    }
  }

  // function for DDGs, for coverage expansion algo, we first fetch tweet's recent engaged users as consumeSeedSet from MH store,
  // and query consumersBasedUTG using the consumeSeedSet
  private def getCoverageExpansionCandidates(
    tweetId: TweetId,
    query: TweetBasedUserTweetGraphSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    StatsUtil
      .trackOptionItemsStats(fetchCoverageExpansionCandidatesStat) {
        tweetEngagedUsersStore
          .get(tweetId).flatMap {
            _.map { tweetRecentEngagedUsers =>
              val consumerSeedSet =
                tweetRecentEngagedUsers.recentEngagedUsers
                  .map { _.userId }.take(query.maxConsumerSeedsNum)
              val consumersBasedRelatedTweetRequest =
                ConsumersBasedRelatedTweetRequest(
                  consumerSeedSet = consumerSeedSet,
                  maxResults = Some(query.maxResults),
                  minCooccurrence = Some(query.minCooccurrence),
                  excludeTweetIds = Some(Seq(tweetId)),
                  minScore = Some(query.consumersBasedMinScore),
                  maxTweetAgeInHours = Some(query.maxTweetAgeInHours)
                )

              toTweetWithScore(userTweetGraphService
                .consumersBasedRelatedTweets(consumersBasedRelatedTweetRequest).map { Some(_) })
            }.getOrElse(Future.value(None))
          }
      }
  }

}

object TweetBasedUserTweetGraphSimilarityEngine {

  def toSimilarityEngineInfo(score: Double): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.TweetBasedUserTweetGraph,
      modelId = None,
      score = Some(score))
  }

  private val oldTweetCap: Duration = Duration(48, HOURS)

  private def toTweetWithScore(
    relatedTweetResponseFut: Future[Option[RelatedTweetResponse]]
  ): Future[Option[Seq[TweetWithScore]]] = {
    relatedTweetResponseFut.map { relatedTweetResponseOpt =>
      relatedTweetResponseOpt.map { relatedTweetResponse =>
        val candidates =
          relatedTweetResponse.tweets.map(tweet => TweetWithScore(tweet.tweetId, tweet.score))
        candidates
      }
    }
  }

  private def isOldTweet(tweetId: TweetId): Boolean = {
    SnowflakeId
      .timeFromIdOpt(tweetId).forall { tweetTime => tweetTime < Time.now - oldTweetCap }
    // If there's no snowflake timestamp, we have no idea when this tweet happened.
  }

  case class Query(
    sourceId: InternalId,
    maxResults: Int,
    minCooccurrence: Int,
    tweetBasedMinScore: Double,
    consumersBasedMinScore: Double,
    maxTweetAgeInHours: Int,
    maxConsumerSeedsNum: Int,
    enableCoverageExpansionOldTweet: Boolean,
    enableCoverageExpansionAllTweet: Boolean,
  )

  def fromParams(
    sourceId: InternalId,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    EngineQuery(
      Query(
        sourceId = sourceId,
        maxResults = params(GlobalParams.MaxCandidateNumPerSourceKeyParam),
        minCooccurrence = params(TweetBasedUserTweetGraphParams.MinCoOccurrenceParam),
        tweetBasedMinScore = params(TweetBasedUserTweetGraphParams.TweetBasedMinScoreParam),
        consumersBasedMinScore = params(TweetBasedUserTweetGraphParams.ConsumersBasedMinScoreParam),
        maxTweetAgeInHours = params(GlobalParams.MaxTweetAgeHoursParam).inHours,
        maxConsumerSeedsNum = params(TweetBasedUserTweetGraphParams.MaxConsumerSeedsNumParam),
        enableCoverageExpansionOldTweet =
          params(TweetBasedUserTweetGraphParams.EnableCoverageExpansionOldTweetParam),
        enableCoverageExpansionAllTweet =
          params(TweetBasedUserTweetGraphParams.EnableCoverageExpansionAllTweetParam),
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

/**
 * @tparam Query ReadableStore's input type.
 */
case class EngineQuery[Query](
  storeQuery: Query,
  params: Params,
)

/**
 * A straight forward SimilarityEngine implementation that wraps a ReadableStore
 *
 * @param implementingStore   Provides the candidate retrieval's implementations
 * @param memCacheConfig      If specified, it will wrap the underlying store with a MemCache layer
 *                            You should only enable this for cacheable queries, e.x. TweetIds.
 *                            consumer based UserIds are generally not possible to cache.
 * @tparam Query              ReadableStore's input type
 * @tparam Candidate          ReadableStore's return type is Seq[[[Candidate]]]
 */
class StandardSimilarityEngine[Query, Candidate <: Serializable](
  implementingStore: ReadableStore[Query, Seq[Candidate]],
  override val identifier: SimilarityEngineType,
  globalStats: StatsReceiver,
  engineConfig: SimilarityEngineConfig,
  memCacheConfig: Option[MemCacheConfig[Query]] = None)
    extends SimilarityEngine[EngineQuery[Query], Candidate] {

  private val scopedStats = globalStats.scope("similarityEngine", identifier.toString)

  def getScopedStats: StatsReceiver = scopedStats

  // Add memcache wrapper, if specified
  private val store = {
    memCacheConfig match {
      case Some(config) =>
        SimilarityEngine.addMemCache(
          underlyingStore = implementingStore,
          memCacheConfig = config,
          statsReceiver = scopedStats
        )
      case _ => implementingStore
    }
  }

  override def getCandidates(
    engineQuery: EngineQuery[Query]
  ): Future[Option[Seq[Candidate]]] = {
    SimilarityEngine.getFromFn(
      store.get,
      engineQuery.storeQuery,
      engineConfig,
      engineQuery.params,
      scopedStats
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.config.SimClustersANNConfig
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.thriftscala.EmbeddingType
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.simclusters_v2.thriftscala.ModelVersion
import com.twitter.simclusters_v2.thriftscala.SimClustersEmbeddingId
import com.twitter.simclustersann.thriftscala.SimClustersANNService
import com.twitter.simclustersann.thriftscala.{Query => SimClustersANNQuery}
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Singleton
import com.twitter.cr_mixer.exception.InvalidSANNConfigException
import com.twitter.relevance_platform.simclustersann.multicluster.ServiceNameMapper

@Singleton
case class SimClustersANNSimilarityEngine(
  simClustersANNServiceNameToClientMapper: Map[String, SimClustersANNService.MethodPerEndpoint],
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      SimClustersANNSimilarityEngine.Query,
      Seq[TweetWithScore]
    ] {

  private val name: String = this.getClass.getSimpleName
  private val stats = statsReceiver.scope(name)
  private val fetchCandidatesStat = stats.scope("fetchCandidates")

  private def getSimClustersANNService(
    query: SimClustersANNQuery
  ): Option[SimClustersANNService.MethodPerEndpoint] = {
    ServiceNameMapper
      .getServiceName(
        query.sourceEmbeddingId.modelVersion,
        query.config.candidateEmbeddingType).flatMap(serviceName =>
        simClustersANNServiceNameToClientMapper.get(serviceName))
  }

  override def get(
    query: SimClustersANNSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    StatsUtil.trackOptionItemsStats(fetchCandidatesStat) {

      getSimClustersANNService(query.simClustersANNQuery) match {
        case Some(simClustersANNService) =>
          simClustersANNService.getTweetCandidates(query.simClustersANNQuery).map {
            simClustersANNTweetCandidates =>
              val tweetWithScores = simClustersANNTweetCandidates.map { candidate =>
                TweetWithScore(candidate.tweetId, candidate.score)
              }
              Some(tweetWithScores)
          }
        case None =>
          throw InvalidSANNConfigException(
            "No SANN Cluster configured to serve this query, check CandidateEmbeddingType and ModelVersion")
      }
    }
  }
}

object SimClustersANNSimilarityEngine {
  case class Query(
    simClustersANNQuery: SimClustersANNQuery,
    simClustersANNConfigId: String)

  def toSimilarityEngineInfo(
    query: EngineQuery[Query],
    score: Double
  ): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.SimClustersANN,
      modelId = Some(
        s"SimClustersANN_${query.storeQuery.simClustersANNQuery.sourceEmbeddingId.embeddingType.toString}_" +
          s"${query.storeQuery.simClustersANNQuery.sourceEmbeddingId.modelVersion.toString}_" +
          s"${query.storeQuery.simClustersANNConfigId}"),
      score = Some(score)
    )
  }

  def fromParams(
    internalId: InternalId,
    embeddingType: EmbeddingType,
    modelVersion: ModelVersion,
    simClustersANNConfigId: String,
    params: configapi.Params,
  ): EngineQuery[Query] = {

    // SimClusters EmbeddingId and ANNConfig
    val simClustersEmbeddingId =
      SimClustersEmbeddingId(embeddingType, modelVersion, internalId)
    val simClustersANNConfig =
      SimClustersANNConfig
        .getConfig(embeddingType.toString, modelVersion.toString, simClustersANNConfigId)

    EngineQuery(
      Query(
        SimClustersANNQuery(
          sourceEmbeddingId = simClustersEmbeddingId,
          config = simClustersANNConfig.toSANNConfigThrift
        ),
        simClustersANNConfigId
      ),
      params
    )
  }

}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.recos.recos_common.thriftscala.SocialProofType
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScoreAndSocialProof
import com.twitter.cr_mixer.param.UtegTweetGlobalParams
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.recos.user_tweet_entity_graph.thriftscala.TweetEntityDisplayLocation
import com.twitter.recos.user_tweet_entity_graph.thriftscala.UserTweetEntityGraph
import com.twitter.recos.user_tweet_entity_graph.thriftscala.RecommendTweetEntityRequest
import com.twitter.recos.user_tweet_entity_graph.thriftscala.RecommendationType
import com.twitter.recos.user_tweet_entity_graph.thriftscala.UserTweetEntityRecommendationUnion.TweetRec
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Duration
import com.twitter.util.Future
import javax.inject.Singleton

@Singleton
case class UserTweetEntityGraphSimilarityEngine(
  userTweetEntityGraph: UserTweetEntityGraph.MethodPerEndpoint,
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      UserTweetEntityGraphSimilarityEngine.Query,
      Seq[TweetWithScoreAndSocialProof]
    ] {

  override def get(
    query: UserTweetEntityGraphSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScoreAndSocialProof]]] = {
    val recommendTweetEntityRequest =
      RecommendTweetEntityRequest(
        requesterId = query.userId,
        displayLocation = TweetEntityDisplayLocation.HomeTimeline,
        recommendationTypes = Seq(RecommendationType.Tweet),
        seedsWithWeights = query.seedsWithWeights,
        maxResultsByType = Some(Map(RecommendationType.Tweet -> query.maxUtegCandidates)),
        maxTweetAgeInMillis = Some(query.maxTweetAge.inMilliseconds),
        excludedTweetIds = query.excludedTweetIds,
        maxUserSocialProofSize = Some(UserTweetEntityGraphSimilarityEngine.MaxUserSocialProofSize),
        maxTweetSocialProofSize =
          Some(UserTweetEntityGraphSimilarityEngine.MaxTweetSocialProofSize),
        minUserSocialProofSizes = Some(Map(RecommendationType.Tweet -> 1)),
        tweetTypes = None,
        socialProofTypes = query.socialProofTypes,
        socialProofTypeUnions = None,
        tweetAuthors = None,
        maxEngagementAgeInMillis = None,
        excludedTweetAuthors = None,
      )

    userTweetEntityGraph
      .recommendTweets(recommendTweetEntityRequest)
      .map { recommendTweetsResponse =>
        val candidates = recommendTweetsResponse.recommendations.flatMap {
          case TweetRec(recommendation) =>
            Some(
              TweetWithScoreAndSocialProof(
                recommendation.tweetId,
                recommendation.score,
                recommendation.socialProofByType.toMap))
          case _ => None
        }
        Some(candidates)
      }
  }
}

object UserTweetEntityGraphSimilarityEngine {

  private val MaxUserSocialProofSize = 10
  private val MaxTweetSocialProofSize = 10

  def toSimilarityEngineInfo(score: Double): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.Uteg,
      modelId = None,
      score = Some(score))
  }

  case class Query(
    userId: UserId,
    seedsWithWeights: Map[UserId, Double],
    excludedTweetIds: Option[Seq[Long]] = None,
    maxUtegCandidates: Int,
    maxTweetAge: Duration,
    socialProofTypes: Option[Seq[SocialProofType]])

  def fromParams(
    userId: UserId,
    seedsWithWeights: Map[UserId, Double],
    excludedTweetIds: Option[Seq[TweetId]] = None,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    EngineQuery(
      Query(
        userId = userId,
        seedsWithWeights = seedsWithWeights,
        excludedTweetIds = excludedTweetIds,
        maxUtegCandidates = params(UtegTweetGlobalParams.MaxUtegCandidatesToRequestParam),
        maxTweetAge = params(UtegTweetGlobalParams.CandidateRefreshSinceTimeOffsetHoursParam),
        socialProofTypes = Some(Seq(SocialProofType.Favorite))
      ),
      params
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.TweetWithAuthor
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.search.earlybird.thriftscala.EarlybirdRequest
import com.twitter.search.earlybird.thriftscala.EarlybirdResponseCode
import com.twitter.search.earlybird.thriftscala.EarlybirdService
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future

/**
 * This trait is a base trait for Earlybird similarity engines. All Earlybird similarity
 * engines extend from it and override the construction method for EarlybirdRequest
 */
trait EarlybirdSimilarityEngineBase[EarlybirdSearchQuery]
    extends ReadableStore[EarlybirdSearchQuery, Seq[TweetWithAuthor]] {
  def earlybirdSearchClient: EarlybirdService.MethodPerEndpoint

  def statsReceiver: StatsReceiver

  def getEarlybirdRequest(query: EarlybirdSearchQuery): Option[EarlybirdRequest]

  override def get(query: EarlybirdSearchQuery): Future[Option[Seq[TweetWithAuthor]]] = {
    getEarlybirdRequest(query)
      .map { earlybirdRequest =>
        earlybirdSearchClient
          .search(earlybirdRequest).map { response =>
            response.responseCode match {
              case EarlybirdResponseCode.Success =>
                val earlybirdSearchResult =
                  response.searchResults
                    .map(
                      _.results
                        .map(searchResult =>
                          TweetWithAuthor(
                            searchResult.id,
                            // fromUserId should be there since MetadataOptions.getFromUserId = true
                            searchResult.metadata.map(_.fromUserId).getOrElse(0))).toSeq)
                statsReceiver.scope("result").stat("size").add(earlybirdSearchResult.size)
                earlybirdSearchResult
              case e =>
                statsReceiver.scope("failures").counter(e.getClass.getSimpleName).incr()
                Some(Seq.empty)
            }
          }
      }.getOrElse(Future.None)
  }
}

object EarlybirdSimilarityEngineBase {
  trait EarlybirdSearchQuery {
    def seedUserIds: Seq[UserId]
    def maxNumTweets: Int
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Singleton

@Singleton
case class TwhinCollabFilterSimilarityEngine(
  twhinCandidatesStratoStore: ReadableStore[Long, Seq[TweetId]],
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      TwhinCollabFilterSimilarityEngine.Query,
      Seq[TweetWithScore]
    ] {

  import TwhinCollabFilterSimilarityEngine._
  override def get(
    query: TwhinCollabFilterSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {

    query.sourceId match {
      case InternalId.UserId(userId) =>
        twhinCandidatesStratoStore.get(userId).map {
          _.map {
            _.map { tweetId => TweetWithScore(tweetId, defaultScore) }
          }
        }
      case _ =>
        Future.None
    }
  }
}

object TwhinCollabFilterSimilarityEngine {

  val defaultScore: Double = 1.0

  case class TwhinCollabFilterView(clusterVersion: String)

  case class Query(
    sourceId: InternalId,
  )

  def toSimilarityEngineInfo(
    query: LookupEngineQuery[Query],
    score: Double
  ): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.TwhinCollabFilter,
      modelId = Some(query.lookupKey),
      score = Some(score))
  }

  def fromParams(
    sourceId: InternalId,
    modelId: String,
    params: configapi.Params,
  ): LookupEngineQuery[Query] = {
    LookupEngineQuery(
      Query(sourceId = sourceId),
      modelId,
      params
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.ConsumerBasedWalsParams
import com.twitter.cr_mixer.similarity_engine.ConsumerBasedWalsSimilarityEngine.Query
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Future
import io.grpc.ManagedChannel
import tensorflow.serving.Predict.PredictRequest
import tensorflow.serving.Predict.PredictResponse
import tensorflow.serving.PredictionServiceGrpc
import org.tensorflow.example.Feature
import org.tensorflow.example.Int64List
import org.tensorflow.example.FloatList
import org.tensorflow.example.Features
import org.tensorflow.example.Example
import tensorflow.serving.Model
import org.tensorflow.framework.TensorProto
import org.tensorflow.framework.DataType
import org.tensorflow.framework.TensorShapeProto
import com.twitter.finagle.grpc.FutureConverters
import java.util.ArrayList
import java.lang
import com.twitter.util.Return
import com.twitter.util.Throw
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

// Stats object maintain a set of stats that are specific to the Wals Engine.
case class WalsStats(scope: String, scopedStats: StatsReceiver) {

  val requestStat = scopedStats.scope(scope)
  val inputSignalSize = requestStat.stat("input_signal_size")

  val latency = requestStat.stat("latency_ms")
  val latencyOnError = requestStat.stat("error_latency_ms")
  val latencyOnSuccess = requestStat.stat("success_latency_ms")

  val requests = requestStat.counter("requests")
  val success = requestStat.counter("success")
  val failures = requestStat.scope("failures")

  def onFailure(t: Throwable, startTimeMs: Long) {
    val duration = System.currentTimeMillis() - startTimeMs
    latency.add(duration)
    latencyOnError.add(duration)
    failures.counter(t.getClass.getName).incr()
  }

  def onSuccess(startTimeMs: Long) {
    val duration = System.currentTimeMillis() - startTimeMs
    latency.add(duration)
    latencyOnSuccess.add(duration)
    success.incr()
  }
}

// StatsMap maintains a mapping from Model's input signature to a stats receiver
// The Wals model suports multiple input signature which can run different graphs internally and
// can have a different performance profile.
// Invoking StatsReceiver.stat() on each request can create a new stat object and can be expensive
// in performance critical paths.
object WalsStatsMap {
  val mapping = new ConcurrentHashMap[String, WalsStats]()

  def get(scope: String, scopedStats: StatsReceiver): WalsStats = {
    mapping.computeIfAbsent(scope, (scope) => WalsStats(scope, scopedStats))
  }
}

case class ConsumerBasedWalsSimilarityEngine(
  homeNaviGRPCClient: ManagedChannel,
  adsFavedNaviGRPCClient: ManagedChannel,
  adsMonetizableNaviGRPCClient: ManagedChannel,
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      Query,
      Seq[TweetWithScore]
    ] {

  override def get(
    query: ConsumerBasedWalsSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    val startTimeMs = System.currentTimeMillis()
    val stats =
      WalsStatsMap.get(
        query.wilyNsName + "/" + query.modelSignatureName,
        statsReceiver.scope("NaviPredictionService")
      )
    stats.requests.incr()
    stats.inputSignalSize.add(query.sourceIds.size)
    try {
      // avoid inference calls is source signals are empty
      if (query.sourceIds.isEmpty) {
        Future.value(Some(Seq.empty))
      } else {
        val grpcClient = query.wilyNsName match {
          case "navi-wals-recommended-tweets-home-client" => homeNaviGRPCClient
          case "navi-wals-ads-faved-tweets" => adsFavedNaviGRPCClient
          case "navi-wals-ads-monetizable-tweets" => adsFavedNaviGRPCClient
          // default to homeNaviGRPCClient
          case _ => homeNaviGRPCClient
        }
        val stub = PredictionServiceGrpc.newFutureStub(grpcClient)
        val inferRequest = getModelInput(query)

        FutureConverters
          .RichListenableFuture(stub.predict(inferRequest)).toTwitter
          .transform {
            case Return(resp) =>
              stats.onSuccess(startTimeMs)
              Future.value(Some(getModelOutput(query, resp)))
            case Throw(e) =>
              stats.onFailure(e, startTimeMs)
              Future.exception(e)
          }
      }
    } catch {
      case e: Throwable => Future.exception(e)
    }
  }

  def getFeaturesForRecommendations(query: ConsumerBasedWalsSimilarityEngine.Query): Example = {
    val tweetIds = new ArrayList[lang.Long]()
    val tweetFaveWeight = new ArrayList[lang.Float]()

    query.sourceIds.foreach { sourceInfo =>
      val weight = sourceInfo.sourceType match {
        case SourceType.TweetFavorite | SourceType.Retweet => 1.0f
        // currently no-op - as we do not get negative signals
        case SourceType.TweetDontLike | SourceType.TweetReport | SourceType.AccountMute |
            SourceType.AccountBlock =>
          0.0f
        case _ => 0.0f
      }
      sourceInfo.internalId match {
        case InternalId.TweetId(tweetId) =>
          tweetIds.add(tweetId)
          tweetFaveWeight.add(weight)
        case _ =>
          throw new IllegalArgumentException(
            s"Invalid InternalID - does not contain TweetId for Source Signal: ${sourceInfo}")
      }
    }

    val tweetIdsFeature =
      Feature
        .newBuilder().setInt64List(
          Int64List
            .newBuilder().addAllValue(tweetIds).build()
        ).build()

    val tweetWeightsFeature = Feature
      .newBuilder().setFloatList(
        FloatList.newBuilder().addAllValue(tweetFaveWeight).build()).build()

    val features = Features
      .newBuilder()
      .putFeature("tweet_ids", tweetIdsFeature)
      .putFeature("tweet_weights", tweetWeightsFeature)
      .build()
    Example.newBuilder().setFeatures(features).build()
  }

  def getModelInput(query: ConsumerBasedWalsSimilarityEngine.Query): PredictRequest = {
    val tfExample = getFeaturesForRecommendations(query)

    val inferenceRequest = PredictRequest
      .newBuilder()
      .setModelSpec(
        Model.ModelSpec
          .newBuilder()
          .setName(query.modelName)
          .setSignatureName(query.modelSignatureName))
      .putInputs(
        query.modelInputName,
        TensorProto
          .newBuilder()
          .setDtype(DataType.DT_STRING)
          .setTensorShape(TensorShapeProto
            .newBuilder()
            .addDim(TensorShapeProto.Dim.newBuilder().setSize(1)))
          .addStringVal(tfExample.toByteString)
          .build()
      ).build()
    inferenceRequest
  }

  def getModelOutput(query: Query, response: PredictResponse): Seq[TweetWithScore] = {
    val outputName = query.modelOutputName
    if (response.containsOutputs(outputName)) {
      val tweetList = response.getOutputsMap
        .get(outputName)
        .getInt64ValList.asScala
      tweetList.zip(tweetList.size to 1 by -1).map { (tweetWithScore) =>
        TweetWithScore(tweetWithScore._1, tweetWithScore._2.toLong)
      }
    } else {
      Seq.empty
    }
  }
}

object ConsumerBasedWalsSimilarityEngine {
  case class Query(
    sourceIds: Seq[SourceInfo],
    modelName: String,
    modelInputName: String,
    modelOutputName: String,
    modelSignatureName: String,
    wilyNsName: String,
  )

  def fromParams(
    sourceIds: Seq[SourceInfo],
    params: configapi.Params,
  ): EngineQuery[Query] = {
    EngineQuery(
      Query(
        sourceIds,
        params(ConsumerBasedWalsParams.ModelNameParam),
        params(ConsumerBasedWalsParams.ModelInputNameParam),
        params(ConsumerBasedWalsParams.ModelOutputNameParam),
        params(ConsumerBasedWalsParams.ModelSignatureNameParam),
        params(ConsumerBasedWalsParams.WilyNsNameParam),
      ),
      params
    )
  }

  def toSimilarityEngineInfo(
    score: Double
  ): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.ConsumerBasedWalsANN,
      modelId = None,
      score = Some(score))
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.ann.common.thriftscala.AnnQueryService
import com.twitter.ann.common.thriftscala.Distance
import com.twitter.ann.common.thriftscala.NearestNeighborQuery
import com.twitter.ann.hnsw.HnswCommon
import com.twitter.ann.hnsw.HnswParams
import com.twitter.bijection.Injection
import com.twitter.cortex.ml.embeddings.common.TweetKind
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.MemCacheConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.mediaservices.commons.codec.ArrayByteBufferCodec
import com.twitter.ml.api.thriftscala.{Embedding => ThriftEmbedding}
import com.twitter.ml.featurestore.lib
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi.Params
import com.twitter.util.Future

case class HnswANNEngineQuery(
  modelId: String,
  sourceId: InternalId,
  params: Params,
) {
  val cacheKey: String = s"${modelId}_${sourceId.toString}"
}

/**
 * This Engine looks for tweets whose similarity is close to a Source Dense Embedding.
 * Only support Long based embedding lookup. UserId or TweetId.
 *
 * It provides HNSW specific implementations
 *
 * @param memCacheConfigOpt   If specified, it will wrap the underlying store with a MemCache layer
 *                            You should only enable this for cacheable queries, e.x. TweetIds.
 *                            consumer based UserIds are generally not possible to cache.
 */
class HnswANNSimilarityEngine(
  embeddingStoreLookUpMap: Map[String, ReadableStore[InternalId, ThriftEmbedding]],
  annServiceLookUpMap: Map[String, AnnQueryService.MethodPerEndpoint],
  globalStats: StatsReceiver,
  override val identifier: SimilarityEngineType,
  engineConfig: SimilarityEngineConfig,
  memCacheConfigOpt: Option[MemCacheConfig[HnswANNEngineQuery]] = None)
    extends SimilarityEngine[HnswANNEngineQuery, TweetWithScore] {

  private val MaxNumResults: Int = 200
  private val ef: Int = 800
  private val TweetIdByteInjection: Injection[lib.TweetId, Array[Byte]] = TweetKind.byteInjection

  private val scopedStats = globalStats.scope("similarityEngine", identifier.toString)

  def getScopedStats: StatsReceiver = scopedStats

  private def fetchEmbedding(
    query: HnswANNEngineQuery,
  ): Future[Option[ThriftEmbedding]] = {
    val embeddingStore = embeddingStoreLookUpMap.getOrElse(
      query.modelId,
      throw new IllegalArgumentException(
        s"${this.getClass.getSimpleName} ${identifier.toString}: " +
          s"ModelId ${query.modelId} does not exist for embeddingStore"
      )
    )

    embeddingStore.get(query.sourceId)
  }

  private def fetchCandidates(
    query: HnswANNEngineQuery,
    embedding: ThriftEmbedding
  ): Future[Seq[TweetWithScore]] = {
    val annService = annServiceLookUpMap.getOrElse(
      query.modelId,
      throw new IllegalArgumentException(
        s"${this.getClass.getSimpleName} ${identifier.toString}: " +
          s"ModelId ${query.modelId} does not exist for annStore"
      )
    )

    val hnswParams = HnswCommon.RuntimeParamsInjection.apply(HnswParams(ef))

    val annQuery =
      NearestNeighborQuery(embedding, withDistance = true, hnswParams, MaxNumResults)

    annService
      .query(annQuery)
      .map(
        _.nearestNeighbors
          .map { nearestNeighbor =>
            val candidateId = TweetIdByteInjection
              .invert(ArrayByteBufferCodec.decode(nearestNeighbor.id))
              .toOption
              .map(_.tweetId)
            (candidateId, nearestNeighbor.distance)
          }.collect {
            case (Some(candidateId), Some(distance)) =>
              TweetWithScore(candidateId, toScore(distance))
          })
  }

  // Convert Distance to a score such that higher scores mean more similar.
  def toScore(distance: Distance): Double = {
    distance match {
      case Distance.EditDistance(editDistance) =>
        // (-Infinite, 0.0]
        0.0 - editDistance.distance
      case Distance.L2Distance(l2Distance) =>
        // (-Infinite, 0.0]
        0.0 - l2Distance.distance
      case Distance.CosineDistance(cosineDistance) =>
        // [0.0 - 1.0]
        1.0 - cosineDistance.distance
      case Distance.InnerProductDistance(innerProductDistance) =>
        // (-Infinite, Infinite)
        1.0 - innerProductDistance.distance
      case Distance.UnknownUnionField(_) =>
        throw new IllegalStateException(
          s"${this.getClass.getSimpleName} does not recognize $distance.toString"
        )
    }
  }

  private[similarity_engine] def getEmbeddingAndCandidates(
    query: HnswANNEngineQuery
  ): Future[Option[Seq[TweetWithScore]]] = {

    val fetchEmbeddingStat = scopedStats.scope(query.modelId).scope("fetchEmbedding")
    val fetchCandidatesStat = scopedStats.scope(query.modelId).scope("fetchCandidates")

    for {
      embeddingOpt <- StatsUtil.trackOptionStats(fetchEmbeddingStat) { fetchEmbedding(query) }
      candidates <- StatsUtil.trackItemsStats(fetchCandidatesStat) {

        embeddingOpt match {
          case Some(embedding) => fetchCandidates(query, embedding)
          case None => Future.Nil
        }
      }
    } yield {
      Some(candidates)
    }
  }

  // Add memcache wrapper, if specified
  private val store = {
    val uncachedStore = ReadableStore.fromFnFuture(getEmbeddingAndCandidates)

    memCacheConfigOpt match {
      case Some(config) =>
        SimilarityEngine.addMemCache(
          underlyingStore = uncachedStore,
          memCacheConfig = config,
          statsReceiver = scopedStats
        )
      case _ => uncachedStore
    }
  }

  def toSimilarityEngineInfo(
    query: HnswANNEngineQuery,
    score: Double
  ): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = this.identifier,
      modelId = Some(query.modelId),
      score = Some(score))
  }

  override def getCandidates(
    engineQuery: HnswANNEngineQuery
  ): Future[Option[Seq[TweetWithScore]]] = {
    val versionedStats = globalStats.scope(engineQuery.modelId)
    SimilarityEngine.getFromFn(
      store.get,
      engineQuery,
      engineConfig,
      engineQuery.params,
      versionedStats
    )
  }
}
package com.twitter.cr_mixer.source_signal

import com.twitter.cr_mixer.model.GraphSourceInfo
import com.twitter.cr_mixer.source_signal.SourceFetcher.FetcherQuery
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.UserId
import com.twitter.util.Future

/***
 * A SourceGraphFetcher is a trait that extends from `SourceFetcher`
 * and is specialized in tackling User Graph (eg., RealGraphOon, FRS) fetch.
 *
 * The [[ResultType]] of a SourceGraphFetcher is a `GraphSourceInfo` which contains a userSeedSet.
 * When we pass in userId, the underlying store returns one GraphSourceInfo.
 */
trait SourceGraphFetcher extends SourceFetcher[GraphSourceInfo] {
  protected final val DefaultSeedScore = 1.0
  protected def graphSourceType: SourceType

  /***
   * RawDataType contains a consumers seed UserId and a score (weight)
   */
  protected type RawDataType = (UserId, Double)

  def trackStats(
    query: FetcherQuery
  )(
    func: => Future[Option[GraphSourceInfo]]
  ): Future[Option[GraphSourceInfo]] = {
    val productScopedStats = stats.scope(query.product.originalName)
    val productUserStateScopedStats = productScopedStats.scope(query.userState.toString)
    StatsUtil
      .trackOptionStats(productScopedStats) {
        StatsUtil
          .trackOptionStats(productUserStateScopedStats) {
            func
          }
      }
  }

  // Track per item stats on the fetched graph results
  def trackPerItemStats(
    query: FetcherQuery
  )(
    func: => Future[Option[Seq[RawDataType]]]
  ): Future[Option[Seq[RawDataType]]] = {
    val productScopedStats = stats.scope(query.product.originalName)
    val productUserStateScopedStats = productScopedStats.scope(query.userState.toString)
    StatsUtil.trackOptionItemsStats(productScopedStats) {
      StatsUtil.trackOptionItemsStats(productUserStateScopedStats) {
        func
      }
    }
  }

  /***
   * Convert Seq[RawDataType] into GraphSourceInfo
   */
  protected final def convertGraphSourceInfo(
    userWithScores: Seq[RawDataType]
  ): GraphSourceInfo = {
    GraphSourceInfo(
      sourceType = graphSourceType,
      seedWithScores = userWithScores.map { userWithScore =>
        userWithScore._1 -> userWithScore._2
      }.toMap
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.ann.common.thriftscala.AnnQueryService
import com.twitter.ann.common.thriftscala.Distance
import com.twitter.ann.common.thriftscala.NearestNeighborQuery
import com.twitter.ann.common.thriftscala.NearestNeighborResult
import com.twitter.ann.hnsw.HnswCommon
import com.twitter.ann.hnsw.HnswParams
import com.twitter.bijection.Injection
import com.twitter.conversions.DurationOps._
import com.twitter.cortex.ml.embeddings.common.TweetKind
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.mediaservices.commons.codec.ArrayByteBufferCodec
import com.twitter.ml.api.thriftscala.{Embedding => ThriftEmbedding}
import com.twitter.ml.featurestore.lib
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Duration
import com.twitter.util.Future
import javax.inject.Singleton

/**
 * This store looks for tweets whose similarity is close to a Source Dense Embedding.
 * Only support Long based embedding lookup. UserId or TweetId
 */
@Singleton
class ModelBasedANNStore(
  embeddingStoreLookUpMap: Map[String, ReadableStore[InternalId, ThriftEmbedding]],
  annServiceLookUpMap: Map[String, AnnQueryService.MethodPerEndpoint],
  globalStats: StatsReceiver)
    extends ReadableStore[
      ModelBasedANNStore.Query,
      Seq[TweetWithScore]
    ] {

  import ModelBasedANNStore._

  private val stats = globalStats.scope(this.getClass.getSimpleName)
  private val fetchEmbeddingStat = stats.scope("fetchEmbedding")
  private val fetchCandidatesStat = stats.scope("fetchCandidates")

  override def get(query: Query): Future[Option[Seq[TweetWithScore]]] = {
    for {
      maybeEmbedding <- StatsUtil.trackOptionStats(fetchEmbeddingStat.scope(query.modelId)) {
        fetchEmbedding(query)
      }
      maybeCandidates <- StatsUtil.trackOptionStats(fetchCandidatesStat.scope(query.modelId)) {
        maybeEmbedding match {
          case Some(embedding) =>
            fetchCandidates(query, embedding)
          case None =>
            Future.None
        }
      }
    } yield {
      maybeCandidates.map(
        _.nearestNeighbors
          .map { nearestNeighbor =>
            val candidateId = TweetIdByteInjection
              .invert(ArrayByteBufferCodec.decode(nearestNeighbor.id))
              .toOption
              .map(_.tweetId)
            (candidateId, nearestNeighbor.distance)
          }.collect {
            case (Some(candidateId), Some(distance)) =>
              TweetWithScore(candidateId, toScore(distance))
          })
    }
  }

  private def fetchEmbedding(query: Query): Future[Option[ThriftEmbedding]] = {
    embeddingStoreLookUpMap.get(query.modelId) match {
      case Some(embeddingStore) =>
        embeddingStore.get(query.sourceId)
      case _ =>
        Future.None
    }
  }

  private def fetchCandidates(
    query: Query,
    embedding: ThriftEmbedding
  ): Future[Option[NearestNeighborResult]] = {
    val hnswParams = HnswCommon.RuntimeParamsInjection.apply(HnswParams(query.ef))

    annServiceLookUpMap.get(query.modelId) match {
      case Some(annService) =>
        val annQuery =
          NearestNeighborQuery(embedding, withDistance = true, hnswParams, MaxNumResults)
        annService.query(annQuery).map(v => Some(v))
      case _ =>
        Future.None
    }
  }
}

object ModelBasedANNStore {

  val MaxNumResults: Int = 200
  val MaxTweetCandidateAge: Duration = 1.day

  val TweetIdByteInjection: Injection[lib.TweetId, Array[Byte]] = TweetKind.byteInjection

  // For more information about HNSW algorithm: https://docbird.twitter.biz/ann/hnsw.html
  case class Query(
    sourceId: InternalId,
    modelId: String,
    similarityEngineType: SimilarityEngineType,
    ef: Int = 800)

  def toScore(distance: Distance): Double = {
    distance match {
      case Distance.L2Distance(l2Distance) =>
        // (-Infinite, 0.0]
        0.0 - l2Distance.distance
      case Distance.CosineDistance(cosineDistance) =>
        // [0.0 - 1.0]
        1.0 - cosineDistance.distance
      case Distance.InnerProductDistance(innerProductDistance) =>
        // (-Infinite, Infinite)
        1.0 - innerProductDistance.distance
      case _ =>
        0.0
    }
  }
  def toSimilarityEngineInfo(query: Query, score: Double): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = query.similarityEngineType,
      modelId = Some(query.modelId),
      score = Some(score))
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.ConsumersBasedUserVideoGraphParams
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.recos.user_video_graph.thriftscala.ConsumersBasedRelatedTweetRequest
import com.twitter.recos.user_video_graph.thriftscala.RelatedTweetResponse
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Singleton

/**
 * This store uses the graph based input (a list of userIds)
 * to query consumersBasedUserVideoGraph and get their top engaged tweets
 */
@Singleton
case class ConsumersBasedUserVideoGraphSimilarityEngine(
  consumersBasedUserVideoGraphStore: ReadableStore[
    ConsumersBasedRelatedTweetRequest,
    RelatedTweetResponse
  ],
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      ConsumersBasedUserVideoGraphSimilarityEngine.Query,
      Seq[TweetWithScore]
    ] {

  override def get(
    query: ConsumersBasedUserVideoGraphSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    val consumersBasedRelatedTweetRequest =
      ConsumersBasedRelatedTweetRequest(
        query.seedWithScores.keySet.toSeq,
        maxResults = Some(query.maxResults),
        minCooccurrence = Some(query.minCooccurrence),
        minScore = Some(query.minScore),
        maxTweetAgeInHours = Some(query.maxTweetAgeInHours)
      )
    consumersBasedUserVideoGraphStore
      .get(consumersBasedRelatedTweetRequest)
      .map { relatedTweetResponseOpt =>
        relatedTweetResponseOpt.map { relatedTweetResponse =>
          relatedTweetResponse.tweets.map { tweet =>
            TweetWithScore(tweet.tweetId, tweet.score)
          }
        }
      }
  }
}

object ConsumersBasedUserVideoGraphSimilarityEngine {

  case class Query(
    seedWithScores: Map[UserId, Double],
    maxResults: Int,
    minCooccurrence: Int,
    minScore: Double,
    maxTweetAgeInHours: Int)

  def toSimilarityEngineInfo(
    score: Double
  ): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.ConsumersBasedUserVideoGraph,
      modelId = None,
      score = Some(score))
  }

  def fromParamsForRealGraphIn(
    seedWithScores: Map[UserId, Double],
    params: configapi.Params,
  ): EngineQuery[Query] = {

    EngineQuery(
      Query(
        seedWithScores = seedWithScores,
        maxResults = params(GlobalParams.MaxCandidateNumPerSourceKeyParam),
        minCooccurrence =
          params(ConsumersBasedUserVideoGraphParams.RealGraphInMinCoOccurrenceParam),
        minScore = params(ConsumersBasedUserVideoGraphParams.RealGraphInMinScoreParam),
        maxTweetAgeInHours = params(GlobalParams.MaxTweetAgeHoursParam).inHours
      ),
      params
    )
  }
}
