package com.twitter.cr_mixer.similarity_engine

import com.google.inject.Inject
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TopicTweetWithScore
import com.twitter.cr_mixer.param.TopicTweetParams
import com.twitter.cr_mixer.similarity_engine.CertoTopicTweetSimilarityEngine._
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.thriftscala.TopicId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.topic_recos.thriftscala._
import com.twitter.util.Future

@Singleton
case class CertoTopicTweetSimilarityEngine @Inject() (
  @Named(ModuleNames.CertoStratoStoreName) certoStratoStore: ReadableStore[
    TopicId,
    Seq[TweetWithScores]
  ],
  statsReceiver: StatsReceiver)
    extends ReadableStore[EngineQuery[Query], Seq[TopicTweetWithScore]] {

  private val name: String = this.getClass.getSimpleName
  private val stats = statsReceiver.scope(name)

  override def get(query: EngineQuery[Query]): Future[Option[Seq[TopicTweetWithScore]]] = {
    StatsUtil.trackOptionItemsStats(stats) {
      topTweetsByFollowerL2NormalizedScore.get(query).map {
        _.map { topicTopTweets =>
          topicTopTweets.map { topicTweet =>
            TopicTweetWithScore(
              tweetId = topicTweet.tweetId,
              score = topicTweet.scores.followerL2NormalizedCosineSimilarity8HrHalfLife,
              similarityEngineType = SimilarityEngineType.CertoTopicTweet
            )
          }
        }
      }
    }
  }

  private val topTweetsByFollowerL2NormalizedScore: ReadableStore[EngineQuery[Query], Seq[
    TweetWithScores
  ]] = {
    ReadableStore.fromFnFuture { query: EngineQuery[Query] =>
      StatsUtil.trackOptionItemsStats(stats) {
        for {
          topKTweetsWithScores <- certoStratoStore.get(query.storeQuery.topicId)
        } yield {
          topKTweetsWithScores.map(
            _.filter(
              _.scores.followerL2NormalizedCosineSimilarity8HrHalfLife >= query.storeQuery.certoScoreTheshold)
              .take(query.storeQuery.maxCandidates))
        }
      }
    }
  }
}

object CertoTopicTweetSimilarityEngine {

  // Query is used as a cache key. Do not add any user level information in this.
  case class Query(
    topicId: TopicId,
    maxCandidates: Int,
    certoScoreTheshold: Double)

  def fromParams(
    topicId: TopicId,
    isVideoOnly: Boolean,
    params: configapi.Params,
  ): EngineQuery[Query] = {

    val maxCandidates = if (isVideoOnly) {
      params(TopicTweetParams.MaxCertoCandidatesParam) * 2
    } else {
      params(TopicTweetParams.MaxCertoCandidatesParam)
    }

    EngineQuery(
      Query(
        topicId = topicId,
        maxCandidates = maxCandidates,
        certoScoreTheshold = params(TopicTweetParams.CertoScoreThresholdParam)
      ),
      params
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.param.TweetBasedUserAdGraphParams
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.recos.user_ad_graph.thriftscala.ConsumersBasedRelatedAdRequest
import com.twitter.recos.user_ad_graph.thriftscala.RelatedAdResponse
import com.twitter.recos.user_ad_graph.thriftscala.UserAdGraph
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.twistly.thriftscala.TweetRecentEngagedUsers
import com.twitter.util.Future
import javax.inject.Singleton

/**
 * This store looks for similar tweets from UserAdGraph for a Source TweetId
 * For a query tweet,User Ad Graph (UAG)
 * lets us find out which other tweets share a lot of the same engagers with the query tweet
 */
@Singleton
case class TweetBasedUserAdGraphSimilarityEngine(
  userAdGraphService: UserAdGraph.MethodPerEndpoint,
  tweetEngagedUsersStore: ReadableStore[TweetId, TweetRecentEngagedUsers],
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      TweetBasedUserAdGraphSimilarityEngine.Query,
      Seq[TweetWithScore]
    ] {

  import TweetBasedUserAdGraphSimilarityEngine._

  private val stats = statsReceiver.scope(this.getClass.getSimpleName)
  private val fetchCoverageExpansionCandidatesStat = stats.scope("fetchCoverageExpansionCandidates")
  override def get(
    query: TweetBasedUserAdGraphSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    query.sourceId match {
      case InternalId.TweetId(tweetId) => getCandidates(tweetId, query)
      case _ =>
        Future.value(None)
    }
  }

  // We first fetch tweet's recent engaged users as consumeSeedSet from MH store,
  // then query consumersBasedUTG using the consumerSeedSet
  private def getCandidates(
    tweetId: TweetId,
    query: TweetBasedUserAdGraphSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    StatsUtil
      .trackOptionItemsStats(fetchCoverageExpansionCandidatesStat) {
        tweetEngagedUsersStore
          .get(tweetId).flatMap {
            _.map { tweetRecentEngagedUsers =>
              val consumerSeedSet =
                tweetRecentEngagedUsers.recentEngagedUsers
                  .map { _.userId }.take(query.maxConsumerSeedsNum)
              val consumersBasedRelatedAdRequest =
                ConsumersBasedRelatedAdRequest(
                  consumerSeedSet = consumerSeedSet,
                  maxResults = Some(query.maxResults),
                  minCooccurrence = Some(query.minCooccurrence),
                  excludeTweetIds = Some(Seq(tweetId)),
                  minScore = Some(query.consumersBasedMinScore),
                  maxTweetAgeInHours = Some(query.maxTweetAgeInHours)
                )
              toTweetWithScore(userAdGraphService
                .consumersBasedRelatedAds(consumersBasedRelatedAdRequest).map { Some(_) })
            }.getOrElse(Future.value(None))
          }
      }
  }

}

object TweetBasedUserAdGraphSimilarityEngine {

  def toSimilarityEngineInfo(score: Double): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.TweetBasedUserAdGraph,
      modelId = None,
      score = Some(score))
  }
  private def toTweetWithScore(
    relatedAdResponseFut: Future[Option[RelatedAdResponse]]
  ): Future[Option[Seq[TweetWithScore]]] = {
    relatedAdResponseFut.map { relatedAdResponseOpt =>
      relatedAdResponseOpt.map { relatedAdResponse =>
        val candidates =
          relatedAdResponse.adTweets.map(tweet => TweetWithScore(tweet.adTweetId, tweet.score))

        candidates
      }
    }
  }

  case class Query(
    sourceId: InternalId,
    maxResults: Int,
    minCooccurrence: Int,
    consumersBasedMinScore: Double,
    maxTweetAgeInHours: Int,
    maxConsumerSeedsNum: Int,
  )

  def fromParams(
    sourceId: InternalId,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    EngineQuery(
      Query(
        sourceId = sourceId,
        maxResults = params(GlobalParams.MaxCandidateNumPerSourceKeyParam),
        minCooccurrence = params(TweetBasedUserAdGraphParams.MinCoOccurrenceParam),
        consumersBasedMinScore = params(TweetBasedUserAdGraphParams.ConsumersBasedMinScoreParam),
        maxTweetAgeInHours = params(GlobalParams.MaxTweetAgeHoursParam).inHours,
        maxConsumerSeedsNum = params(TweetBasedUserAdGraphParams.MaxConsumerSeedsNumParam),
      ),
      params
    )
  }

}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.simclusters_v2.thriftscala.TweetsWithScore
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Singleton

@Singleton
case class DiffusionBasedSimilarityEngine(
  retweetBasedDiffusionRecsMhStore: ReadableStore[Long, TweetsWithScore],
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      DiffusionBasedSimilarityEngine.Query,
      Seq[TweetWithScore]
    ] {

  override def get(
    query: DiffusionBasedSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {

    query.sourceId match {
      case InternalId.UserId(userId) =>
        retweetBasedDiffusionRecsMhStore.get(userId).map {
          _.map { tweetsWithScore =>
            {
              tweetsWithScore.tweets
                .map(tweet => TweetWithScore(tweet.tweetId, tweet.score))
            }
          }
        }
      case _ =>
        Future.None
    }
  }
}

object DiffusionBasedSimilarityEngine {

  val defaultScore: Double = 0.0

  case class Query(
    sourceId: InternalId,
  )

  def toSimilarityEngineInfo(
    query: LookupEngineQuery[Query],
    score: Double
  ): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.DiffusionBasedTweet,
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
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.param.TweetBasedUserVideoGraphParams
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.recos.user_video_graph.thriftscala.RelatedTweetResponse
import com.twitter.recos.user_video_graph.thriftscala.ConsumersBasedRelatedTweetRequest
import com.twitter.recos.user_video_graph.thriftscala.TweetBasedRelatedTweetRequest
import com.twitter.recos.user_video_graph.thriftscala.UserVideoGraph
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.timelines.configapi
import com.twitter.twistly.thriftscala.TweetRecentEngagedUsers
import com.twitter.util.Duration
import javax.inject.Singleton
import com.twitter.util.Future
import com.twitter.util.Time
import scala.concurrent.duration.HOURS

/**
 * This store looks for similar tweets from UserVideoGraph for a Source TweetId
 * For a query tweet,User Video Graph (UVG),
 * lets us find out which other video tweets share a lot of the same engagers with the query tweet
 */
@Singleton
case class TweetBasedUserVideoGraphSimilarityEngine(
  userVideoGraphService: UserVideoGraph.MethodPerEndpoint,
  tweetEngagedUsersStore: ReadableStore[TweetId, TweetRecentEngagedUsers],
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      TweetBasedUserVideoGraphSimilarityEngine.Query,
      Seq[TweetWithScore]
    ] {

  import TweetBasedUserVideoGraphSimilarityEngine._

  private val stats = statsReceiver.scope(this.getClass.getSimpleName)
  private val fetchCandidatesStat = stats.scope("fetchCandidates")
  private val fetchCoverageExpansionCandidatesStat = stats.scope("fetchCoverageExpansionCandidates")

  override def get(
    query: TweetBasedUserVideoGraphSimilarityEngine.Query
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

  private def getCandidates(
    tweetId: TweetId,
    query: TweetBasedUserVideoGraphSimilarityEngine.Query
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
        userVideoGraphService.tweetBasedRelatedTweets(tweetBasedRelatedTweetRequest).map {
          Some(_)
        })
    }
  }

  private def getCoverageExpansionCandidates(
    tweetId: TweetId,
    query: TweetBasedUserVideoGraphSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    StatsUtil
      .trackOptionItemsStats(fetchCoverageExpansionCandidatesStat) {
        tweetEngagedUsersStore
          .get(tweetId).flatMap {
            _.map { tweetRecentEngagedUsers =>
              val consumerSeedSet =
                tweetRecentEngagedUsers.recentEngagedUsers
                  .map {
                    _.userId
                  }.take(query.maxConsumerSeedsNum)
              val consumersBasedRelatedTweetRequest =
                ConsumersBasedRelatedTweetRequest(
                  consumerSeedSet = consumerSeedSet,
                  maxResults = Some(query.maxResults),
                  minCooccurrence = Some(query.minCooccurrence),
                  excludeTweetIds = Some(Seq(tweetId)),
                  minScore = Some(query.consumersBasedMinScore),
                  maxTweetAgeInHours = Some(query.maxTweetAgeInHours)
                )

              toTweetWithScore(userVideoGraphService
                .consumersBasedRelatedTweets(consumersBasedRelatedTweetRequest).map {
                  Some(_)
                })
            }.getOrElse(Future.value(None))
          }
      }
  }

}

object TweetBasedUserVideoGraphSimilarityEngine {

  private val oldTweetCap: Duration = Duration(24, HOURS)

  def toSimilarityEngineInfo(score: Double): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.TweetBasedUserVideoGraph,
      modelId = None,
      score = Some(score))
  }

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
    enableCoverageExpansionAllTweet: Boolean)

  def fromParams(
    sourceId: InternalId,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    EngineQuery(
      Query(
        sourceId = sourceId,
        maxResults = params(GlobalParams.MaxCandidateNumPerSourceKeyParam),
        minCooccurrence = params(TweetBasedUserVideoGraphParams.MinCoOccurrenceParam),
        tweetBasedMinScore = params(TweetBasedUserVideoGraphParams.TweetBasedMinScoreParam),
        consumersBasedMinScore = params(TweetBasedUserVideoGraphParams.ConsumersBasedMinScoreParam),
        maxTweetAgeInHours = params(GlobalParams.MaxTweetAgeHoursParam).inHours,
        maxConsumerSeedsNum = params(TweetBasedUserVideoGraphParams.MaxConsumerSeedsNumParam),
        enableCoverageExpansionOldTweet =
          params(TweetBasedUserVideoGraphParams.EnableCoverageExpansionOldTweetParam),
        enableCoverageExpansionAllTweet =
          params(TweetBasedUserVideoGraphParams.EnableCoverageExpansionAllTweetParam)
      ),
      params
    )
  }

}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.param.ProducerBasedUserAdGraphParams
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.recos.user_ad_graph.thriftscala.ProducerBasedRelatedAdRequest
import com.twitter.recos.user_ad_graph.thriftscala.UserAdGraph
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import javax.inject.Singleton
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.timelines.configapi

/**
 * This store looks for similar tweets from UserAdGraph for a Source ProducerId
 * For a query producerId,User Tweet Graph (UAG),
 * lets us find out which ad tweets the query producer's followers co-engaged
 */
@Singleton
case class ProducerBasedUserAdGraphSimilarityEngine(
  userAdGraphService: UserAdGraph.MethodPerEndpoint,
  statsReceiver: StatsReceiver)
    extends ReadableStore[ProducerBasedUserAdGraphSimilarityEngine.Query, Seq[
      TweetWithScore
    ]] {

  private val stats = statsReceiver.scope(this.getClass.getSimpleName)
  private val fetchCandidatesStat = stats.scope("fetchCandidates")

  override def get(
    query: ProducerBasedUserAdGraphSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    query.sourceId match {
      case InternalId.UserId(producerId) =>
        StatsUtil.trackOptionItemsStats(fetchCandidatesStat) {
          val relatedAdRequest =
            ProducerBasedRelatedAdRequest(
              producerId,
              maxResults = Some(query.maxResults),
              minCooccurrence = Some(query.minCooccurrence),
              minScore = Some(query.minScore),
              maxNumFollowers = Some(query.maxNumFollowers),
              maxTweetAgeInHours = Some(query.maxTweetAgeInHours),
            )

          userAdGraphService.producerBasedRelatedAds(relatedAdRequest).map { relatedAdResponse =>
            val candidates =
              relatedAdResponse.adTweets.map(tweet => TweetWithScore(tweet.adTweetId, tweet.score))
            Some(candidates)
          }
        }
      case _ =>
        Future.value(None)
    }
  }
}

object ProducerBasedUserAdGraphSimilarityEngine {

  def toSimilarityEngineInfo(score: Double): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.ProducerBasedUserAdGraph,
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
        minCooccurrence = params(ProducerBasedUserAdGraphParams.MinCoOccurrenceParam),
        maxNumFollowers = params(ProducerBasedUserAdGraphParams.MaxNumFollowersParam),
        maxTweetAgeInHours = params(GlobalParams.MaxTweetAgeHoursParam).inHours,
        minScore = params(ProducerBasedUserAdGraphParams.MinScoreParam)
      ),
      params
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.TweetWithAuthor
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future

class EarlybirdSimilarityEngine[
  Query,
  EarlybirdSimilarityEngineStore <: ReadableStore[Query, Seq[TweetWithAuthor]]
](
  implementingStore: EarlybirdSimilarityEngineStore,
  override val identifier: SimilarityEngineType,
  globalStats: StatsReceiver,
  engineConfig: SimilarityEngineConfig,
) extends SimilarityEngine[EngineQuery[Query], TweetWithAuthor] {
  private val scopedStats = globalStats.scope("similarityEngine", identifier.toString)

  def getScopedStats: StatsReceiver = scopedStats

  def getCandidates(query: EngineQuery[Query]): Future[Option[Seq[TweetWithAuthor]]] = {
    SimilarityEngine.getFromFn(
      implementingStore.get,
      query.storeQuery,
      engineConfig,
      query.params,
      scopedStats
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.param.ConsumerEmbeddingBasedTwHINParams
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.timelines.configapi

object ConsumerEmbeddingBasedTwHINSimilarityEngine {
  def fromParams(
    sourceId: InternalId,
    params: configapi.Params,
  ): HnswANNEngineQuery = {
    HnswANNEngineQuery(
      sourceId = sourceId,
      modelId = params(ConsumerEmbeddingBasedTwHINParams.ModelIdParam),
      params = params
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.param.RelatedTweetTweetBasedParams
import com.twitter.cr_mixer.param.RelatedVideoTweetTweetBasedParams
import com.twitter.cr_mixer.param.SimClustersANNParams
import com.twitter.cr_mixer.param.TweetBasedCandidateGenerationParams
import com.twitter.cr_mixer.param.TweetBasedTwHINParams
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.cr_mixer.util.InterleaveUtil
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.ModelVersions
import com.twitter.simclusters_v2.thriftscala.EmbeddingType
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import javax.inject.Named
import javax.inject.Singleton
import scala.collection.mutable.ArrayBuffer

/**
 * This store fetches similar tweets from multiple tweet based candidate sources
 * and combines them using different methods obtained from query params
 */
@Singleton
case class TweetBasedUnifiedSimilarityEngine(
  @Named(ModuleNames.TweetBasedUserTweetGraphSimilarityEngine)
  tweetBasedUserTweetGraphSimilarityEngine: StandardSimilarityEngine[
    TweetBasedUserTweetGraphSimilarityEngine.Query,
    TweetWithScore
  ],
  @Named(ModuleNames.TweetBasedUserVideoGraphSimilarityEngine)
  tweetBasedUserVideoGraphSimilarityEngine: StandardSimilarityEngine[
    TweetBasedUserVideoGraphSimilarityEngine.Query,
    TweetWithScore
  ],
  simClustersANNSimilarityEngine: StandardSimilarityEngine[
    SimClustersANNSimilarityEngine.Query,
    TweetWithScore
  ],
  @Named(ModuleNames.TweetBasedQigSimilarityEngine)
  tweetBasedQigSimilarTweetsSimilarityEngine: StandardSimilarityEngine[
    TweetBasedQigSimilarityEngine.Query,
    TweetWithScore
  ],
  @Named(ModuleNames.TweetBasedTwHINANNSimilarityEngine)
  tweetBasedTwHINANNSimilarityEngine: HnswANNSimilarityEngine,
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      TweetBasedUnifiedSimilarityEngine.Query,
      Seq[TweetWithCandidateGenerationInfo]
    ] {

  import TweetBasedUnifiedSimilarityEngine._
  private val stats = statsReceiver.scope(this.getClass.getSimpleName)
  private val fetchCandidatesStat = stats.scope("fetchCandidates")

  override def get(
    query: Query
  ): Future[Option[Seq[TweetWithCandidateGenerationInfo]]] = {

    query.sourceInfo.internalId match {
      case _: InternalId.TweetId =>
        StatsUtil.trackOptionItemsStats(fetchCandidatesStat) {
          val twhinQuery =
            HnswANNEngineQuery(
              sourceId = query.sourceInfo.internalId,
              modelId = query.twhinModelId,
              params = query.params)
          val utgCandidatesFut =
            if (query.enableUtg)
              tweetBasedUserTweetGraphSimilarityEngine.getCandidates(query.utgQuery)
            else Future.None

          val uvgCandidatesFut =
            if (query.enableUvg)
              tweetBasedUserVideoGraphSimilarityEngine.getCandidates(query.uvgQuery)
            else Future.None

          val sannCandidatesFut = if (query.enableSimClustersANN) {
            simClustersANNSimilarityEngine.getCandidates(query.simClustersANNQuery)
          } else Future.None

          val sann1CandidatesFut =
            if (query.enableSimClustersANN1) {
              simClustersANNSimilarityEngine.getCandidates(query.simClustersANN1Query)
            } else Future.None

          val sann2CandidatesFut =
            if (query.enableSimClustersANN2) {
              simClustersANNSimilarityEngine.getCandidates(query.simClustersANN2Query)
            } else Future.None

          val sann3CandidatesFut =
            if (query.enableSimClustersANN3) {
              simClustersANNSimilarityEngine.getCandidates(query.simClustersANN3Query)
            } else Future.None

          val sann5CandidatesFut =
            if (query.enableSimClustersANN5) {
              simClustersANNSimilarityEngine.getCandidates(query.simClustersANN5Query)
            } else Future.None

          val sann4CandidatesFut =
            if (query.enableSimClustersANN4) {
              simClustersANNSimilarityEngine.getCandidates(query.simClustersANN4Query)
            } else Future.None

          val experimentalSANNCandidatesFut =
            if (query.enableExperimentalSimClustersANN) {
              simClustersANNSimilarityEngine.getCandidates(query.experimentalSimClustersANNQuery)
            } else Future.None

          val qigCandidatesFut =
            if (query.enableQig)
              tweetBasedQigSimilarTweetsSimilarityEngine.getCandidates(query.qigQuery)
            else Future.None

          val twHINCandidateFut = if (query.enableTwHIN) {
            tweetBasedTwHINANNSimilarityEngine.getCandidates(twhinQuery)
          } else Future.None

          Future
            .join(
              utgCandidatesFut,
              sannCandidatesFut,
              sann1CandidatesFut,
              sann2CandidatesFut,
              sann3CandidatesFut,
              sann5CandidatesFut,
              sann4CandidatesFut,
              experimentalSANNCandidatesFut,
              qigCandidatesFut,
              twHINCandidateFut,
              uvgCandidatesFut
            ).map {
              case (
                    userTweetGraphCandidates,
                    simClustersANNCandidates,
                    simClustersANN1Candidates,
                    simClustersANN2Candidates,
                    simClustersANN3Candidates,
                    simClustersANN5Candidates,
                    simClustersANN4Candidates,
                    experimentalSANNCandidates,
                    qigSimilarTweetsCandidates,
                    twhinCandidates,
                    userVideoGraphCandidates) =>
                val filteredUTGTweets =
                  userTweetGraphFilter(userTweetGraphCandidates.toSeq.flatten)
                val filteredUVGTweets =
                  userVideoGraphFilter(userVideoGraphCandidates.toSeq.flatten)
                val filteredSANNTweets = simClustersCandidateMinScoreFilter(
                  simClustersANNCandidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANNQuery.storeQuery.simClustersANNConfigId)

                val filteredSANN1Tweets = simClustersCandidateMinScoreFilter(
                  simClustersANN1Candidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANN1Query.storeQuery.simClustersANNConfigId)

                val filteredSANN2Tweets = simClustersCandidateMinScoreFilter(
                  simClustersANN2Candidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANN2Query.storeQuery.simClustersANNConfigId)

                val filteredSANN3Tweets = simClustersCandidateMinScoreFilter(
                  simClustersANN3Candidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANN3Query.storeQuery.simClustersANNConfigId)

                val filteredSANN4Tweets = simClustersCandidateMinScoreFilter(
                  simClustersANN4Candidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANN4Query.storeQuery.simClustersANNConfigId)

                val filteredSANN5Tweets = simClustersCandidateMinScoreFilter(
                  simClustersANN5Candidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANN5Query.storeQuery.simClustersANNConfigId)

                val filteredExperimentalSANNTweets = simClustersCandidateMinScoreFilter(
                  experimentalSANNCandidates.toSeq.flatten,
                  query.simClustersVideoBasedMinScore,
                  query.experimentalSimClustersANNQuery.storeQuery.simClustersANNConfigId)

                val filteredQigTweets = qigSimilarTweetsFilter(
                  qigSimilarTweetsCandidates.toSeq.flatten,
                  query.qigMaxTweetAgeHours,
                  query.qigMaxNumSimilarTweets
                )

                val filteredTwHINTweets = twhinFilter(
                  twhinCandidates.toSeq.flatten.sortBy(-_.score),
                  query.twhinMaxTweetAgeHours,
                  tweetBasedTwHINANNSimilarityEngine.getScopedStats
                )
                val utgTweetsWithCGInfo = filteredUTGTweets.map { tweetWithScore =>
                  val similarityEngineInfo = TweetBasedUserTweetGraphSimilarityEngine
                    .toSimilarityEngineInfo(tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }

                val uvgTweetsWithCGInfo = filteredUVGTweets.map { tweetWithScore =>
                  val similarityEngineInfo = TweetBasedUserVideoGraphSimilarityEngine
                    .toSimilarityEngineInfo(tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }
                val sannTweetsWithCGInfo = filteredSANNTweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANNQuery, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }
                val sann1TweetsWithCGInfo = filteredSANN1Tweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANN1Query, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }
                val sann2TweetsWithCGInfo = filteredSANN2Tweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANN2Query, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }
                val sann3TweetsWithCGInfo = filteredSANN3Tweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANN3Query, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }
                val sann4TweetsWithCGInfo = filteredSANN4Tweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANN4Query, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }
                val sann5TweetsWithCGInfo = filteredSANN5Tweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANN5Query, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }

                val experimentalSANNTweetsWithCGInfo = filteredExperimentalSANNTweets.map {
                  tweetWithScore =>
                    val similarityEngineInfo = SimClustersANNSimilarityEngine
                      .toSimilarityEngineInfo(
                        query.experimentalSimClustersANNQuery,
                        tweetWithScore.score)
                    TweetWithCandidateGenerationInfo(
                      tweetWithScore.tweetId,
                      CandidateGenerationInfo(
                        Some(query.sourceInfo),
                        similarityEngineInfo,
                        Seq(similarityEngineInfo)
                      ))
                }
                val qigTweetsWithCGInfo = filteredQigTweets.map { tweetWithScore =>
                  val similarityEngineInfo = TweetBasedQigSimilarityEngine
                    .toSimilarityEngineInfo(tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }

                val twHINTweetsWithCGInfo = filteredTwHINTweets.map { tweetWithScore =>
                  val similarityEngineInfo = tweetBasedTwHINANNSimilarityEngine
                    .toSimilarityEngineInfo(twhinQuery, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }

                val candidateSourcesToBeInterleaved =
                  ArrayBuffer[Seq[TweetWithCandidateGenerationInfo]](
                    sannTweetsWithCGInfo,
                    experimentalSANNTweetsWithCGInfo,
                    sann1TweetsWithCGInfo,
                    sann2TweetsWithCGInfo,
                    sann3TweetsWithCGInfo,
                    sann5TweetsWithCGInfo,
                    sann4TweetsWithCGInfo,
                    qigTweetsWithCGInfo,
                    uvgTweetsWithCGInfo,
                    utgTweetsWithCGInfo,
                    twHINTweetsWithCGInfo
                  )

                val interleavedCandidates =
                  InterleaveUtil.interleave(candidateSourcesToBeInterleaved)

                val unifiedCandidatesWithUnifiedCGInfo =
                  interleavedCandidates.map { candidate =>
                    /***
                     * when a candidate was made by interleave/keepGivenOrder,
                     * then we apply getTweetBasedUnifiedCGInfo() to override with the unified CGInfo
                     *
                     * we'll not have ALL SEs that generated the tweet
                     * in contributingSE list for interleave. We only have the chosen SE available.
                     */
                    TweetWithCandidateGenerationInfo(
                      tweetId = candidate.tweetId,
                      candidateGenerationInfo = getTweetBasedUnifiedCGInfo(
                        candidate.candidateGenerationInfo.sourceInfoOpt,
                        candidate.getSimilarityScore,
                        candidate.candidateGenerationInfo.contributingSimilarityEngines
                      ) // getSimilarityScore comes from either unifiedScore or single score
                    )
                  }
                stats
                  .stat("unified_candidate_size").add(unifiedCandidatesWithUnifiedCGInfo.size)

                val truncatedCandidates =
                  unifiedCandidatesWithUnifiedCGInfo.take(query.maxCandidateNumPerSourceKey)
                stats.stat("truncatedCandidates_size").add(truncatedCandidates.size)

                Some(truncatedCandidates)
            }
        }

      case _ =>
        stats.counter("sourceId_is_not_tweetId_cnt").incr()
        Future.None
    }
  }

  private def simClustersCandidateMinScoreFilter(
    simClustersAnnCandidates: Seq[TweetWithScore],
    simClustersMinScore: Double,
    simClustersANNConfigId: String
  ): Seq[TweetWithScore] = {
    val filteredCandidates = simClustersAnnCandidates
      .filter { candidate =>
        candidate.score > simClustersMinScore
      }

    stats.stat(simClustersANNConfigId, "simClustersAnnCandidates_size").add(filteredCandidates.size)
    stats.counter(simClustersANNConfigId, "simClustersAnnRequests").incr()
    if (filteredCandidates.isEmpty)
      stats.counter(simClustersANNConfigId, "emptyFilteredSimClustersAnnCandidates").incr()

    filteredCandidates.map { candidate =>
      TweetWithScore(candidate.tweetId, candidate.score)
    }
  }

  /** Returns a list of tweets that are generated less than `maxTweetAgeHours` hours ago */
  private def tweetAgeFilter(
    candidates: Seq[TweetWithScore],
    maxTweetAgeHours: Duration
  ): Seq[TweetWithScore] = {
    // Tweet IDs are approximately chronological (see http://go/snowflake),
    // so we are building the earliest tweet id once
    // The per-candidate logic here then be candidate.tweetId > earliestPermittedTweetId, which is far cheaper.
    val earliestTweetId = SnowflakeId.firstIdFor(Time.now - maxTweetAgeHours)
    candidates.filter { candidate => candidate.tweetId >= earliestTweetId }
  }

  private def twhinFilter(
    twhinCandidates: Seq[TweetWithScore],
    twhinMaxTweetAgeHours: Duration,
    simEngineStats: StatsReceiver
  ): Seq[TweetWithScore] = {
    simEngineStats.stat("twhinCandidates_size").add(twhinCandidates.size)
    val candidates = twhinCandidates.map { candidate =>
      TweetWithScore(candidate.tweetId, candidate.score)
    }

    val filteredCandidates = tweetAgeFilter(candidates, twhinMaxTweetAgeHours)
    simEngineStats.stat("filteredTwhinCandidates_size").add(filteredCandidates.size)
    if (filteredCandidates.isEmpty) simEngineStats.counter("emptyFilteredTwhinCandidates").incr()

    filteredCandidates
  }

  /** A no-op filter as UTG filtering already happens on UTG service side */
  private def userTweetGraphFilter(
    userTweetGraphCandidates: Seq[TweetWithScore]
  ): Seq[TweetWithScore] = {
    val filteredCandidates = userTweetGraphCandidates

    stats.stat("userTweetGraphCandidates_size").add(userTweetGraphCandidates.size)
    if (filteredCandidates.isEmpty) stats.counter("emptyFilteredUserTweetGraphCandidates").incr()

    filteredCandidates.map { candidate =>
      TweetWithScore(candidate.tweetId, candidate.score)
    }
  }

  /** A no-op filter as UVG filtering already happens on UVG service side */
  private def userVideoGraphFilter(
    userVideoGraphCandidates: Seq[TweetWithScore]
  ): Seq[TweetWithScore] = {
    val filteredCandidates = userVideoGraphCandidates

    stats.stat("userVideoGraphCandidates_size").add(userVideoGraphCandidates.size)
    if (filteredCandidates.isEmpty) stats.counter("emptyFilteredUserVideoGraphCandidates").incr()

    filteredCandidates.map { candidate =>
      TweetWithScore(candidate.tweetId, candidate.score)
    }
  }
  private def qigSimilarTweetsFilter(
    qigSimilarTweetsCandidates: Seq[TweetWithScore],
    qigMaxTweetAgeHours: Duration,
    qigMaxNumSimilarTweets: Int
  ): Seq[TweetWithScore] = {
    val ageFilteredCandidates = tweetAgeFilter(qigSimilarTweetsCandidates, qigMaxTweetAgeHours)
    stats.stat("ageFilteredQigSimilarTweetsCandidates_size").add(ageFilteredCandidates.size)

    val filteredCandidates = ageFilteredCandidates.take(qigMaxNumSimilarTweets)
    if (filteredCandidates.isEmpty) stats.counter("emptyFilteredQigSimilarTweetsCandidates").incr()

    filteredCandidates
  }

  /***
   * Every candidate will have the CG Info with TweetBasedUnifiedSimilarityEngine
   * as they are generated by a composite of Similarity Engines.
   * Additionally, we store the contributing SEs (eg., SANN, UTG).
   */
  private def getTweetBasedUnifiedCGInfo(
    sourceInfoOpt: Option[SourceInfo],
    unifiedScore: Double,
    contributingSimilarityEngines: Seq[SimilarityEngineInfo]
  ): CandidateGenerationInfo = {
    CandidateGenerationInfo(
      sourceInfoOpt,
      SimilarityEngineInfo(
        similarityEngineType = SimilarityEngineType.TweetBasedUnifiedSimilarityEngine,
        modelId = None, // We do not assign modelId for a unified similarity engine
        score = Some(unifiedScore)
      ),
      contributingSimilarityEngines
    )
  }
}

object TweetBasedUnifiedSimilarityEngine {

  case class Query(
    sourceInfo: SourceInfo,
    maxCandidateNumPerSourceKey: Int,
    enableSimClustersANN: Boolean,
    simClustersANNQuery: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableExperimentalSimClustersANN: Boolean,
    experimentalSimClustersANNQuery: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableSimClustersANN1: Boolean,
    simClustersANN1Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableSimClustersANN2: Boolean,
    simClustersANN2Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableSimClustersANN3: Boolean,
    simClustersANN3Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableSimClustersANN5: Boolean,
    simClustersANN5Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableSimClustersANN4: Boolean,
    simClustersANN4Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    simClustersMinScore: Double,
    simClustersVideoBasedMinScore: Double,
    twhinModelId: String,
    enableTwHIN: Boolean,
    twhinMaxTweetAgeHours: Duration,
    qigMaxTweetAgeHours: Duration,
    qigMaxNumSimilarTweets: Int,
    enableUtg: Boolean,
    utgQuery: EngineQuery[TweetBasedUserTweetGraphSimilarityEngine.Query],
    enableUvg: Boolean,
    uvgQuery: EngineQuery[TweetBasedUserVideoGraphSimilarityEngine.Query],
    enableQig: Boolean,
    qigQuery: EngineQuery[TweetBasedQigSimilarityEngine.Query],
    params: configapi.Params)

  def fromParams(
    sourceInfo: SourceInfo,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    // SimClusters
    val enableSimClustersANN =
      params(TweetBasedCandidateGenerationParams.EnableSimClustersANNParam)

    val simClustersModelVersion =
      ModelVersions.Enum.enumToSimClustersModelVersionMap(params(GlobalParams.ModelVersionParam))
    val simClustersMinScore = params(TweetBasedCandidateGenerationParams.SimClustersMinScoreParam)
    val simClustersVideoBasedMinScore = params(
      TweetBasedCandidateGenerationParams.SimClustersVideoBasedMinScoreParam)
    val simClustersANNConfigId = params(SimClustersANNParams.SimClustersANNConfigId)
    // SimClusters - Experimental SANN Similarity Engine (Video based SE)
    val enableExperimentalSimClustersANN =
      params(TweetBasedCandidateGenerationParams.EnableExperimentalSimClustersANNParam)

    val experimentalSimClustersANNConfigId = params(
      SimClustersANNParams.ExperimentalSimClustersANNConfigId)
    // SimClusters - SANN cluster 1 Similarity Engine
    val enableSimClustersANN1 =
      params(TweetBasedCandidateGenerationParams.EnableSimClustersANN1Param)

    val simClustersANN1ConfigId = params(SimClustersANNParams.SimClustersANN1ConfigId)
    // SimClusters - SANN cluster 2 Similarity Engine
    val enableSimClustersANN2 =
      params(TweetBasedCandidateGenerationParams.EnableSimClustersANN2Param)
    val simClustersANN2ConfigId = params(SimClustersANNParams.SimClustersANN2ConfigId)
    // SimClusters - SANN cluster 3 Similarity Engine
    val enableSimClustersANN3 =
      params(TweetBasedCandidateGenerationParams.EnableSimClustersANN3Param)
    val simClustersANN3ConfigId = params(SimClustersANNParams.SimClustersANN3ConfigId)
    // SimClusters - SANN cluster 5 Similarity Engine
    val enableSimClustersANN5 =
      params(TweetBasedCandidateGenerationParams.EnableSimClustersANN5Param)
    val simClustersANN5ConfigId = params(SimClustersANNParams.SimClustersANN5ConfigId)
    // SimClusters - SANN cluster 4 Similarity Engine
    val enableSimClustersANN4 =
      params(TweetBasedCandidateGenerationParams.EnableSimClustersANN4Param)
    val simClustersANN4ConfigId = params(SimClustersANNParams.SimClustersANN4ConfigId)
    // SimClusters ANN Queries for different SANN clusters
    val simClustersANNQuery = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANNConfigId,
      params
    )
    val experimentalSimClustersANNQuery = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      experimentalSimClustersANNConfigId,
      params
    )
    val simClustersANN1Query = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN1ConfigId,
      params
    )
    val simClustersANN2Query = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN2ConfigId,
      params
    )
    val simClustersANN3Query = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN3ConfigId,
      params
    )
    val simClustersANN5Query = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN5ConfigId,
      params
    )
    val simClustersANN4Query = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN4ConfigId,
      params
    )
    // TweetBasedCandidateGeneration
    val maxCandidateNumPerSourceKey = params(GlobalParams.MaxCandidateNumPerSourceKeyParam)
    // TwHIN
    val twhinModelId = params(TweetBasedTwHINParams.ModelIdParam)
    val enableTwHIN =
      params(TweetBasedCandidateGenerationParams.EnableTwHINParam)

    val twhinMaxTweetAgeHours = params(GlobalParams.MaxTweetAgeHoursParam)

    // QIG
    val enableQig =
      params(TweetBasedCandidateGenerationParams.EnableQigSimilarTweetsParam)
    val qigMaxTweetAgeHours = params(GlobalParams.MaxTweetAgeHoursParam)
    val qigMaxNumSimilarTweets = params(
      TweetBasedCandidateGenerationParams.QigMaxNumSimilarTweetsParam)

    // UTG
    val enableUtg =
      params(TweetBasedCandidateGenerationParams.EnableUTGParam)
    // UVG
    val enableUvg =
      params(TweetBasedCandidateGenerationParams.EnableUVGParam)
    EngineQuery(
      Query(
        sourceInfo = sourceInfo,
        maxCandidateNumPerSourceKey = maxCandidateNumPerSourceKey,
        enableSimClustersANN = enableSimClustersANN,
        simClustersANNQuery = simClustersANNQuery,
        enableExperimentalSimClustersANN = enableExperimentalSimClustersANN,
        experimentalSimClustersANNQuery = experimentalSimClustersANNQuery,
        enableSimClustersANN1 = enableSimClustersANN1,
        simClustersANN1Query = simClustersANN1Query,
        enableSimClustersANN2 = enableSimClustersANN2,
        simClustersANN2Query = simClustersANN2Query,
        enableSimClustersANN3 = enableSimClustersANN3,
        simClustersANN3Query = simClustersANN3Query,
        enableSimClustersANN5 = enableSimClustersANN5,
        simClustersANN5Query = simClustersANN5Query,
        enableSimClustersANN4 = enableSimClustersANN4,
        simClustersANN4Query = simClustersANN4Query,
        simClustersMinScore = simClustersMinScore,
        simClustersVideoBasedMinScore = simClustersVideoBasedMinScore,
        twhinModelId = twhinModelId,
        enableTwHIN = enableTwHIN,
        twhinMaxTweetAgeHours = twhinMaxTweetAgeHours,
        qigMaxTweetAgeHours = qigMaxTweetAgeHours,
        qigMaxNumSimilarTweets = qigMaxNumSimilarTweets,
        enableUtg = enableUtg,
        utgQuery = TweetBasedUserTweetGraphSimilarityEngine
          .fromParams(sourceInfo.internalId, params),
        enableQig = enableQig,
        qigQuery = TweetBasedQigSimilarityEngine.fromParams(sourceInfo.internalId, params),
        enableUvg = enableUvg,
        uvgQuery =
          TweetBasedUserVideoGraphSimilarityEngine.fromParams(sourceInfo.internalId, params),
        params = params
      ),
      params
    )
  }

  def fromParamsForRelatedTweet(
    internalId: InternalId,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    // SimClusters
    val enableSimClustersANN = params(RelatedTweetTweetBasedParams.EnableSimClustersANNParam)
    val simClustersModelVersion =
      ModelVersions.Enum.enumToSimClustersModelVersionMap(params(GlobalParams.ModelVersionParam))
    val simClustersMinScore = params(RelatedTweetTweetBasedParams.SimClustersMinScoreParam)
    val simClustersANNConfigId = params(SimClustersANNParams.SimClustersANNConfigId)
    val enableExperimentalSimClustersANN =
      params(RelatedTweetTweetBasedParams.EnableExperimentalSimClustersANNParam)
    val experimentalSimClustersANNConfigId = params(
      SimClustersANNParams.ExperimentalSimClustersANNConfigId)
    // SimClusters - SANN cluster 1 Similarity Engine
    val enableSimClustersANN1 = params(RelatedTweetTweetBasedParams.EnableSimClustersANN1Param)
    val simClustersANN1ConfigId = params(SimClustersANNParams.SimClustersANN1ConfigId)
    // SimClusters - SANN cluster 2 Similarity Engine
    val enableSimClustersANN2 = params(RelatedTweetTweetBasedParams.EnableSimClustersANN2Param)
    val simClustersANN2ConfigId = params(SimClustersANNParams.SimClustersANN2ConfigId)
    // SimClusters - SANN cluster 3 Similarity Engine
    val enableSimClustersANN3 = params(RelatedTweetTweetBasedParams.EnableSimClustersANN3Param)
    val simClustersANN3ConfigId = params(SimClustersANNParams.SimClustersANN3ConfigId)
    // SimClusters - SANN cluster 5 Similarity Engine
    val enableSimClustersANN5 = params(RelatedTweetTweetBasedParams.EnableSimClustersANN5Param)
    val simClustersANN5ConfigId = params(SimClustersANNParams.SimClustersANN5ConfigId)
    // SimClusters - SANN cluster 4 Similarity Engine
    val enableSimClustersANN4 = params(RelatedTweetTweetBasedParams.EnableSimClustersANN4Param)
    val simClustersANN4ConfigId = params(SimClustersANNParams.SimClustersANN4ConfigId)
    // SimClusters ANN Queries for different SANN clusters
    val simClustersANNQuery = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANNConfigId,
      params
    )
    val experimentalSimClustersANNQuery = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      experimentalSimClustersANNConfigId,
      params
    )
    val simClustersANN1Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN1ConfigId,
      params
    )
    val simClustersANN2Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN2ConfigId,
      params
    )
    val simClustersANN3Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN3ConfigId,
      params
    )
    val simClustersANN5Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN5ConfigId,
      params
    )
    val simClustersANN4Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN4ConfigId,
      params
    )
    // TweetBasedCandidateGeneration
    val maxCandidateNumPerSourceKey = params(GlobalParams.MaxCandidateNumPerSourceKeyParam)
    // TwHIN
    val twhinModelId = params(TweetBasedTwHINParams.ModelIdParam)
    val enableTwHIN = params(RelatedTweetTweetBasedParams.EnableTwHINParam)
    val twhinMaxTweetAgeHours = params(GlobalParams.MaxTweetAgeHoursParam)
    // QIG
    val enableQig = params(RelatedTweetTweetBasedParams.EnableQigSimilarTweetsParam)
    val qigMaxTweetAgeHours = params(GlobalParams.MaxTweetAgeHoursParam)
    val qigMaxNumSimilarTweets = params(
      TweetBasedCandidateGenerationParams.QigMaxNumSimilarTweetsParam)
    // UTG
    val enableUtg = params(RelatedTweetTweetBasedParams.EnableUTGParam)
    // UVG
    val enableUvg = params(RelatedTweetTweetBasedParams.EnableUVGParam)
    // SourceType.RequestTweetId is a placeholder.
    val sourceInfo = SourceInfo(SourceType.RequestTweetId, internalId, None)

    EngineQuery(
      Query(
        sourceInfo = sourceInfo,
        maxCandidateNumPerSourceKey = maxCandidateNumPerSourceKey,
        enableSimClustersANN = enableSimClustersANN,
        simClustersMinScore = simClustersMinScore,
        simClustersVideoBasedMinScore = simClustersMinScore,
        simClustersANNQuery = simClustersANNQuery,
        enableExperimentalSimClustersANN = enableExperimentalSimClustersANN,
        experimentalSimClustersANNQuery = experimentalSimClustersANNQuery,
        enableSimClustersANN1 = enableSimClustersANN1,
        simClustersANN1Query = simClustersANN1Query,
        enableSimClustersANN2 = enableSimClustersANN2,
        simClustersANN2Query = simClustersANN2Query,
        enableSimClustersANN3 = enableSimClustersANN3,
        simClustersANN3Query = simClustersANN3Query,
        enableSimClustersANN5 = enableSimClustersANN5,
        simClustersANN5Query = simClustersANN5Query,
        enableSimClustersANN4 = enableSimClustersANN4,
        simClustersANN4Query = simClustersANN4Query,
        twhinModelId = twhinModelId,
        enableTwHIN = enableTwHIN,
        twhinMaxTweetAgeHours = twhinMaxTweetAgeHours,
        qigMaxTweetAgeHours = qigMaxTweetAgeHours,
        qigMaxNumSimilarTweets = qigMaxNumSimilarTweets,
        enableUtg = enableUtg,
        utgQuery = TweetBasedUserTweetGraphSimilarityEngine
          .fromParams(sourceInfo.internalId, params),
        enableQig = enableQig,
        qigQuery = TweetBasedQigSimilarityEngine.fromParams(sourceInfo.internalId, params),
        enableUvg = enableUvg,
        uvgQuery =
          TweetBasedUserVideoGraphSimilarityEngine.fromParams(sourceInfo.internalId, params),
        params = params,
      ),
      params
    )
  }
  def fromParamsForRelatedVideoTweet(
    internalId: InternalId,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    // SimClusters
    val enableSimClustersANN = params(RelatedVideoTweetTweetBasedParams.EnableSimClustersANNParam)
    val simClustersModelVersion =
      ModelVersions.Enum.enumToSimClustersModelVersionMap(params(GlobalParams.ModelVersionParam))
    val simClustersMinScore = params(RelatedVideoTweetTweetBasedParams.SimClustersMinScoreParam)
    val simClustersANNConfigId = params(SimClustersANNParams.SimClustersANNConfigId)
    val enableExperimentalSimClustersANN = params(
      RelatedVideoTweetTweetBasedParams.EnableExperimentalSimClustersANNParam)
    val experimentalSimClustersANNConfigId = params(
      SimClustersANNParams.ExperimentalSimClustersANNConfigId)
    // SimClusters - SANN cluster 1 Similarity Engine
    val enableSimClustersANN1 = params(RelatedVideoTweetTweetBasedParams.EnableSimClustersANN1Param)
    val simClustersANN1ConfigId = params(SimClustersANNParams.SimClustersANN1ConfigId)
    // SimClusters - SANN cluster 2 Similarity Engine
    val enableSimClustersANN2 = params(RelatedVideoTweetTweetBasedParams.EnableSimClustersANN2Param)
    val simClustersANN2ConfigId = params(SimClustersANNParams.SimClustersANN2ConfigId)
    // SimClusters - SANN cluster 3 Similarity Engine
    val enableSimClustersANN3 = params(RelatedVideoTweetTweetBasedParams.EnableSimClustersANN3Param)
    val simClustersANN3ConfigId = params(SimClustersANNParams.SimClustersANN3ConfigId)
    // SimClusters - SANN cluster 5 Similarity Engine
    val enableSimClustersANN5 = params(RelatedVideoTweetTweetBasedParams.EnableSimClustersANN5Param)
    val simClustersANN5ConfigId = params(SimClustersANNParams.SimClustersANN5ConfigId)

    // SimClusters - SANN cluster 4 Similarity Engine
    val enableSimClustersANN4 = params(RelatedVideoTweetTweetBasedParams.EnableSimClustersANN4Param)
    val simClustersANN4ConfigId = params(SimClustersANNParams.SimClustersANN4ConfigId)
    // SimClusters ANN Queries for different SANN clusters
    val simClustersANNQuery = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANNConfigId,
      params
    )
    val experimentalSimClustersANNQuery = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      experimentalSimClustersANNConfigId,
      params
    )
    val simClustersANN1Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN1ConfigId,
      params
    )
    val simClustersANN2Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN2ConfigId,
      params
    )
    val simClustersANN3Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN3ConfigId,
      params
    )
    val simClustersANN5Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN5ConfigId,
      params
    )

    val simClustersANN4Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.LogFavLongestL2EmbeddingTweet,
      simClustersModelVersion,
      simClustersANN4ConfigId,
      params
    )
    // TweetBasedCandidateGeneration
    val maxCandidateNumPerSourceKey = params(GlobalParams.MaxCandidateNumPerSourceKeyParam)
    // TwHIN
    val twhinModelId = params(TweetBasedTwHINParams.ModelIdParam)
    val enableTwHIN = params(RelatedVideoTweetTweetBasedParams.EnableTwHINParam)
    val twhinMaxTweetAgeHours = params(GlobalParams.MaxTweetAgeHoursParam)
    // QIG
    val enableQig = params(RelatedVideoTweetTweetBasedParams.EnableQigSimilarTweetsParam)
    val qigMaxTweetAgeHours = params(GlobalParams.MaxTweetAgeHoursParam)
    val qigMaxNumSimilarTweets = params(
      TweetBasedCandidateGenerationParams.QigMaxNumSimilarTweetsParam)
    // UTG
    val enableUtg = params(RelatedVideoTweetTweetBasedParams.EnableUTGParam)

    // SourceType.RequestTweetId is a placeholder.
    val sourceInfo = SourceInfo(SourceType.RequestTweetId, internalId, None)

    val enableUvg = params(RelatedVideoTweetTweetBasedParams.EnableUVGParam)
    EngineQuery(
      Query(
        sourceInfo = sourceInfo,
        maxCandidateNumPerSourceKey = maxCandidateNumPerSourceKey,
        enableSimClustersANN = enableSimClustersANN,
        simClustersMinScore = simClustersMinScore,
        simClustersVideoBasedMinScore = simClustersMinScore,
        simClustersANNQuery = simClustersANNQuery,
        enableExperimentalSimClustersANN = enableExperimentalSimClustersANN,
        experimentalSimClustersANNQuery = experimentalSimClustersANNQuery,
        enableSimClustersANN1 = enableSimClustersANN1,
        simClustersANN1Query = simClustersANN1Query,
        enableSimClustersANN2 = enableSimClustersANN2,
        simClustersANN2Query = simClustersANN2Query,
        enableSimClustersANN3 = enableSimClustersANN3,
        simClustersANN3Query = simClustersANN3Query,
        enableSimClustersANN5 = enableSimClustersANN5,
        simClustersANN5Query = simClustersANN5Query,
        enableSimClustersANN4 = enableSimClustersANN4,
        simClustersANN4Query = simClustersANN4Query,
        twhinModelId = twhinModelId,
        enableTwHIN = enableTwHIN,
        twhinMaxTweetAgeHours = twhinMaxTweetAgeHours,
        qigMaxTweetAgeHours = qigMaxTweetAgeHours,
        qigMaxNumSimilarTweets = qigMaxNumSimilarTweets,
        enableUtg = enableUtg,
        utgQuery = TweetBasedUserTweetGraphSimilarityEngine
          .fromParams(sourceInfo.internalId, params),
        enableUvg = enableUvg,
        uvgQuery =
          TweetBasedUserVideoGraphSimilarityEngine.fromParams(sourceInfo.internalId, params),
        enableQig = enableQig,
        qigQuery = TweetBasedQigSimilarityEngine.fromParams(sourceInfo.internalId, params),
        params = params
      ),
      params
    )
  }
}
package com.twitter.cr_mixer.source_signal

import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.param.GoodProfileClickParams
import com.twitter.cr_mixer.param.GoodTweetClickParams
import com.twitter.cr_mixer.param.RealGraphOonParams
import com.twitter.cr_mixer.param.RecentFollowsParams
import com.twitter.cr_mixer.param.RecentNegativeSignalParams
import com.twitter.cr_mixer.param.RecentNotificationsParams
import com.twitter.cr_mixer.param.RecentOriginalTweetsParams
import com.twitter.cr_mixer.param.RecentReplyTweetsParams
import com.twitter.cr_mixer.param.RecentRetweetsParams
import com.twitter.cr_mixer.param.RecentTweetFavoritesParams
import com.twitter.cr_mixer.param.RepeatedProfileVisitsParams
import com.twitter.cr_mixer.param.TweetSharesParams
import com.twitter.cr_mixer.param.UnifiedUSSSignalParams
import com.twitter.cr_mixer.param.VideoViewTweetsParams
import com.twitter.cr_mixer.source_signal.UssStore.Query
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.usersignalservice.thriftscala.{Signal => UssSignal}
import com.twitter.usersignalservice.thriftscala.SignalType
import javax.inject.Singleton
import com.twitter.timelines.configapi
import com.twitter.timelines.configapi.Params
import com.twitter.usersignalservice.thriftscala.BatchSignalRequest
import com.twitter.usersignalservice.thriftscala.BatchSignalResponse
import com.twitter.usersignalservice.thriftscala.SignalRequest
import com.twitter.util.Future
import com.twitter.cr_mixer.thriftscala.Product
import com.twitter.usersignalservice.thriftscala.ClientIdentifier

@Singleton
case class UssStore(
  stratoStore: ReadableStore[BatchSignalRequest, BatchSignalResponse],
  statsReceiver: StatsReceiver)
    extends ReadableStore[Query, Seq[(SignalType, Seq[UssSignal])]] {

  import com.twitter.cr_mixer.source_signal.UssStore._

  override def get(query: Query): Future[Option[Seq[(SignalType, Seq[UssSignal])]]] = {
    val ussClientIdentifier = query.product match {
      case Product.Home =>
        ClientIdentifier.CrMixerHome
      case Product.Notifications =>
        ClientIdentifier.CrMixerNotifications
      case Product.Email =>
        ClientIdentifier.CrMixerEmail
      case _ =>
        ClientIdentifier.Unknown
    }
    val batchSignalRequest =
      BatchSignalRequest(
        query.userId,
        buildUserSignalServiceRequests(query.params),
        Some(ussClientIdentifier))

    stratoStore
      .get(batchSignalRequest)
      .map {
        _.map { batchSignalResponse =>
          batchSignalResponse.signalResponse.toSeq.map {
            case (signalType, ussSignals) =>
              (signalType, ussSignals)
          }
        }
      }
  }

  private def buildUserSignalServiceRequests(
    param: Params,
  ): Seq[SignalRequest] = {
    val unifiedMaxSourceKeyNum = param(GlobalParams.UnifiedMaxSourceKeyNum)
    val goodTweetClickMaxSignalNum = param(GoodTweetClickParams.MaxSignalNumParam)
    val aggrTweetMaxSourceKeyNum = param(UnifiedUSSSignalParams.UnifiedTweetSourceNumberParam)
    val aggrProducerMaxSourceKeyNum = param(UnifiedUSSSignalParams.UnifiedProducerSourceNumberParam)

    val maybeRecentTweetFavorite =
      if (param(RecentTweetFavoritesParams.EnableSourceParam))
        Some(SignalRequest(Some(unifiedMaxSourceKeyNum), SignalType.TweetFavorite))
      else None
    val maybeRecentRetweet =
      if (param(RecentRetweetsParams.EnableSourceParam))
        Some(SignalRequest(Some(unifiedMaxSourceKeyNum), SignalType.Retweet))
      else None
    val maybeRecentReply =
      if (param(RecentReplyTweetsParams.EnableSourceParam))
        Some(SignalRequest(Some(unifiedMaxSourceKeyNum), SignalType.Reply))
      else None
    val maybeRecentOriginalTweet =
      if (param(RecentOriginalTweetsParams.EnableSourceParam))
        Some(SignalRequest(Some(unifiedMaxSourceKeyNum), SignalType.OriginalTweet))
      else None
    val maybeRecentFollow =
      if (param(RecentFollowsParams.EnableSourceParam))
        Some(SignalRequest(Some(unifiedMaxSourceKeyNum), SignalType.AccountFollow))
      else None
    val maybeRepeatedProfileVisits =
      if (param(RepeatedProfileVisitsParams.EnableSourceParam))
        Some(
          SignalRequest(
            Some(unifiedMaxSourceKeyNum),
            param(RepeatedProfileVisitsParams.ProfileMinVisitType).signalType))
      else None
    val maybeRecentNotifications =
      if (param(RecentNotificationsParams.EnableSourceParam))
        Some(SignalRequest(Some(unifiedMaxSourceKeyNum), SignalType.NotificationOpenAndClickV1))
      else None
    val maybeTweetShares =
      if (param(TweetSharesParams.EnableSourceParam)) {
        Some(SignalRequest(Some(unifiedMaxSourceKeyNum), SignalType.TweetShareV1))
      } else None
    val maybeRealGraphOon =
      if (param(RealGraphOonParams.EnableSourceParam)) {
        Some(SignalRequest(Some(unifiedMaxSourceKeyNum), SignalType.RealGraphOon))
      } else None

    val maybeGoodTweetClick =
      if (param(GoodTweetClickParams.EnableSourceParam))
        Some(
          SignalRequest(
            Some(goodTweetClickMaxSignalNum),
            param(GoodTweetClickParams.ClickMinDwellTimeType).signalType))
      else None
    val maybeVideoViewTweets =
      if (param(VideoViewTweetsParams.EnableSourceParam)) {
        Some(
          SignalRequest(
            Some(unifiedMaxSourceKeyNum),
            param(VideoViewTweetsParams.VideoViewTweetTypeParam).signalType))
      } else None
    val maybeGoodProfileClick =
      if (param(GoodProfileClickParams.EnableSourceParam))
        Some(
          SignalRequest(
            Some(unifiedMaxSourceKeyNum),
            param(GoodProfileClickParams.ClickMinDwellTimeType).signalType))
      else None
    val maybeAggTweetSignal =
      if (param(UnifiedUSSSignalParams.EnableTweetAggSourceParam))
        Some(
          SignalRequest(
            Some(aggrTweetMaxSourceKeyNum),
            param(UnifiedUSSSignalParams.TweetAggTypeParam).signalType
          )
        )
      else None
    val maybeAggProducerSignal =
      if (param(UnifiedUSSSignalParams.EnableProducerAggSourceParam))
        Some(
          SignalRequest(
            Some(aggrProducerMaxSourceKeyNum),
            param(UnifiedUSSSignalParams.ProducerAggTypeParam).signalType
          )
        )
      else None

    // negative signals
    val maybeNegativeSignals = if (param(RecentNegativeSignalParams.EnableSourceParam)) {
      EnabledNegativeSignalTypes
        .map(negativeSignal => SignalRequest(Some(unifiedMaxSourceKeyNum), negativeSignal)).toSeq
    } else Seq.empty

    val allPositiveSignals =
      if (param(UnifiedUSSSignalParams.ReplaceIndividualUSSSourcesParam))
        Seq(
          maybeRecentOriginalTweet,
          maybeRecentNotifications,
          maybeRealGraphOon,
          maybeGoodTweetClick,
          maybeGoodProfileClick,
          maybeAggProducerSignal,
          maybeAggTweetSignal,
        )
      else
        Seq(
          maybeRecentTweetFavorite,
          maybeRecentRetweet,
          maybeRecentReply,
          maybeRecentOriginalTweet,
          maybeRecentFollow,
          maybeRepeatedProfileVisits,
          maybeRecentNotifications,
          maybeTweetShares,
          maybeRealGraphOon,
          maybeGoodTweetClick,
          maybeVideoViewTweets,
          maybeGoodProfileClick,
          maybeAggProducerSignal,
          maybeAggTweetSignal,
        )
    allPositiveSignals.flatten ++ maybeNegativeSignals
  }

}

object UssStore {
  case class Query(
    userId: UserId,
    params: configapi.Params,
    product: Product)

  val EnabledNegativeSourceTypes: Set[SourceType] =
    Set(SourceType.AccountBlock, SourceType.AccountMute)
  private val EnabledNegativeSignalTypes: Set[SignalType] =
    Set(SignalType.AccountBlock, SignalType.AccountMute)
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.param.ProducerBasedCandidateGenerationParams
import com.twitter.cr_mixer.param.UnifiedSETweetCombinationMethod
import com.twitter.cr_mixer.param.RelatedTweetProducerBasedParams
import com.twitter.cr_mixer.param.SimClustersANNParams
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.cr_mixer.util.InterleaveUtil
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.ModelVersions
import com.twitter.simclusters_v2.thriftscala.EmbeddingType
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Duration
import com.twitter.util.Future
import javax.inject.Named
import javax.inject.Singleton
import scala.collection.mutable.ArrayBuffer

/**
 * This store looks for similar tweets from UserTweetGraph for a Source ProducerId
 * For a query producerId,User Tweet Graph (UTG),
 * lets us find out which tweets the query producer's followers co-engaged
 */
@Singleton
case class ProducerBasedUnifiedSimilarityEngine(
  @Named(ModuleNames.ProducerBasedUserTweetGraphSimilarityEngine)
  producerBasedUserTweetGraphSimilarityEngine: StandardSimilarityEngine[
    ProducerBasedUserTweetGraphSimilarityEngine.Query,
    TweetWithScore
  ],
  simClustersANNSimilarityEngine: StandardSimilarityEngine[
    SimClustersANNSimilarityEngine.Query,
    TweetWithScore
  ],
  statsReceiver: StatsReceiver)
    extends ReadableStore[ProducerBasedUnifiedSimilarityEngine.Query, Seq[
      TweetWithCandidateGenerationInfo
    ]] {

  import ProducerBasedUnifiedSimilarityEngine._
  private val stats = statsReceiver.scope(this.getClass.getSimpleName)
  private val fetchCandidatesStat = stats.scope("fetchCandidates")

  override def get(
    query: Query
  ): Future[Option[Seq[TweetWithCandidateGenerationInfo]]] = {
    query.sourceInfo.internalId match {
      case _: InternalId.UserId =>
        StatsUtil.trackOptionItemsStats(fetchCandidatesStat) {
          val sannCandidatesFut = if (query.enableSimClustersANN) {
            simClustersANNSimilarityEngine.getCandidates(query.simClustersANNQuery)
          } else Future.None

          val sann1CandidatesFut =
            if (query.enableSimClustersANN1) {
              simClustersANNSimilarityEngine.getCandidates(query.simClustersANN1Query)
            } else Future.None

          val sann2CandidatesFut =
            if (query.enableSimClustersANN2) {
              simClustersANNSimilarityEngine.getCandidates(query.simClustersANN2Query)
            } else Future.None

          val sann3CandidatesFut =
            if (query.enableSimClustersANN3) {
              simClustersANNSimilarityEngine.getCandidates(query.simClustersANN3Query)
            } else Future.None

          val sann4CandidatesFut =
            if (query.enableSimClustersANN4) {
              simClustersANNSimilarityEngine.getCandidates(query.simClustersANN4Query)
            } else Future.None

          val sann5CandidatesFut =
            if (query.enableSimClustersANN5) {
              simClustersANNSimilarityEngine.getCandidates(query.simClustersANN5Query)
            } else Future.None

          val experimentalSANNCandidatesFut =
            if (query.enableExperimentalSimClustersANN) {
              simClustersANNSimilarityEngine.getCandidates(query.experimentalSimClustersANNQuery)
            } else Future.None

          val utgCandidatesFut = if (query.enableUtg) {
            producerBasedUserTweetGraphSimilarityEngine.getCandidates(query.utgQuery)
          } else Future.None

          Future
            .join(
              sannCandidatesFut,
              sann1CandidatesFut,
              sann2CandidatesFut,
              sann3CandidatesFut,
              sann4CandidatesFut,
              sann5CandidatesFut,
              experimentalSANNCandidatesFut,
              utgCandidatesFut
            ).map {
              case (
                    simClustersAnnCandidates,
                    simClustersAnn1Candidates,
                    simClustersAnn2Candidates,
                    simClustersAnn3Candidates,
                    simClustersAnn4Candidates,
                    simClustersAnn5Candidates,
                    experimentalSANNCandidates,
                    userTweetGraphCandidates) =>
                val filteredSANNTweets = simClustersCandidateMinScoreFilter(
                  simClustersAnnCandidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANNQuery.storeQuery.simClustersANNConfigId)

                val filteredExperimentalSANNTweets = simClustersCandidateMinScoreFilter(
                  experimentalSANNCandidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.experimentalSimClustersANNQuery.storeQuery.simClustersANNConfigId)

                val filteredSANN1Tweets = simClustersCandidateMinScoreFilter(
                  simClustersAnn1Candidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANN1Query.storeQuery.simClustersANNConfigId)

                val filteredSANN2Tweets = simClustersCandidateMinScoreFilter(
                  simClustersAnn2Candidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANN2Query.storeQuery.simClustersANNConfigId)

                val filteredSANN3Tweets = simClustersCandidateMinScoreFilter(
                  simClustersAnn3Candidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANN3Query.storeQuery.simClustersANNConfigId)

                val filteredSANN4Tweets = simClustersCandidateMinScoreFilter(
                  simClustersAnn4Candidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANN4Query.storeQuery.simClustersANNConfigId)

                val filteredSANN5Tweets = simClustersCandidateMinScoreFilter(
                  simClustersAnn5Candidates.toSeq.flatten,
                  query.simClustersMinScore,
                  query.simClustersANN5Query.storeQuery.simClustersANNConfigId)

                val filteredUTGTweets =
                  userTweetGraphFilter(userTweetGraphCandidates.toSeq.flatten)

                val sannTweetsWithCGInfo = filteredSANNTweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANNQuery, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }
                val sann1TweetsWithCGInfo = filteredSANN1Tweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANN1Query, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }
                val sann2TweetsWithCGInfo = filteredSANN2Tweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANN2Query, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }

                val sann3TweetsWithCGInfo = filteredSANN3Tweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANN3Query, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }

                val sann4TweetsWithCGInfo = filteredSANN4Tweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANN4Query, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }

                val sann5TweetsWithCGInfo = filteredSANN5Tweets.map { tweetWithScore =>
                  val similarityEngineInfo = SimClustersANNSimilarityEngine
                    .toSimilarityEngineInfo(query.simClustersANN5Query, tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }

                val experimentalSANNTweetsWithCGInfo = filteredExperimentalSANNTweets.map {
                  tweetWithScore =>
                    val similarityEngineInfo = SimClustersANNSimilarityEngine
                      .toSimilarityEngineInfo(
                        query.experimentalSimClustersANNQuery,
                        tweetWithScore.score)
                    TweetWithCandidateGenerationInfo(
                      tweetWithScore.tweetId,
                      CandidateGenerationInfo(
                        Some(query.sourceInfo),
                        similarityEngineInfo,
                        Seq(similarityEngineInfo)
                      ))
                }
                val utgTweetsWithCGInfo = filteredUTGTweets.map { tweetWithScore =>
                  val similarityEngineInfo =
                    ProducerBasedUserTweetGraphSimilarityEngine
                      .toSimilarityEngineInfo(tweetWithScore.score)
                  TweetWithCandidateGenerationInfo(
                    tweetWithScore.tweetId,
                    CandidateGenerationInfo(
                      Some(query.sourceInfo),
                      similarityEngineInfo,
                      Seq(similarityEngineInfo)
                    ))
                }

                val candidateSourcesToBeInterleaved =
                  ArrayBuffer[Seq[TweetWithCandidateGenerationInfo]](
                    sannTweetsWithCGInfo,
                    sann1TweetsWithCGInfo,
                    sann2TweetsWithCGInfo,
                    sann3TweetsWithCGInfo,
                    sann4TweetsWithCGInfo,
                    sann5TweetsWithCGInfo,
                    experimentalSANNTweetsWithCGInfo,
                  )

                if (query.utgCombinationMethod == UnifiedSETweetCombinationMethod.Interleave) {
                  candidateSourcesToBeInterleaved += utgTweetsWithCGInfo
                }

                val interleavedCandidates =
                  InterleaveUtil.interleave(candidateSourcesToBeInterleaved)

                val candidateSourcesToBeOrdered =
                  ArrayBuffer[Seq[TweetWithCandidateGenerationInfo]](interleavedCandidates)

                if (query.utgCombinationMethod == UnifiedSETweetCombinationMethod.Frontload)
                  candidateSourcesToBeOrdered.prepend(utgTweetsWithCGInfo)

                val candidatesFromGivenOrderCombination =
                  SimilaritySourceOrderingUtil.keepGivenOrder(candidateSourcesToBeOrdered)

                val unifiedCandidatesWithUnifiedCGInfo = candidatesFromGivenOrderCombination.map {
                  candidate =>
                    /***
                     * when a candidate was made by interleave/keepGivenOrder,
                     * then we apply getProducerBasedUnifiedCGInfo() to override with the unified CGInfo
                     *
                     * in contributingSE list for interleave. We only have the chosen SE available.
                     * This is hard to add for interleave, and we plan to add it later after abstraction improvement.
                     */
                    TweetWithCandidateGenerationInfo(
                      tweetId = candidate.tweetId,
                      candidateGenerationInfo = getProducerBasedUnifiedCGInfo(
                        candidate.candidateGenerationInfo.sourceInfoOpt,
                        candidate.getSimilarityScore,
                        candidate.candidateGenerationInfo.contributingSimilarityEngines
                      ) // getSimilarityScore comes from either unifiedScore or single score
                    )
                }
                stats.stat("unified_candidate_size").add(unifiedCandidatesWithUnifiedCGInfo.size)
                val truncatedCandidates =
                  unifiedCandidatesWithUnifiedCGInfo.take(query.maxCandidateNumPerSourceKey)
                stats.stat("truncatedCandidates_size").add(truncatedCandidates.size)

                Some(truncatedCandidates)

            }
        }

      case _ =>
        stats.counter("sourceId_is_not_userId_cnt").incr()
        Future.None
    }
  }

  private def simClustersCandidateMinScoreFilter(
    simClustersAnnCandidates: Seq[TweetWithScore],
    simClustersMinScore: Double,
    simClustersANNConfigId: String
  ): Seq[TweetWithScore] = {
    val filteredCandidates = simClustersAnnCandidates
      .filter { candidate =>
        candidate.score > simClustersMinScore
      }

    stats.stat(simClustersANNConfigId, "simClustersAnnCandidates_size").add(filteredCandidates.size)
    stats.counter(simClustersANNConfigId, "simClustersAnnRequests").incr()
    if (filteredCandidates.isEmpty)
      stats.counter(simClustersANNConfigId, "emptyFilteredSimClustersAnnCandidates").incr()

    filteredCandidates.map { candidate =>
      TweetWithScore(candidate.tweetId, candidate.score)
    }
  }

  /** A no-op filter as UTG filter already happened at UTG service side */
  private def userTweetGraphFilter(
    userTweetGraphCandidates: Seq[TweetWithScore]
  ): Seq[TweetWithScore] = {
    val filteredCandidates = userTweetGraphCandidates

    stats.stat("userTweetGraphCandidates_size").add(userTweetGraphCandidates.size)
    if (filteredCandidates.isEmpty) stats.counter("emptyFilteredUserTweetGraphCandidates").incr()

    filteredCandidates.map { candidate =>
      TweetWithScore(candidate.tweetId, candidate.score)
    }
  }

}
object ProducerBasedUnifiedSimilarityEngine {

  /***
   * Every candidate will have the CG Info with ProducerBasedUnifiedSimilarityEngine
   * as they are generated by a composite of Similarity Engines.
   * Additionally, we store the contributing SEs (eg., SANN, UTG).
   */
  private def getProducerBasedUnifiedCGInfo(
    sourceInfoOpt: Option[SourceInfo],
    unifiedScore: Double,
    contributingSimilarityEngines: Seq[SimilarityEngineInfo]
  ): CandidateGenerationInfo = {
    CandidateGenerationInfo(
      sourceInfoOpt,
      SimilarityEngineInfo(
        similarityEngineType = SimilarityEngineType.ProducerBasedUnifiedSimilarityEngine,
        modelId = None, // We do not assign modelId for a unified similarity engine
        score = Some(unifiedScore)
      ),
      contributingSimilarityEngines
    )
  }

  case class Query(
    sourceInfo: SourceInfo,
    maxCandidateNumPerSourceKey: Int,
    maxTweetAgeHours: Duration,
    // SimClusters
    enableSimClustersANN: Boolean,
    simClustersANNQuery: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableExperimentalSimClustersANN: Boolean,
    experimentalSimClustersANNQuery: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableSimClustersANN1: Boolean,
    simClustersANN1Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableSimClustersANN2: Boolean,
    simClustersANN2Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableSimClustersANN4: Boolean,
    simClustersANN4Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableSimClustersANN3: Boolean,
    simClustersANN3Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    enableSimClustersANN5: Boolean,
    simClustersANN5Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    simClustersMinScore: Double,
    // UTG
    enableUtg: Boolean,
    utgCombinationMethod: UnifiedSETweetCombinationMethod.Value,
    utgQuery: EngineQuery[ProducerBasedUserTweetGraphSimilarityEngine.Query])

  def fromParams(
    sourceInfo: SourceInfo,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    val maxCandidateNumPerSourceKey = params(GlobalParams.MaxCandidateNumPerSourceKeyParam)
    val maxTweetAgeHours = params(GlobalParams.MaxTweetAgeHoursParam)
    // SimClusters
    val enableSimClustersANN = params(
      ProducerBasedCandidateGenerationParams.EnableSimClustersANNParam)
    val simClustersModelVersion =
      ModelVersions.Enum.enumToSimClustersModelVersionMap(params(GlobalParams.ModelVersionParam))
    val simClustersANNConfigId = params(SimClustersANNParams.SimClustersANNConfigId)
    // SimClusters - Experimental SANN Similarity Engine
    val enableExperimentalSimClustersANN = params(
      ProducerBasedCandidateGenerationParams.EnableExperimentalSimClustersANNParam)
    val experimentalSimClustersANNConfigId = params(
      SimClustersANNParams.ExperimentalSimClustersANNConfigId)
    // SimClusters - SANN cluster 1 Similarity Engine
    val enableSimClustersANN1 = params(
      ProducerBasedCandidateGenerationParams.EnableSimClustersANN1Param)
    val simClustersANN1ConfigId = params(SimClustersANNParams.SimClustersANN1ConfigId)
    // SimClusters - SANN cluster 2 Similarity Engine
    val enableSimClustersANN2 = params(
      ProducerBasedCandidateGenerationParams.EnableSimClustersANN2Param)
    val simClustersANN2ConfigId = params(SimClustersANNParams.SimClustersANN2ConfigId)
    // SimClusters - SANN cluster 3 Similarity Engine
    val enableSimClustersANN3 = params(
      ProducerBasedCandidateGenerationParams.EnableSimClustersANN3Param)
    val simClustersANN3ConfigId = params(SimClustersANNParams.SimClustersANN3ConfigId)
    // SimClusters - SANN cluster 5 Similarity Engine
    val enableSimClustersANN5 = params(
      ProducerBasedCandidateGenerationParams.EnableSimClustersANN5Param)
    val simClustersANN5ConfigId = params(SimClustersANNParams.SimClustersANN5ConfigId)
    val enableSimClustersANN4 = params(
      ProducerBasedCandidateGenerationParams.EnableSimClustersANN4Param)
    val simClustersANN4ConfigId = params(SimClustersANNParams.SimClustersANN4ConfigId)

    val simClustersMinScore = params(
      ProducerBasedCandidateGenerationParams.SimClustersMinScoreParam)

    // SimClusters ANN Query
    val simClustersANNQuery = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANNConfigId,
      params
    )
    val experimentalSimClustersANNQuery = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      experimentalSimClustersANNConfigId,
      params
    )
    val simClustersANN1Query = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANN1ConfigId,
      params
    )
    val simClustersANN2Query = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANN2ConfigId,
      params
    )
    val simClustersANN3Query = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANN3ConfigId,
      params
    )
    val simClustersANN5Query = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANN5ConfigId,
      params
    )
    val simClustersANN4Query = SimClustersANNSimilarityEngine.fromParams(
      sourceInfo.internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANN4ConfigId,
      params
    )
    // UTG
    val enableUtg = params(ProducerBasedCandidateGenerationParams.EnableUTGParam)
    val utgCombinationMethod = params(
      ProducerBasedCandidateGenerationParams.UtgCombinationMethodParam)

    EngineQuery(
      Query(
        sourceInfo = sourceInfo,
        maxCandidateNumPerSourceKey = maxCandidateNumPerSourceKey,
        maxTweetAgeHours = maxTweetAgeHours,
        enableSimClustersANN = enableSimClustersANN,
        simClustersANNQuery = simClustersANNQuery,
        enableExperimentalSimClustersANN = enableExperimentalSimClustersANN,
        experimentalSimClustersANNQuery = experimentalSimClustersANNQuery,
        enableSimClustersANN1 = enableSimClustersANN1,
        simClustersANN1Query = simClustersANN1Query,
        enableSimClustersANN2 = enableSimClustersANN2,
        simClustersANN2Query = simClustersANN2Query,
        enableSimClustersANN3 = enableSimClustersANN3,
        simClustersANN3Query = simClustersANN3Query,
        enableSimClustersANN5 = enableSimClustersANN5,
        simClustersANN5Query = simClustersANN5Query,
        enableSimClustersANN4 = enableSimClustersANN4,
        simClustersANN4Query = simClustersANN4Query,
        simClustersMinScore = simClustersMinScore,
        enableUtg = enableUtg,
        utgCombinationMethod = utgCombinationMethod,
        utgQuery = ProducerBasedUserTweetGraphSimilarityEngine
          .fromParams(sourceInfo.internalId, params)
      ),
      params
    )
  }

  def fromParamsForRelatedTweet(
    internalId: InternalId,
    params: configapi.Params
  ): EngineQuery[Query] = {
    val maxCandidateNumPerSourceKey = params(GlobalParams.MaxCandidateNumPerSourceKeyParam)
    val maxTweetAgeHours = params(GlobalParams.MaxTweetAgeHoursParam)
    // SimClusters
    val enableSimClustersANN = params(RelatedTweetProducerBasedParams.EnableSimClustersANNParam)
    val simClustersModelVersion =
      ModelVersions.Enum.enumToSimClustersModelVersionMap(params(GlobalParams.ModelVersionParam))
    val simClustersANNConfigId = params(SimClustersANNParams.SimClustersANNConfigId)
    val simClustersMinScore =
      params(RelatedTweetProducerBasedParams.SimClustersMinScoreParam)
    // SimClusters - Experimental SANN Similarity Engine
    val enableExperimentalSimClustersANN = params(
      RelatedTweetProducerBasedParams.EnableExperimentalSimClustersANNParam)
    val experimentalSimClustersANNConfigId = params(
      SimClustersANNParams.ExperimentalSimClustersANNConfigId)
    // SimClusters - SANN cluster 1 Similarity Engine
    val enableSimClustersANN1 = params(RelatedTweetProducerBasedParams.EnableSimClustersANN1Param)
    val simClustersANN1ConfigId = params(SimClustersANNParams.SimClustersANN1ConfigId)
    // SimClusters - SANN cluster 2 Similarity Engine
    val enableSimClustersANN2 = params(RelatedTweetProducerBasedParams.EnableSimClustersANN2Param)
    val simClustersANN2ConfigId = params(SimClustersANNParams.SimClustersANN2ConfigId)
    // SimClusters - SANN cluster 3 Similarity Engine
    val enableSimClustersANN3 = params(RelatedTweetProducerBasedParams.EnableSimClustersANN3Param)
    val simClustersANN3ConfigId = params(SimClustersANNParams.SimClustersANN3ConfigId)
    // SimClusters - SANN cluster 5 Similarity Engine
    val enableSimClustersANN5 = params(RelatedTweetProducerBasedParams.EnableSimClustersANN5Param)
    val simClustersANN5ConfigId = params(SimClustersANNParams.SimClustersANN5ConfigId)

    val enableSimClustersANN4 = params(RelatedTweetProducerBasedParams.EnableSimClustersANN4Param)
    val simClustersANN4ConfigId = params(SimClustersANNParams.SimClustersANN4ConfigId)
    // Build SANN Query
    val simClustersANNQuery = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANNConfigId,
      params
    )
    val experimentalSimClustersANNQuery = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      experimentalSimClustersANNConfigId,
      params
    )
    val simClustersANN1Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANN1ConfigId,
      params
    )
    val simClustersANN2Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANN2ConfigId,
      params
    )
    val simClustersANN3Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANN3ConfigId,
      params
    )
    val simClustersANN5Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANN5ConfigId,
      params
    )
    val simClustersANN4Query = SimClustersANNSimilarityEngine.fromParams(
      internalId,
      EmbeddingType.FavBasedProducer,
      simClustersModelVersion,
      simClustersANN4ConfigId,
      params
    )
    // UTG
    val enableUtg = params(RelatedTweetProducerBasedParams.EnableUTGParam)
    val utgCombinationMethod = params(
      ProducerBasedCandidateGenerationParams.UtgCombinationMethodParam)

    // SourceType.RequestUserId is a placeholder.
    val sourceInfo = SourceInfo(SourceType.RequestUserId, internalId, None)

    EngineQuery(
      Query(
        sourceInfo = sourceInfo,
        maxCandidateNumPerSourceKey = maxCandidateNumPerSourceKey,
        maxTweetAgeHours = maxTweetAgeHours,
        enableSimClustersANN = enableSimClustersANN,
        simClustersANNQuery = simClustersANNQuery,
        enableExperimentalSimClustersANN = enableExperimentalSimClustersANN,
        experimentalSimClustersANNQuery = experimentalSimClustersANNQuery,
        enableSimClustersANN1 = enableSimClustersANN1,
        simClustersANN1Query = simClustersANN1Query,
        enableSimClustersANN2 = enableSimClustersANN2,
        simClustersANN2Query = simClustersANN2Query,
        enableSimClustersANN3 = enableSimClustersANN3,
        simClustersANN3Query = simClustersANN3Query,
        enableSimClustersANN5 = enableSimClustersANN5,
        simClustersANN5Query = simClustersANN5Query,
        enableSimClustersANN4 = enableSimClustersANN4,
        simClustersANN4Query = simClustersANN4Query,
        simClustersMinScore = simClustersMinScore,
        enableUtg = enableUtg,
        utgQuery = ProducerBasedUserTweetGraphSimilarityEngine.fromParams(internalId, params),
        utgCombinationMethod = utgCombinationMethod
      ),
      params
    )
  }

}
