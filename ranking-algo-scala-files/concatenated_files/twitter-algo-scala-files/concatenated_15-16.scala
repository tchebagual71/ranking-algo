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
package com.twitter.cr_mixer.controller

import com.twitter.core_workflows.user_model.thriftscala.UserState
import com.twitter.cr_mixer.candidate_generation.AdsCandidateGenerator
import com.twitter.cr_mixer.candidate_generation.CrCandidateGenerator
import com.twitter.cr_mixer.candidate_generation.FrsTweetCandidateGenerator
import com.twitter.cr_mixer.candidate_generation.RelatedTweetCandidateGenerator
import com.twitter.cr_mixer.candidate_generation.RelatedVideoTweetCandidateGenerator
import com.twitter.cr_mixer.candidate_generation.TopicTweetCandidateGenerator
import com.twitter.cr_mixer.candidate_generation.UtegTweetCandidateGenerator
import com.twitter.cr_mixer.featureswitch.ParamsBuilder
import com.twitter.cr_mixer.logging.CrMixerScribeLogger
import com.twitter.cr_mixer.logging.RelatedTweetScribeLogger
import com.twitter.cr_mixer.logging.AdsRecommendationsScribeLogger
import com.twitter.cr_mixer.logging.RelatedTweetScribeMetadata
import com.twitter.cr_mixer.logging.ScribeMetadata
import com.twitter.cr_mixer.logging.UtegTweetScribeLogger
import com.twitter.cr_mixer.model.AdsCandidateGeneratorQuery
import com.twitter.cr_mixer.model.CrCandidateGeneratorQuery
import com.twitter.cr_mixer.model.FrsTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.RankedAdsCandidate
import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.cr_mixer.model.RelatedTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.RelatedVideoTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.TopicTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.TweetWithScoreAndSocialProof
import com.twitter.cr_mixer.model.UtegTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.param.AdsParams
import com.twitter.cr_mixer.param.FrsParams.FrsBasedCandidateGenerationMaxCandidatesNumParam
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.param.RelatedTweetGlobalParams
import com.twitter.cr_mixer.param.RelatedVideoTweetGlobalParams
import com.twitter.cr_mixer.param.TopicTweetParams
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.param.decider.EndpointLoadShedder
import com.twitter.cr_mixer.thriftscala.AdTweetRecommendation
import com.twitter.cr_mixer.thriftscala.AdsRequest
import com.twitter.cr_mixer.thriftscala.AdsResponse
import com.twitter.cr_mixer.thriftscala.CrMixerTweetRequest
import com.twitter.cr_mixer.thriftscala.CrMixerTweetResponse
import com.twitter.cr_mixer.thriftscala.FrsTweetRequest
import com.twitter.cr_mixer.thriftscala.FrsTweetResponse
import com.twitter.cr_mixer.thriftscala.RelatedTweet
import com.twitter.cr_mixer.thriftscala.RelatedTweetRequest
import com.twitter.cr_mixer.thriftscala.RelatedTweetResponse
import com.twitter.cr_mixer.thriftscala.RelatedVideoTweet
import com.twitter.cr_mixer.thriftscala.RelatedVideoTweetRequest
import com.twitter.cr_mixer.thriftscala.RelatedVideoTweetResponse
import com.twitter.cr_mixer.thriftscala.TopicTweet
import com.twitter.cr_mixer.thriftscala.TopicTweetRequest
import com.twitter.cr_mixer.thriftscala.TopicTweetResponse
import com.twitter.cr_mixer.thriftscala.TweetRecommendation
import com.twitter.cr_mixer.thriftscala.UtegTweet
import com.twitter.cr_mixer.thriftscala.UtegTweetRequest
import com.twitter.cr_mixer.thriftscala.UtegTweetResponse
import com.twitter.cr_mixer.util.MetricTagUtil
import com.twitter.cr_mixer.util.SignalTimestampStatsUtil
import com.twitter.cr_mixer.{thriftscala => t}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finatra.thrift.Controller
import com.twitter.hermit.store.common.ReadableWritableStore
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.TopicId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.timeline_logging.{thriftscala => thriftlog}
import com.twitter.timelines.tracing.lensview.funnelseries.TweetScoreFunnelSeries
import com.twitter.util.Future
import com.twitter.util.Time
import java.util.UUID
import javax.inject.Inject
import org.apache.commons.lang.exception.ExceptionUtils

class CrMixerThriftController @Inject() (
  crCandidateGenerator: CrCandidateGenerator,
  relatedTweetCandidateGenerator: RelatedTweetCandidateGenerator,
  relatedVideoTweetCandidateGenerator: RelatedVideoTweetCandidateGenerator,
  utegTweetCandidateGenerator: UtegTweetCandidateGenerator,
  frsTweetCandidateGenerator: FrsTweetCandidateGenerator,
  topicTweetCandidateGenerator: TopicTweetCandidateGenerator,
  crMixerScribeLogger: CrMixerScribeLogger,
  relatedTweetScribeLogger: RelatedTweetScribeLogger,
  utegTweetScribeLogger: UtegTweetScribeLogger,
  adsRecommendationsScribeLogger: AdsRecommendationsScribeLogger,
  adsCandidateGenerator: AdsCandidateGenerator,
  decider: CrMixerDecider,
  paramsBuilder: ParamsBuilder,
  endpointLoadShedder: EndpointLoadShedder,
  signalTimestampStatsUtil: SignalTimestampStatsUtil,
  tweetRecommendationResultsStore: ReadableWritableStore[UserId, CrMixerTweetResponse],
  userStateStore: ReadableStore[UserId, UserState],
  statsReceiver: StatsReceiver)
    extends Controller(t.CrMixer) {

  lazy private val tweetScoreFunnelSeries = new TweetScoreFunnelSeries(statsReceiver)

  private def logErrMessage(endpoint: String, e: Throwable): Unit = {
    val msg = Seq(
      s"Failed endpoint $endpoint: ${e.getLocalizedMessage}",
      ExceptionUtils.getStackTrace(e)
    ).mkString("\n")

    /** *
     * We chose logger.info() here to print message instead of logger.error since that
     * logger.error sometimes suppresses detailed stacktrace.
     */
    logger.info(msg)
  }

  private def generateRequestUUID(): Long = {

    /** *
     * We generate unique UUID via bitwise operations. See the below link for more:
     * https://stackoverflow.com/questions/15184820/how-to-generate-unique-positive-long-using-uuid
     */
    UUID.randomUUID().getMostSignificantBits & Long.MaxValue
  }

  handle(t.CrMixer.GetTweetRecommendations) { args: t.CrMixer.GetTweetRecommendations.Args =>
    val endpointName = "getTweetRecommendations"

    val requestUUID = generateRequestUUID()
    val startTime = Time.now.inMilliseconds
    val userId = args.request.clientContext.userId.getOrElse(
      throw new IllegalArgumentException("userId must be present in the Thrift clientContext")
    )
    val queryFut = buildCrCandidateGeneratorQuery(args.request, requestUUID, userId)
    queryFut.flatMap { query =>
      val scribeMetadata = ScribeMetadata.from(query)
      endpointLoadShedder(endpointName, query.product.originalName) {

        val response = crCandidateGenerator.get(query)

        val blueVerifiedScribedResponse = response.flatMap { rankedCandidates =>
          val hasBlueVerifiedCandidate = rankedCandidates.exists { tweet =>
            tweet.tweetInfo.hasBlueVerifiedAnnotation.contains(true)
          }

          if (hasBlueVerifiedCandidate) {
            crMixerScribeLogger.scribeGetTweetRecommendationsForBlueVerified(
              scribeMetadata,
              response)
          } else {
            response
          }
        }

        val thriftResponse = blueVerifiedScribedResponse.map { candidates =>
          if (query.product == t.Product.Home) {
            scribeTweetScoreFunnelSeries(candidates)
          }
          buildThriftResponse(candidates)
        }

        cacheTweetRecommendationResults(args.request, thriftResponse)

        crMixerScribeLogger.scribeGetTweetRecommendations(
          args.request,
          startTime,
          scribeMetadata,
          thriftResponse)
      }.rescue {
        case EndpointLoadShedder.LoadSheddingException =>
          Future(CrMixerTweetResponse(Seq.empty))
        case e =>
          logErrMessage(endpointName, e)
          Future(CrMixerTweetResponse(Seq.empty))
      }
    }

  }

  /** *
   * GetRelatedTweetsForQueryTweet and GetRelatedTweetsForQueryAuthor are essentially
   * doing very similar things, except that one passes in TweetId which calls TweetBased engine,
   * and the other passes in AuthorId which calls ProducerBased engine.
   */
  handle(t.CrMixer.GetRelatedTweetsForQueryTweet) {
    args: t.CrMixer.GetRelatedTweetsForQueryTweet.Args =>
      val endpointName = "getRelatedTweetsForQueryTweet"
      getRelatedTweets(endpointName, args.request)
  }

  handle(t.CrMixer.GetRelatedVideoTweetsForQueryTweet) {
    args: t.CrMixer.GetRelatedVideoTweetsForQueryTweet.Args =>
      val endpointName = "getRelatedVideoTweetsForQueryVideoTweet"
      getRelatedVideoTweets(endpointName, args.request)

  }

  handle(t.CrMixer.GetRelatedTweetsForQueryAuthor) {
    args: t.CrMixer.GetRelatedTweetsForQueryAuthor.Args =>
      val endpointName = "getRelatedTweetsForQueryAuthor"
      getRelatedTweets(endpointName, args.request)
  }

  private def getRelatedTweets(
    endpointName: String,
    request: RelatedTweetRequest
  ): Future[RelatedTweetResponse] = {
    val requestUUID = generateRequestUUID()
    val startTime = Time.now.inMilliseconds
    val queryFut = buildRelatedTweetQuery(request, requestUUID)

    queryFut.flatMap { query =>
      val relatedTweetScribeMetadata = RelatedTweetScribeMetadata.from(query)
      endpointLoadShedder(endpointName, query.product.originalName) {
        relatedTweetScribeLogger.scribeGetRelatedTweets(
          request,
          startTime,
          relatedTweetScribeMetadata,
          relatedTweetCandidateGenerator
            .get(query)
            .map(buildRelatedTweetResponse))
      }.rescue {
        case EndpointLoadShedder.LoadSheddingException =>
          Future(RelatedTweetResponse(Seq.empty))
        case e =>
          logErrMessage(endpointName, e)
          Future(RelatedTweetResponse(Seq.empty))
      }
    }

  }

  private def getRelatedVideoTweets(
    endpointName: String,
    request: RelatedVideoTweetRequest
  ): Future[RelatedVideoTweetResponse] = {
    val requestUUID = generateRequestUUID()
    val queryFut = buildRelatedVideoTweetQuery(request, requestUUID)

    queryFut.flatMap { query =>
      endpointLoadShedder(endpointName, query.product.originalName) {
        relatedVideoTweetCandidateGenerator.get(query).map { initialCandidateSeq =>
          buildRelatedVideoTweetResponse(initialCandidateSeq)
        }
      }.rescue {
        case EndpointLoadShedder.LoadSheddingException =>
          Future(RelatedVideoTweetResponse(Seq.empty))
        case e =>
          logErrMessage(endpointName, e)
          Future(RelatedVideoTweetResponse(Seq.empty))
      }
    }
  }

  handle(t.CrMixer.GetFrsBasedTweetRecommendations) {
    args: t.CrMixer.GetFrsBasedTweetRecommendations.Args =>
      val endpointName = "getFrsBasedTweetRecommendations"

      val requestUUID = generateRequestUUID()
      val queryFut = buildFrsBasedTweetQuery(args.request, requestUUID)
      queryFut.flatMap { query =>
        endpointLoadShedder(endpointName, query.product.originalName) {
          frsTweetCandidateGenerator.get(query).map(FrsTweetResponse(_))
        }.rescue {
          case e =>
            logErrMessage(endpointName, e)
            Future(FrsTweetResponse(Seq.empty))
        }
      }
  }

  handle(t.CrMixer.GetTopicTweetRecommendations) {
    args: t.CrMixer.GetTopicTweetRecommendations.Args =>
      val endpointName = "getTopicTweetRecommendations"

      val requestUUID = generateRequestUUID()
      val query = buildTopicTweetQuery(args.request, requestUUID)

      endpointLoadShedder(endpointName, query.product.originalName) {
        topicTweetCandidateGenerator.get(query).map(TopicTweetResponse(_))
      }.rescue {
        case e =>
          logErrMessage(endpointName, e)
          Future(TopicTweetResponse(Map.empty[Long, Seq[TopicTweet]]))
      }
  }

  handle(t.CrMixer.GetUtegTweetRecommendations) {
    args: t.CrMixer.GetUtegTweetRecommendations.Args =>
      val endpointName = "getUtegTweetRecommendations"

      val requestUUID = generateRequestUUID()
      val startTime = Time.now.inMilliseconds
      val queryFut = buildUtegTweetQuery(args.request, requestUUID)
      queryFut
        .flatMap { query =>
          val scribeMetadata = ScribeMetadata.from(query)
          endpointLoadShedder(endpointName, query.product.originalName) {
            utegTweetScribeLogger.scribeGetUtegTweetRecommendations(
              args.request,
              startTime,
              scribeMetadata,
              utegTweetCandidateGenerator
                .get(query)
                .map(buildUtegTweetResponse)
            )
          }.rescue {
            case e =>
              logErrMessage(endpointName, e)
              Future(UtegTweetResponse(Seq.empty))
          }
        }
  }

  handle(t.CrMixer.GetAdsRecommendations) { args: t.CrMixer.GetAdsRecommendations.Args =>
    val endpointName = "getAdsRecommendations"
    val queryFut = buildAdsCandidateGeneratorQuery(args.request)
    val startTime = Time.now.inMilliseconds
    queryFut.flatMap { query =>
      {
        val scribeMetadata = ScribeMetadata.from(query)
        val response = adsCandidateGenerator
          .get(query).map { candidates =>
            buildAdsResponse(candidates)
          }
        adsRecommendationsScribeLogger.scribeGetAdsRecommendations(
          args.request,
          startTime,
          scribeMetadata,
          response,
          query.params(AdsParams.EnableScribe)
        )
      }.rescue {
        case e =>
          logErrMessage(endpointName, e)
          Future(AdsResponse(Seq.empty))
      }
    }

  }

  private def buildCrCandidateGeneratorQuery(
    thriftRequest: CrMixerTweetRequest,
    requestUUID: Long,
    userId: Long
  ): Future[CrCandidateGeneratorQuery] = {

    val product = thriftRequest.product
    val productContext = thriftRequest.productContext
    val scopedStats = statsReceiver
      .scope(product.toString).scope("CrMixerTweetRequest")

    userStateStore
      .get(userId).map { userStateOpt =>
        val userState = userStateOpt
          .getOrElse(UserState.EnumUnknownUserState(100))
        scopedStats.scope("UserState").counter(userState.toString).incr()

        val params =
          paramsBuilder.buildFromClientContext(
            thriftRequest.clientContext,
            thriftRequest.product,
            userState
          )

        // Specify product-specific behavior mapping here
        val maxNumResults = (product, productContext) match {
          case (t.Product.Home, Some(t.ProductContext.HomeContext(homeContext))) =>
            homeContext.maxResults.getOrElse(9999)
          case (t.Product.Notifications, Some(t.ProductContext.NotificationsContext(cxt))) =>
            params(GlobalParams.MaxCandidatesPerRequestParam)
          case (t.Product.Email, None) =>
            params(GlobalParams.MaxCandidatesPerRequestParam)
          case (t.Product.ImmersiveMediaViewer, None) =>
            params(GlobalParams.MaxCandidatesPerRequestParam)
          case (t.Product.VideoCarousel, None) =>
            params(GlobalParams.MaxCandidatesPerRequestParam)
          case _ =>
            throw new IllegalArgumentException(
              s"Product ${product} and ProductContext ${productContext} are not allowed in CrMixer"
            )
        }

        CrCandidateGeneratorQuery(
          userId = userId,
          product = product,
          userState = userState,
          maxNumResults = maxNumResults,
          impressedTweetList = thriftRequest.excludedTweetIds.getOrElse(Nil).toSet,
          params = params,
          requestUUID = requestUUID,
          languageCode = thriftRequest.clientContext.languageCode
        )
      }
  }

  private def buildRelatedTweetQuery(
    thriftRequest: RelatedTweetRequest,
    requestUUID: Long
  ): Future[RelatedTweetCandidateGeneratorQuery] = {

    val product = thriftRequest.product
    val scopedStats = statsReceiver
      .scope(product.toString).scope("RelatedTweetRequest")
    val userStateFut: Future[UserState] = (thriftRequest.clientContext.userId match {
      case Some(userId) => userStateStore.get(userId)
      case None => Future.value(Some(UserState.EnumUnknownUserState(100)))
    }).map(_.getOrElse(UserState.EnumUnknownUserState(100)))

    userStateFut.map { userState =>
      scopedStats.scope("UserState").counter(userState.toString).incr()
      val params =
        paramsBuilder.buildFromClientContext(
          thriftRequest.clientContext,
          thriftRequest.product,
          userState)

      // Specify product-specific behavior mapping here
      // Currently, Home takes 10, and RUX takes 100
      val maxNumResults = params(RelatedTweetGlobalParams.MaxCandidatesPerRequestParam)

      RelatedTweetCandidateGeneratorQuery(
        internalId = thriftRequest.internalId,
        clientContext = thriftRequest.clientContext,
        product = product,
        maxNumResults = maxNumResults,
        impressedTweetList = thriftRequest.excludedTweetIds.getOrElse(Nil).toSet,
        params = params,
        requestUUID = requestUUID
      )
    }
  }

  private def buildAdsCandidateGeneratorQuery(
    thriftRequest: AdsRequest
  ): Future[AdsCandidateGeneratorQuery] = {
    val userId = thriftRequest.clientContext.userId.getOrElse(
      throw new IllegalArgumentException("userId must be present in the Thrift clientContext")
    )
    val product = thriftRequest.product
    val requestUUID = generateRequestUUID()
    userStateStore
      .get(userId).map { userStateOpt =>
        val userState = userStateOpt
          .getOrElse(UserState.EnumUnknownUserState(100))
        val params =
          paramsBuilder.buildFromClientContext(
            thriftRequest.clientContext,
            thriftRequest.product,
            userState)
        val maxNumResults = params(AdsParams.AdsCandidateGenerationMaxCandidatesNumParam)
        AdsCandidateGeneratorQuery(
          userId = userId,
          product = product,
          userState = userState,
          params = params,
          maxNumResults = maxNumResults,
          requestUUID = requestUUID
        )
      }
  }

  private def buildRelatedVideoTweetQuery(
    thriftRequest: RelatedVideoTweetRequest,
    requestUUID: Long
  ): Future[RelatedVideoTweetCandidateGeneratorQuery] = {

    val product = thriftRequest.product
    val scopedStats = statsReceiver
      .scope(product.toString).scope("RelatedVideoTweetRequest")
    val userStateFut: Future[UserState] = (thriftRequest.clientContext.userId match {
      case Some(userId) => userStateStore.get(userId)
      case None => Future.value(Some(UserState.EnumUnknownUserState(100)))
    }).map(_.getOrElse(UserState.EnumUnknownUserState(100)))

    userStateFut.map { userState =>
      scopedStats.scope("UserState").counter(userState.toString).incr()
      val params =
        paramsBuilder.buildFromClientContext(
          thriftRequest.clientContext,
          thriftRequest.product,
          userState)

      val maxNumResults = params(RelatedVideoTweetGlobalParams.MaxCandidatesPerRequestParam)

      RelatedVideoTweetCandidateGeneratorQuery(
        internalId = thriftRequest.internalId,
        clientContext = thriftRequest.clientContext,
        product = product,
        maxNumResults = maxNumResults,
        impressedTweetList = thriftRequest.excludedTweetIds.getOrElse(Nil).toSet,
        params = params,
        requestUUID = requestUUID
      )
    }

  }

  private def buildUtegTweetQuery(
    thriftRequest: UtegTweetRequest,
    requestUUID: Long
  ): Future[UtegTweetCandidateGeneratorQuery] = {

    val userId = thriftRequest.clientContext.userId.getOrElse(
      throw new IllegalArgumentException("userId must be present in the Thrift clientContext")
    )
    val product = thriftRequest.product
    val productContext = thriftRequest.productContext
    val scopedStats = statsReceiver
      .scope(product.toString).scope("UtegTweetRequest")

    userStateStore
      .get(userId).map { userStateOpt =>
        val userState = userStateOpt
          .getOrElse(UserState.EnumUnknownUserState(100))
        scopedStats.scope("UserState").counter(userState.toString).incr()

        val params =
          paramsBuilder.buildFromClientContext(
            thriftRequest.clientContext,
            thriftRequest.product,
            userState
          )

        // Specify product-specific behavior mapping here
        val maxNumResults = (product, productContext) match {
          case (t.Product.Home, Some(t.ProductContext.HomeContext(homeContext))) =>
            homeContext.maxResults.getOrElse(9999)
          case _ =>
            throw new IllegalArgumentException(
              s"Product ${product} and ProductContext ${productContext} are not allowed in CrMixer"
            )
        }

        UtegTweetCandidateGeneratorQuery(
          userId = userId,
          product = product,
          userState = userState,
          maxNumResults = maxNumResults,
          impressedTweetList = thriftRequest.excludedTweetIds.getOrElse(Nil).toSet,
          params = params,
          requestUUID = requestUUID
        )
      }

  }

  private def buildTopicTweetQuery(
    thriftRequest: TopicTweetRequest,
    requestUUID: Long
  ): TopicTweetCandidateGeneratorQuery = {
    val userId = thriftRequest.clientContext.userId.getOrElse(
      throw new IllegalArgumentException(
        "userId must be present in the TopicTweetRequest clientContext")
    )
    val product = thriftRequest.product
    val productContext = thriftRequest.productContext

    // Specify product-specific behavior mapping here
    val isVideoOnly = (product, productContext) match {
      case (t.Product.ExploreTopics, Some(t.ProductContext.ExploreContext(context))) =>
        context.isVideoOnly
      case (t.Product.TopicLandingPage, None) =>
        false
      case (t.Product.HomeTopicsBackfill, None) =>
        false
      case (t.Product.TopicTweetsStrato, None) =>
        false
      case _ =>
        throw new IllegalArgumentException(
          s"Product ${product} and ProductContext ${productContext} are not allowed in CrMixer"
        )
    }

    statsReceiver.scope(product.toString).counter(TopicTweetRequest.toString).incr()

    val params =
      paramsBuilder.buildFromClientContext(
        thriftRequest.clientContext,
        product,
        UserState.EnumUnknownUserState(100)
      )

    val topicIds = thriftRequest.topicIds.map { topicId =>
      TopicId(
        entityId = topicId,
        language = thriftRequest.clientContext.languageCode,
        country = None
      )
    }.toSet

    TopicTweetCandidateGeneratorQuery(
      userId = userId,
      topicIds = topicIds,
      product = product,
      maxNumResults = params(TopicTweetParams.MaxTopicTweetCandidatesParam),
      impressedTweetList = thriftRequest.excludedTweetIds.getOrElse(Nil).toSet,
      params = params,
      requestUUID = requestUUID,
      isVideoOnly = isVideoOnly
    )
  }

  private def buildFrsBasedTweetQuery(
    thriftRequest: FrsTweetRequest,
    requestUUID: Long
  ): Future[FrsTweetCandidateGeneratorQuery] = {
    val userId = thriftRequest.clientContext.userId.getOrElse(
      throw new IllegalArgumentException(
        "userId must be present in the FrsTweetRequest clientContext")
    )
    val product = thriftRequest.product
    val productContext = thriftRequest.productContext

    val scopedStats = statsReceiver
      .scope(product.toString).scope("FrsTweetRequest")

    userStateStore
      .get(userId).map { userStateOpt =>
        val userState = userStateOpt
          .getOrElse(UserState.EnumUnknownUserState(100))
        scopedStats.scope("UserState").counter(userState.toString).incr()

        val params =
          paramsBuilder.buildFromClientContext(
            thriftRequest.clientContext,
            thriftRequest.product,
            userState
          )
        val maxNumResults = (product, productContext) match {
          case (t.Product.Home, Some(t.ProductContext.HomeContext(homeContext))) =>
            homeContext.maxResults.getOrElse(
              params(FrsBasedCandidateGenerationMaxCandidatesNumParam))
          case _ =>
            params(FrsBasedCandidateGenerationMaxCandidatesNumParam)
        }

        FrsTweetCandidateGeneratorQuery(
          userId = userId,
          product = product,
          maxNumResults = maxNumResults,
          impressedTweetList = thriftRequest.excludedTweetIds.getOrElse(Nil).toSet,
          impressedUserList = thriftRequest.excludedUserIds.getOrElse(Nil).toSet,
          params = params,
          languageCodeOpt = thriftRequest.clientContext.languageCode,
          countryCodeOpt = thriftRequest.clientContext.countryCode,
          requestUUID = requestUUID
        )
      }
  }

  private def buildThriftResponse(
    candidates: Seq[RankedCandidate]
  ): CrMixerTweetResponse = {

    val tweets = candidates.map { candidate =>
      TweetRecommendation(
        tweetId = candidate.tweetId,
        score = candidate.predictionScore,
        metricTags = Some(MetricTagUtil.buildMetricTags(candidate)),
        latestSourceSignalTimestampInMillis =
          SignalTimestampStatsUtil.buildLatestSourceSignalTimestamp(candidate)
      )
    }
    signalTimestampStatsUtil.statsSignalTimestamp(tweets)
    CrMixerTweetResponse(tweets)
  }

  private def scribeTweetScoreFunnelSeries(
    candidates: Seq[RankedCandidate]
  ): Seq[RankedCandidate] = {
    // 202210210901 is a random number for code search of Lensview
    tweetScoreFunnelSeries.startNewSpan(
      name = "GetTweetRecommendationsTopLevelTweetSimilarityEngineType",
      codePtr = 202210210901L) {
      (
        candidates,
        candidates.map { candidate =>
          thriftlog.TweetDimensionMeasure(
            dimension = Some(
              thriftlog
                .RequestTweetDimension(
                  candidate.tweetId,
                  candidate.reasonChosen.similarityEngineInfo.similarityEngineType.value)),
            measure = Some(thriftlog.RequestTweetMeasure(candidate.predictionScore))
          )
        }
      )
    }
  }

  private def buildRelatedTweetResponse(candidates: Seq[InitialCandidate]): RelatedTweetResponse = {
    val tweets = candidates.map { candidate =>
      RelatedTweet(
        tweetId = candidate.tweetId,
        score = Some(candidate.getSimilarityScore),
        authorId = Some(candidate.tweetInfo.authorId)
      )
    }
    RelatedTweetResponse(tweets)
  }

  private def buildRelatedVideoTweetResponse(
    candidates: Seq[InitialCandidate]
  ): RelatedVideoTweetResponse = {
    val tweets = candidates.map { candidate =>
      RelatedVideoTweet(
        tweetId = candidate.tweetId,
        score = Some(candidate.getSimilarityScore)
      )
    }
    RelatedVideoTweetResponse(tweets)
  }

  private def buildUtegTweetResponse(
    candidates: Seq[TweetWithScoreAndSocialProof]
  ): UtegTweetResponse = {
    val tweets = candidates.map { candidate =>
      UtegTweet(
        tweetId = candidate.tweetId,
        score = candidate.score,
        socialProofByType = candidate.socialProofByType
      )
    }
    UtegTweetResponse(tweets)
  }

  private def buildAdsResponse(
    candidates: Seq[RankedAdsCandidate]
  ): AdsResponse = {
    AdsResponse(ads = candidates.map { candidate =>
      AdTweetRecommendation(
        tweetId = candidate.tweetId,
        score = candidate.predictionScore,
        lineItems = Some(candidate.lineItemInfo))
    })
  }

  private def cacheTweetRecommendationResults(
    request: CrMixerTweetRequest,
    response: Future[CrMixerTweetResponse]
  ): Unit = {

    val userId = request.clientContext.userId.getOrElse(
      throw new IllegalArgumentException(
        "userId must be present in getTweetRecommendations() Thrift clientContext"))

    if (decider.isAvailableForId(userId, DeciderConstants.getTweetRecommendationsCacheRate)) {
      response.map { crMixerTweetResponse =>
        {
          (
            request.product,
            request.clientContext.userId,
            crMixerTweetResponse.tweets.nonEmpty) match {
            case (t.Product.Home, Some(userId), true) =>
              tweetRecommendationResultsStore.put((userId, crMixerTweetResponse))
            case _ => Future.value(Unit)
          }
        }
      }
    }
  }
}
package com.twitter.cr_mixer.candidate_generation

import com.twitter.cr_mixer.blender.AdsBlender
import com.twitter.cr_mixer.logging.AdsRecommendationsScribeLogger
import com.twitter.cr_mixer.model.AdsCandidateGeneratorQuery
import com.twitter.cr_mixer.model.BlendedAdsCandidate
import com.twitter.cr_mixer.model.InitialAdsCandidate
import com.twitter.cr_mixer.model.RankedAdsCandidate
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.param.AdsParams
import com.twitter.cr_mixer.param.ConsumersBasedUserAdGraphParams
import com.twitter.cr_mixer.source_signal.RealGraphInSourceGraphFetcher
import com.twitter.cr_mixer.source_signal.SourceFetcher.FetcherQuery
import com.twitter.cr_mixer.source_signal.UssSourceSignalFetcher
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.UserId
import com.twitter.util.Future

import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class AdsCandidateGenerator @Inject() (
  ussSourceSignalFetcher: UssSourceSignalFetcher,
  realGraphInSourceGraphFetcher: RealGraphInSourceGraphFetcher,
  adsCandidateSourceRouter: AdsCandidateSourcesRouter,
  adsBlender: AdsBlender,
  scribeLogger: AdsRecommendationsScribeLogger,
  globalStats: StatsReceiver) {

  private val stats: StatsReceiver = globalStats.scope(this.getClass.getCanonicalName)
  private val fetchSourcesStats = stats.scope("fetchSources")
  private val fetchRealGraphSeedsStats = stats.scope("fetchRealGraphSeeds")
  private val fetchCandidatesStats = stats.scope("fetchCandidates")
  private val interleaveStats = stats.scope("interleave")
  private val rankStats = stats.scope("rank")

  def get(query: AdsCandidateGeneratorQuery): Future[Seq[RankedAdsCandidate]] = {
    val allStats = stats.scope("all")
    val perProductStats = stats.scope("perProduct", query.product.toString)

    StatsUtil.trackItemsStats(allStats) {
      StatsUtil.trackItemsStats(perProductStats) {
        for {
          // fetch source signals
          sourceSignals <- StatsUtil.trackBlockStats(fetchSourcesStats) {
            fetchSources(query)
          }
          realGraphSeeds <- StatsUtil.trackItemMapStats(fetchRealGraphSeedsStats) {
            fetchSeeds(query)
          }
          // get initial candidates from similarity engines
          // hydrate lineItemInfo and filter out non active ads
          initialCandidates <- StatsUtil.trackBlockStats(fetchCandidatesStats) {
            fetchCandidates(query, sourceSignals, realGraphSeeds)
          }

          // blend candidates
          blendedCandidates <- StatsUtil.trackItemsStats(interleaveStats) {
            interleave(initialCandidates)
          }

          rankedCandidates <- StatsUtil.trackItemsStats(rankStats) {
            rank(
              blendedCandidates,
              query.params(AdsParams.EnableScoreBoost),
              query.params(AdsParams.AdsCandidateGenerationScoreBoostFactor),
              rankStats)
          }
        } yield {
          rankedCandidates.take(query.maxNumResults)
        }
      }
    }

  }

  def fetchSources(
    query: AdsCandidateGeneratorQuery
  ): Future[Set[SourceInfo]] = {
    val fetcherQuery =
      FetcherQuery(query.userId, query.product, query.userState, query.params)
    ussSourceSignalFetcher.get(fetcherQuery).map(_.getOrElse(Seq.empty).toSet)
  }

  private def fetchCandidates(
    query: AdsCandidateGeneratorQuery,
    sourceSignals: Set[SourceInfo],
    realGraphSeeds: Map[UserId, Double]
  ): Future[Seq[Seq[InitialAdsCandidate]]] = {
    scribeLogger.scribeInitialAdsCandidates(
      query,
      adsCandidateSourceRouter
        .fetchCandidates(query.userId, sourceSignals, realGraphSeeds, query.params),
      query.params(AdsParams.EnableScribe)
    )

  }

  private def fetchSeeds(
    query: AdsCandidateGeneratorQuery
  ): Future[Map[UserId, Double]] = {
    if (query.params(ConsumersBasedUserAdGraphParams.EnableSourceParam)) {
      realGraphInSourceGraphFetcher
        .get(FetcherQuery(query.userId, query.product, query.userState, query.params))
        .map(_.map(_.seedWithScores).getOrElse(Map.empty))
    } else Future.value(Map.empty[UserId, Double])
  }

  private def interleave(
    candidates: Seq[Seq[InitialAdsCandidate]]
  ): Future[Seq[BlendedAdsCandidate]] = {
    adsBlender
      .blend(candidates)
  }

  private def rank(
    candidates: Seq[BlendedAdsCandidate],
    enableScoreBoost: Boolean,
    scoreBoostFactor: Double,
    statsReceiver: StatsReceiver,
  ): Future[Seq[RankedAdsCandidate]] = {

    val candidateSize = candidates.size
    val rankedCandidates = candidates.zipWithIndex.map {
      case (candidate, index) =>
        val score = 0.5 + 0.5 * ((candidateSize - index).toDouble / candidateSize)
        val boostedScore = if (enableScoreBoost) {
          statsReceiver.stat("boostedScore").add((100.0 * score * scoreBoostFactor).toFloat)
          score * scoreBoostFactor
        } else {
          statsReceiver.stat("score").add((100.0 * score).toFloat)
          score
        }
        candidate.toRankedAdsCandidate(boostedScore)
    }
    Future.value(rankedCandidates)
  }
}
package com.twitter.cr_mixer.candidate_generation

import com.twitter.contentrecommender.thriftscala.TweetInfo
import com.twitter.cr_mixer.logging.UtegTweetScribeLogger
import com.twitter.cr_mixer.filter.UtegFilterRunner
import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScoreAndSocialProof
import com.twitter.cr_mixer.model.UtegTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.similarity_engine.UserTweetEntityGraphSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.source_signal.RealGraphInSourceGraphFetcher
import com.twitter.cr_mixer.source_signal.SourceFetcher.FetcherQuery
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

@Singleton
class UtegTweetCandidateGenerator @Inject() (
  @Named(ModuleNames.UserTweetEntityGraphSimilarityEngine) userTweetEntityGraphSimilarityEngine: StandardSimilarityEngine[
    UserTweetEntityGraphSimilarityEngine.Query,
    TweetWithScoreAndSocialProof
  ],
  utegTweetScribeLogger: UtegTweetScribeLogger,
  tweetInfoStore: ReadableStore[TweetId, TweetInfo],
  realGraphInSourceGraphFetcher: RealGraphInSourceGraphFetcher,
  utegFilterRunner: UtegFilterRunner,
  globalStats: StatsReceiver) {

  private val stats: StatsReceiver = globalStats.scope(this.getClass.getCanonicalName)
  private val fetchSeedsStats = stats.scope("fetchSeeds")
  private val fetchCandidatesStats = stats.scope("fetchCandidates")
  private val utegFilterStats = stats.scope("utegFilter")
  private val rankStats = stats.scope("rank")

  def get(
    query: UtegTweetCandidateGeneratorQuery
  ): Future[Seq[TweetWithScoreAndSocialProof]] = {

    val allStats = stats.scope("all")
    val perProductStats = stats.scope("perProduct", query.product.toString)
    StatsUtil.trackItemsStats(allStats) {
      StatsUtil.trackItemsStats(perProductStats) {

        /**
         * The candidate we return in the end needs a social proof field, which isn't
         * supported by the any existing Candidate type, so we created TweetWithScoreAndSocialProof
         * instead.
         *
         * However, filters and light ranker expect Candidate-typed param to work. In order to minimise the
         * changes to them, we are doing conversions from/to TweetWithScoreAndSocialProof to/from Candidate
         * in this method.
         */
        for {
          realGraphSeeds <- StatsUtil.trackItemMapStats(fetchSeedsStats) {
            fetchSeeds(query)
          }
          initialTweets <- StatsUtil.trackItemsStats(fetchCandidatesStats) {
            fetchCandidates(query, realGraphSeeds)
          }
          initialCandidates <- convertToInitialCandidates(initialTweets)
          filteredCandidates <- StatsUtil.trackItemsStats(utegFilterStats) {
            utegFilter(query, initialCandidates)
          }
          rankedCandidates <- StatsUtil.trackItemsStats(rankStats) {
            rankCandidates(query, filteredCandidates)
          }
        } yield {
          val topTweets = rankedCandidates.take(query.maxNumResults)
          convertToTweets(topTweets, initialTweets.map(tweet => tweet.tweetId -> tweet).toMap)
        }
      }
    }
  }

  private def utegFilter(
    query: UtegTweetCandidateGeneratorQuery,
    candidates: Seq[InitialCandidate]
  ): Future[Seq[InitialCandidate]] = {
    utegFilterRunner.runSequentialFilters(query, Seq(candidates)).map(_.flatten)
  }

  private def fetchSeeds(
    query: UtegTweetCandidateGeneratorQuery
  ): Future[Map[UserId, Double]] = {
    realGraphInSourceGraphFetcher
      .get(FetcherQuery(query.userId, query.product, query.userState, query.params))
      .map(_.map(_.seedWithScores).getOrElse(Map.empty))
  }

  private[candidate_generation] def rankCandidates(
    query: UtegTweetCandidateGeneratorQuery,
    filteredCandidates: Seq[InitialCandidate],
  ): Future[Seq[RankedCandidate]] = {
    val blendedCandidates = filteredCandidates.map(candidate =>
      candidate.toBlendedCandidate(Seq(candidate.candidateGenerationInfo)))

    Future(
      blendedCandidates.map { candidate =>
        val score = candidate.getSimilarityScore
        candidate.toRankedCandidate(score)
      }
    )

  }

  def fetchCandidates(
    query: UtegTweetCandidateGeneratorQuery,
    realGraphSeeds: Map[UserId, Double],
  ): Future[Seq[TweetWithScoreAndSocialProof]] = {
    val engineQuery = UserTweetEntityGraphSimilarityEngine.fromParams(
      query.userId,
      realGraphSeeds,
      Some(query.impressedTweetList.toSeq),
      query.params
    )

    utegTweetScribeLogger.scribeInitialCandidates(
      query,
      userTweetEntityGraphSimilarityEngine.getCandidates(engineQuery).map(_.toSeq.flatten)
    )
  }

  private[candidate_generation] def convertToInitialCandidates(
    candidates: Seq[TweetWithScoreAndSocialProof],
  ): Future[Seq[InitialCandidate]] = {
    val tweetIds = candidates.map(_.tweetId).toSet
    Future.collect(tweetInfoStore.multiGet(tweetIds)).map { tweetInfos =>
      /** *
       * If tweetInfo does not exist, we will filter out this tweet candidate.
       */
      candidates.collect {
        case candidate if tweetInfos.getOrElse(candidate.tweetId, None).isDefined =>
          val tweetInfo = tweetInfos(candidate.tweetId)
            .getOrElse(throw new IllegalStateException("Check previous line's condition"))

          InitialCandidate(
            tweetId = candidate.tweetId,
            tweetInfo = tweetInfo,
            CandidateGenerationInfo(
              None,
              SimilarityEngineInfo(
                similarityEngineType = SimilarityEngineType.Uteg,
                modelId = None,
                score = Some(candidate.score)),
              Seq.empty
            )
          )
      }
    }
  }

  private[candidate_generation] def convertToTweets(
    candidates: Seq[RankedCandidate],
    tweetMap: Map[TweetId, TweetWithScoreAndSocialProof]
  ): Seq[TweetWithScoreAndSocialProof] = {
    candidates.map { candidate =>
      tweetMap
        .get(candidate.tweetId).map { tweet =>
          TweetWithScoreAndSocialProof(
            tweet.tweetId,
            candidate.predictionScore,
            tweet.socialProofByType
          )
        // The exception should never be thrown
        }.getOrElse(throw new Exception("Cannot find ranked candidate in original UTEG tweets"))
    }
  }
}
package com.twitter.cr_mixer.candidate_generation

import com.twitter.contentrecommender.thriftscala.TweetInfo
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TopicTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.TopicTweetWithScore
import com.twitter.cr_mixer.param.TopicTweetParams
import com.twitter.cr_mixer.similarity_engine.CertoTopicTweetSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SkitHighPrecisionTopicTweetSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SkitTopicTweetSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.cr_mixer.thriftscala.TopicTweet
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.servo.util.MemoizingStatsReceiver
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.thriftscala.TopicId
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Formerly CrTopic in legacy Content Recommender. This generator finds top Tweets per Topic.
 */
@Singleton
class TopicTweetCandidateGenerator @Inject() (
  certoTopicTweetSimilarityEngine: CertoTopicTweetSimilarityEngine,
  skitTopicTweetSimilarityEngine: SkitTopicTweetSimilarityEngine,
  skitHighPrecisionTopicTweetSimilarityEngine: SkitHighPrecisionTopicTweetSimilarityEngine,
  tweetInfoStore: ReadableStore[TweetId, TweetInfo],
  timeoutConfig: TimeoutConfig,
  globalStats: StatsReceiver) {
  private val timer = DefaultTimer
  private val stats: StatsReceiver = globalStats.scope(this.getClass.getCanonicalName)
  private val fetchCandidatesStats = stats.scope("fetchCandidates")
  private val filterCandidatesStats = stats.scope("filterCandidates")
  private val tweetyPieFilteredStats = filterCandidatesStats.stat("tweetypie_filtered")
  private val memoizedStatsReceiver = new MemoizingStatsReceiver(stats)

  def get(
    query: TopicTweetCandidateGeneratorQuery
  ): Future[Map[Long, Seq[TopicTweet]]] = {
    val maxTweetAge = query.params(TopicTweetParams.MaxTweetAge)
    val product = query.product
    val allStats = memoizedStatsReceiver.scope("all")
    val perProductStats = memoizedStatsReceiver.scope("perProduct", product.name)
    StatsUtil.trackMapValueStats(allStats) {
      StatsUtil.trackMapValueStats(perProductStats) {
        val result = for {
          retrievedTweets <- fetchCandidates(query)
          initialTweetCandidates <- convertToInitialCandidates(retrievedTweets)
          filteredTweetCandidates <- filterCandidates(
            initialTweetCandidates,
            maxTweetAge,
            query.isVideoOnly,
            query.impressedTweetList)
          rankedTweetCandidates = rankCandidates(filteredTweetCandidates)
          hydratedTweetCandidates = hydrateCandidates(rankedTweetCandidates)
        } yield {
          hydratedTweetCandidates.map {
            case (topicId, topicTweets) =>
              val topKTweets = topicTweets.take(query.maxNumResults)
              topicId -> topKTweets
          }
        }
        result.raiseWithin(timeoutConfig.topicTweetEndpointTimeout)(timer)
      }
    }
  }

  private def fetchCandidates(
    query: TopicTweetCandidateGeneratorQuery
  ): Future[Map[TopicId, Option[Seq[TopicTweetWithScore]]]] = {
    Future.collect {
      query.topicIds.map { topicId =>
        topicId -> StatsUtil.trackOptionStats(fetchCandidatesStats) {
          Future
            .join(
              certoTopicTweetSimilarityEngine.get(CertoTopicTweetSimilarityEngine
                .fromParams(topicId, query.isVideoOnly, query.params)),
              skitTopicTweetSimilarityEngine
                .get(SkitTopicTweetSimilarityEngine
                  .fromParams(topicId, query.isVideoOnly, query.params)),
              skitHighPrecisionTopicTweetSimilarityEngine
                .get(SkitHighPrecisionTopicTweetSimilarityEngine
                  .fromParams(topicId, query.isVideoOnly, query.params))
            ).map {
              case (certoTopicTweets, skitTfgTopicTweets, skitHighPrecisionTopicTweets) =>
                val uniqueCandidates = (certoTopicTweets.getOrElse(Nil) ++
                  skitTfgTopicTweets.getOrElse(Nil) ++
                  skitHighPrecisionTopicTweets.getOrElse(Nil))
                  .groupBy(_.tweetId).map {
                    case (_, dupCandidates) => dupCandidates.head
                  }.toSeq
                Some(uniqueCandidates)
            }
        }
      }.toMap
    }
  }

  private def convertToInitialCandidates(
    candidatesMap: Map[TopicId, Option[Seq[TopicTweetWithScore]]]
  ): Future[Map[TopicId, Seq[InitialCandidate]]] = {
    val initialCandidates = candidatesMap.map {
      case (topicId, candidatesOpt) =>
        val candidates = candidatesOpt.getOrElse(Nil)
        val tweetIds = candidates.map(_.tweetId).toSet
        val numTweetsPreFilter = tweetIds.size
        Future.collect(tweetInfoStore.multiGet(tweetIds)).map { tweetInfos =>
          /** *
           * If tweetInfo does not exist, we will filter out this tweet candidate.
           */
          val tweetyPieFilteredInitialCandidates = candidates.collect {
            case candidate if tweetInfos.getOrElse(candidate.tweetId, None).isDefined =>
              val tweetInfo = tweetInfos(candidate.tweetId)
                .getOrElse(throw new IllegalStateException("Check previous line's condition"))

              InitialCandidate(
                tweetId = candidate.tweetId,
                tweetInfo = tweetInfo,
                CandidateGenerationInfo(
                  None,
                  SimilarityEngineInfo(
                    similarityEngineType = candidate.similarityEngineType,
                    modelId = None,
                    score = Some(candidate.score)),
                  Seq.empty
                )
              )
          }
          val numTweetsPostFilter = tweetyPieFilteredInitialCandidates.size
          tweetyPieFilteredStats.add(numTweetsPreFilter - numTweetsPostFilter)
          topicId -> tweetyPieFilteredInitialCandidates
        }
    }

    Future.collect(initialCandidates.toSeq).map(_.toMap)
  }

  private def filterCandidates(
    topicTweetMap: Map[TopicId, Seq[InitialCandidate]],
    maxTweetAge: Duration,
    isVideoOnly: Boolean,
    excludeTweetIds: Set[TweetId]
  ): Future[Map[TopicId, Seq[InitialCandidate]]] = {

    val earliestTweetId = SnowflakeId.firstIdFor(Time.now - maxTweetAge)

    val filteredResults = topicTweetMap.map {
      case (topicId, tweetsWithScore) =>
        topicId -> StatsUtil.trackItemsStats(filterCandidatesStats) {

          val timeFilteredTweets =
            tweetsWithScore.filter { tweetWithScore =>
              tweetWithScore.tweetId >= earliestTweetId && !excludeTweetIds.contains(
                tweetWithScore.tweetId)
            }

          filterCandidatesStats
            .stat("exclude_and_time_filtered").add(tweetsWithScore.size - timeFilteredTweets.size)

          val tweetNudityFilteredTweets =
            timeFilteredTweets.collect {
              case tweet if tweet.tweetInfo.isPassTweetMediaNudityTag.contains(true) => tweet
            }

          filterCandidatesStats
            .stat("tweet_nudity_filtered").add(
              timeFilteredTweets.size - tweetNudityFilteredTweets.size)

          val userNudityFilteredTweets =
            tweetNudityFilteredTweets.collect {
              case tweet if tweet.tweetInfo.isPassUserNudityRateStrict.contains(true) => tweet
            }

          filterCandidatesStats
            .stat("user_nudity_filtered").add(
              tweetNudityFilteredTweets.size - userNudityFilteredTweets.size)

          val videoFilteredTweets = {
            if (isVideoOnly) {
              userNudityFilteredTweets.collect {
                case tweet if tweet.tweetInfo.hasVideo.contains(true) => tweet
              }
            } else {
              userNudityFilteredTweets
            }
          }

          Future.value(videoFilteredTweets)
        }
    }
    Future.collect(filteredResults)
  }

  private def rankCandidates(
    tweetCandidatesMap: Map[TopicId, Seq[InitialCandidate]]
  ): Map[TopicId, Seq[InitialCandidate]] = {
    tweetCandidatesMap.mapValues { tweetCandidates =>
      tweetCandidates.sortBy { candidate =>
        -candidate.tweetInfo.favCount
      }
    }
  }

  private def hydrateCandidates(
    topicCandidatesMap: Map[TopicId, Seq[InitialCandidate]]
  ): Map[Long, Seq[TopicTweet]] = {
    topicCandidatesMap.map {
      case (topicId, tweetsWithScore) =>
        topicId.entityId ->
          tweetsWithScore.map { tweetWithScore =>
            val similarityEngineType: SimilarityEngineType =
              tweetWithScore.candidateGenerationInfo.similarityEngineInfo.similarityEngineType
            TopicTweet(
              tweetId = tweetWithScore.tweetId,
              score = tweetWithScore.getSimilarityScore,
              similarityEngineType = similarityEngineType
            )
          }
    }
  }
}
package com.twitter.cr_mixer.candidate_generation

import com.twitter.contentrecommender.thriftscala.TweetInfo
import com.twitter.cr_mixer.filter.PreRankFilterRunner
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.RelatedVideoTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.TweetBasedUnifiedSimilarityEngine
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

@Singleton
class RelatedVideoTweetCandidateGenerator @Inject() (
  @Named(ModuleNames.TweetBasedUnifiedSimilarityEngine) tweetBasedUnifiedSimilarityEngine: StandardSimilarityEngine[
    TweetBasedUnifiedSimilarityEngine.Query,
    TweetWithCandidateGenerationInfo
  ],
  preRankFilterRunner: PreRankFilterRunner,
  tweetInfoStore: ReadableStore[TweetId, TweetInfo],
  globalStats: StatsReceiver) {

  private val stats: StatsReceiver = globalStats.scope(this.getClass.getCanonicalName)
  private val fetchCandidatesStats = stats.scope("fetchCandidates")
  private val preRankFilterStats = stats.scope("preRankFilter")

  def get(
    query: RelatedVideoTweetCandidateGeneratorQuery
  ): Future[Seq[InitialCandidate]] = {

    val allStats = stats.scope("all")
    val perProductStats = stats.scope("perProduct", query.product.toString)
    StatsUtil.trackItemsStats(allStats) {
      StatsUtil.trackItemsStats(perProductStats) {
        for {
          initialCandidates <- StatsUtil.trackBlockStats(fetchCandidatesStats) {
            fetchCandidates(query)
          }
          filteredCandidates <- StatsUtil.trackBlockStats(preRankFilterStats) {
            preRankFilter(query, initialCandidates)
          }
        } yield {
          filteredCandidates.headOption
            .getOrElse(
              throw new UnsupportedOperationException(
                "RelatedVideoTweetCandidateGenerator results invalid")
            ).take(query.maxNumResults)
        }
      }
    }
  }

  def fetchCandidates(
    query: RelatedVideoTweetCandidateGeneratorQuery
  ): Future[Seq[Seq[InitialCandidate]]] = {
    query.internalId match {
      case InternalId.TweetId(_) =>
        getCandidatesFromSimilarityEngine(
          query,
          TweetBasedUnifiedSimilarityEngine.fromParamsForRelatedVideoTweet,
          tweetBasedUnifiedSimilarityEngine.getCandidates)
      case _ =>
        throw new UnsupportedOperationException(
          "RelatedVideoTweetCandidateGenerator gets invalid InternalId")
    }
  }

  /***
   * fetch Candidates from TweetBased/ProducerBased Unified Similarity Engine,
   * and apply VF filter based on TweetInfoStore
   * To align with the downstream processing (filter, rank), we tend to return a Seq[Seq[InitialCandidate]]
   * instead of a Seq[Candidate] even though we only have a Seq in it.
   */
  private def getCandidatesFromSimilarityEngine[QueryType](
    query: RelatedVideoTweetCandidateGeneratorQuery,
    fromParamsForRelatedVideoTweet: (InternalId, configapi.Params) => QueryType,
    getFunc: QueryType => Future[Option[Seq[TweetWithCandidateGenerationInfo]]]
  ): Future[Seq[Seq[InitialCandidate]]] = {

    /***
     * We wrap the query to be a Seq of queries for the Sim Engine to ensure evolvability of candidate generation
     * and as a result, it will return Seq[Seq[InitialCandidate]]
     */
    val engineQueries =
      Seq(fromParamsForRelatedVideoTweet(query.internalId, query.params))

    Future
      .collect {
        engineQueries.map { query =>
          for {
            candidates <- getFunc(query)
            prefilterCandidates <- convertToInitialCandidates(
              candidates.toSeq.flatten
            )
          } yield prefilterCandidates
        }
      }
  }

  private def preRankFilter(
    query: RelatedVideoTweetCandidateGeneratorQuery,
    candidates: Seq[Seq[InitialCandidate]]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    preRankFilterRunner
      .runSequentialFilters(query, candidates)
  }

  private[candidate_generation] def convertToInitialCandidates(
    candidates: Seq[TweetWithCandidateGenerationInfo],
  ): Future[Seq[InitialCandidate]] = {
    val tweetIds = candidates.map(_.tweetId).toSet
    Future.collect(tweetInfoStore.multiGet(tweetIds)).map { tweetInfos =>
      /***
       * If tweetInfo does not exist, we will filter out this tweet candidate.
       * This tweetInfo filter also acts as the VF filter
       */
      candidates.collect {
        case candidate if tweetInfos.getOrElse(candidate.tweetId, None).isDefined =>
          val tweetInfo = tweetInfos(candidate.tweetId)
            .getOrElse(throw new IllegalStateException("Check previous line's condition"))

          InitialCandidate(
            tweetId = candidate.tweetId,
            tweetInfo = tweetInfo,
            candidate.candidateGenerationInfo
          )
      }
    }
  }
}
package com.twitter.cr_mixer.candidate_generation

import com.twitter.cr_mixer.blender.SwitchBlender
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.filter.PostRankFilterRunner
import com.twitter.cr_mixer.filter.PreRankFilterRunner
import com.twitter.cr_mixer.logging.CrMixerScribeLogger
import com.twitter.cr_mixer.model.BlendedCandidate
import com.twitter.cr_mixer.model.CrCandidateGeneratorQuery
import com.twitter.cr_mixer.model.GraphSourceInfo
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.param.RankerParams
import com.twitter.cr_mixer.param.RecentNegativeSignalParams
import com.twitter.cr_mixer.ranker.SwitchRanker
import com.twitter.cr_mixer.source_signal.SourceInfoRouter
import com.twitter.cr_mixer.source_signal.UssStore.EnabledNegativeSourceTypes
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.util.Future
import com.twitter.util.JavaTimer
import com.twitter.util.Timer

import javax.inject.Inject
import javax.inject.Singleton

/**
 * For now it performs the main steps as follows:
 * 1. Source signal (via USS, FRS) fetch
 * 2. Candidate generation
 * 3. Filtering
 * 4. Interleave blender
 * 5. Ranker
 * 6. Post-ranker filter
 * 7. Truncation
 */
@Singleton
class CrCandidateGenerator @Inject() (
  sourceInfoRouter: SourceInfoRouter,
  candidateSourceRouter: CandidateSourcesRouter,
  switchBlender: SwitchBlender,
  preRankFilterRunner: PreRankFilterRunner,
  postRankFilterRunner: PostRankFilterRunner,
  switchRanker: SwitchRanker,
  crMixerScribeLogger: CrMixerScribeLogger,
  timeoutConfig: TimeoutConfig,
  globalStats: StatsReceiver) {
  private val timer: Timer = new JavaTimer(true)

  private val stats: StatsReceiver = globalStats.scope(this.getClass.getCanonicalName)

  private val fetchSourcesStats = stats.scope("fetchSources")
  private val fetchPositiveSourcesStats = stats.scope("fetchPositiveSources")
  private val fetchNegativeSourcesStats = stats.scope("fetchNegativeSources")
  private val fetchCandidatesStats = stats.scope("fetchCandidates")
  private val fetchCandidatesAfterFilterStats = stats.scope("fetchCandidatesAfterFilter")
  private val preRankFilterStats = stats.scope("preRankFilter")
  private val interleaveStats = stats.scope("interleave")
  private val rankStats = stats.scope("rank")
  private val postRankFilterStats = stats.scope("postRankFilter")
  private val blueVerifiedTweetStats = stats.scope("blueVerifiedTweetStats")
  private val blueVerifiedTweetStatsPerSimilarityEngine =
    stats.scope("blueVerifiedTweetStatsPerSimilarityEngine")

  def get(query: CrCandidateGeneratorQuery): Future[Seq[RankedCandidate]] = {
    val allStats = stats.scope("all")
    val perProductStats = stats.scope("perProduct", query.product.toString)
    val perProductBlueVerifiedStats =
      blueVerifiedTweetStats.scope("perProduct", query.product.toString)

    StatsUtil.trackItemsStats(allStats) {
      trackResultStats(perProductStats) {
        StatsUtil.trackItemsStats(perProductStats) {
          val result = for {
            (sourceSignals, sourceGraphsMap) <- StatsUtil.trackBlockStats(fetchSourcesStats) {
              fetchSources(query)
            }
            initialCandidates <- StatsUtil.trackBlockStats(fetchCandidatesAfterFilterStats) {
              // find the positive and negative signals
              val (positiveSignals, negativeSignals) = sourceSignals.partition { signal =>
                !EnabledNegativeSourceTypes.contains(signal.sourceType)
              }
              fetchPositiveSourcesStats.stat("size").add(positiveSignals.size)
              fetchNegativeSourcesStats.stat("size").add(negativeSignals.size)

              // find the positive signals to keep, removing block and muted users
              val filteredSourceInfo =
                if (negativeSignals.nonEmpty && query.params(
                    RecentNegativeSignalParams.EnableSourceParam)) {
                  filterSourceInfo(positiveSignals, negativeSignals)
                } else {
                  positiveSignals
                }

              // fetch candidates from the positive signals
              StatsUtil.trackBlockStats(fetchCandidatesStats) {
                fetchCandidates(query, filteredSourceInfo, sourceGraphsMap)
              }
            }
            filteredCandidates <- StatsUtil.trackBlockStats(preRankFilterStats) {
              preRankFilter(query, initialCandidates)
            }
            interleavedCandidates <- StatsUtil.trackItemsStats(interleaveStats) {
              interleave(query, filteredCandidates)
            }
            rankedCandidates <- StatsUtil.trackItemsStats(rankStats) {
              val candidatesToRank =
                interleavedCandidates.take(query.params(RankerParams.MaxCandidatesToRank))
              rank(query, candidatesToRank)
            }
            postRankFilterCandidates <- StatsUtil.trackItemsStats(postRankFilterStats) {
              postRankFilter(query, rankedCandidates)
            }
          } yield {
            trackTopKStats(
              800,
              postRankFilterCandidates,
              isQueryK = false,
              perProductBlueVerifiedStats)
            trackTopKStats(
              400,
              postRankFilterCandidates,
              isQueryK = false,
              perProductBlueVerifiedStats)
            trackTopKStats(
              query.maxNumResults,
              postRankFilterCandidates,
              isQueryK = true,
              perProductBlueVerifiedStats)

            val (blueVerifiedTweets, remainingTweets) =
              postRankFilterCandidates.partition(
                _.tweetInfo.hasBlueVerifiedAnnotation.contains(true))
            val topKBlueVerified = blueVerifiedTweets.take(query.maxNumResults)
            val topKRemaining = remainingTweets.take(query.maxNumResults - topKBlueVerified.size)

            trackBlueVerifiedTweetStats(topKBlueVerified, perProductBlueVerifiedStats)

            if (topKBlueVerified.nonEmpty && query.params(RankerParams.EnableBlueVerifiedTopK)) {
              topKBlueVerified ++ topKRemaining
            } else {
              postRankFilterCandidates
            }
          }
          result.raiseWithin(timeoutConfig.serviceTimeout)(timer)
        }
      }
    }
  }

  private def fetchSources(
    query: CrCandidateGeneratorQuery
  ): Future[(Set[SourceInfo], Map[String, Option[GraphSourceInfo]])] = {
    crMixerScribeLogger.scribeSignalSources(
      query,
      sourceInfoRouter
        .get(query.userId, query.product, query.userState, query.params))
  }

  private def filterSourceInfo(
    positiveSignals: Set[SourceInfo],
    negativeSignals: Set[SourceInfo]
  ): Set[SourceInfo] = {
    val filterUsers: Set[Long] = negativeSignals.flatMap {
      case SourceInfo(_, InternalId.UserId(userId), _) => Some(userId)
      case _ => None
    }

    positiveSignals.filter {
      case SourceInfo(_, InternalId.UserId(userId), _) => !filterUsers.contains(userId)
      case _ => true
    }
  }

  def fetchCandidates(
    query: CrCandidateGeneratorQuery,
    sourceSignals: Set[SourceInfo],
    sourceGraphs: Map[String, Option[GraphSourceInfo]]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    val initialCandidates = candidateSourceRouter
      .fetchCandidates(
        query.userId,
        sourceSignals,
        sourceGraphs,
        query.params
      )

    initialCandidates.map(_.flatten.map { candidate =>
      if (candidate.tweetInfo.hasBlueVerifiedAnnotation.contains(true)) {
        blueVerifiedTweetStatsPerSimilarityEngine
          .scope(query.product.toString).scope(
            candidate.candidateGenerationInfo.contributingSimilarityEngines.head.similarityEngineType.toString).counter(
            candidate.tweetInfo.authorId.toString).incr()
      }
    })

    crMixerScribeLogger.scribeInitialCandidates(
      query,
      initialCandidates
    )
  }

  private def preRankFilter(
    query: CrCandidateGeneratorQuery,
    candidates: Seq[Seq[InitialCandidate]]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    crMixerScribeLogger.scribePreRankFilterCandidates(
      query,
      preRankFilterRunner
        .runSequentialFilters(query, candidates))
  }

  private def postRankFilter(
    query: CrCandidateGeneratorQuery,
    candidates: Seq[RankedCandidate]
  ): Future[Seq[RankedCandidate]] = {
    postRankFilterRunner.run(query, candidates)
  }

  private def interleave(
    query: CrCandidateGeneratorQuery,
    candidates: Seq[Seq[InitialCandidate]]
  ): Future[Seq[BlendedCandidate]] = {
    crMixerScribeLogger.scribeInterleaveCandidates(
      query,
      switchBlender
        .blend(query.params, query.userState, candidates))
  }

  private def rank(
    query: CrCandidateGeneratorQuery,
    candidates: Seq[BlendedCandidate],
  ): Future[Seq[RankedCandidate]] = {
    crMixerScribeLogger.scribeRankedCandidates(
      query,
      switchRanker.rank(query, candidates)
    )
  }

  private def trackResultStats(
    stats: StatsReceiver
  )(
    fn: => Future[Seq[RankedCandidate]]
  ): Future[Seq[RankedCandidate]] = {
    fn.onSuccess { candidates =>
      trackReasonChosenSourceTypeStats(candidates, stats)
      trackReasonChosenSimilarityEngineStats(candidates, stats)
      trackPotentialReasonsSourceTypeStats(candidates, stats)
      trackPotentialReasonsSimilarityEngineStats(candidates, stats)
    }
  }

  private def trackReasonChosenSourceTypeStats(
    candidates: Seq[RankedCandidate],
    stats: StatsReceiver
  ): Unit = {
    candidates
      .groupBy(_.reasonChosen.sourceInfoOpt.map(_.sourceType))
      .foreach {
        case (sourceTypeOpt, rankedCands) =>
          val sourceType = sourceTypeOpt.map(_.toString).getOrElse("RequesterId") // default
          stats.stat("reasonChosen", "sourceType", sourceType, "size").add(rankedCands.size)
      }
  }

  private def trackReasonChosenSimilarityEngineStats(
    candidates: Seq[RankedCandidate],
    stats: StatsReceiver
  ): Unit = {
    candidates
      .groupBy(_.reasonChosen.similarityEngineInfo.similarityEngineType)
      .foreach {
        case (seInfoType, rankedCands) =>
          stats
            .stat("reasonChosen", "similarityEngine", seInfoType.toString, "size").add(
              rankedCands.size)
      }
  }

  private def trackPotentialReasonsSourceTypeStats(
    candidates: Seq[RankedCandidate],
    stats: StatsReceiver
  ): Unit = {
    candidates
      .flatMap(_.potentialReasons.map(_.sourceInfoOpt.map(_.sourceType)))
      .groupBy(source => source)
      .foreach {
        case (sourceInfoOpt, seq) =>
          val sourceType = sourceInfoOpt.map(_.toString).getOrElse("RequesterId") // default
          stats.stat("potentialReasons", "sourceType", sourceType, "size").add(seq.size)
      }
  }

  private def trackPotentialReasonsSimilarityEngineStats(
    candidates: Seq[RankedCandidate],
    stats: StatsReceiver
  ): Unit = {
    candidates
      .flatMap(_.potentialReasons.map(_.similarityEngineInfo.similarityEngineType))
      .groupBy(se => se)
      .foreach {
        case (seType, seq) =>
          stats.stat("potentialReasons", "similarityEngine", seType.toString, "size").add(seq.size)
      }
  }

  private def trackBlueVerifiedTweetStats(
    candidates: Seq[RankedCandidate],
    statsReceiver: StatsReceiver
  ): Unit = {
    candidates.foreach { candidate =>
      if (candidate.tweetInfo.hasBlueVerifiedAnnotation.contains(true)) {
        statsReceiver.counter(candidate.tweetInfo.authorId.toString).incr()
        statsReceiver
          .scope(candidate.tweetInfo.authorId.toString).counter(candidate.tweetId.toString).incr()
      }
    }
  }

  private def trackTopKStats(
    k: Int,
    tweetCandidates: Seq[RankedCandidate],
    isQueryK: Boolean,
    statsReceiver: StatsReceiver
  ): Unit = {
    val (topK, beyondK) = tweetCandidates.splitAt(k)

    val blueVerifiedIds = tweetCandidates.collect {
      case candidate if candidate.tweetInfo.hasBlueVerifiedAnnotation.contains(true) =>
        candidate.tweetInfo.authorId
    }.toSet

    blueVerifiedIds.foreach { blueVerifiedId =>
      val numTweetsTopK = topK.count(_.tweetInfo.authorId == blueVerifiedId)
      val numTweetsBeyondK = beyondK.count(_.tweetInfo.authorId == blueVerifiedId)

      if (isQueryK) {
        statsReceiver.scope(blueVerifiedId.toString).stat(s"topK").add(numTweetsTopK)
        statsReceiver
          .scope(blueVerifiedId.toString).stat(s"beyondK").add(numTweetsBeyondK)
      } else {
        statsReceiver.scope(blueVerifiedId.toString).stat(s"top$k").add(numTweetsTopK)
        statsReceiver
          .scope(blueVerifiedId.toString).stat(s"beyond$k").add(numTweetsBeyondK)
      }
    }
  }
}
 package com.twitter.cr_mixer.candidate_generation

import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.InitialAdsCandidate
import com.twitter.cr_mixer.model.ModelConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.ConsumersBasedUserAdGraphParams
import com.twitter.cr_mixer.param.ConsumerBasedWalsParams
import com.twitter.cr_mixer.param.ConsumerEmbeddingBasedCandidateGenerationParams
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.param.InterestedInParams
import com.twitter.cr_mixer.param.ProducerBasedCandidateGenerationParams
import com.twitter.cr_mixer.param.SimClustersANNParams
import com.twitter.cr_mixer.param.TweetBasedCandidateGenerationParams
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.similarity_engine.ConsumerBasedWalsSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.ConsumersBasedUserAdGraphSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.FilterUtil
import com.twitter.cr_mixer.similarity_engine.HnswANNEngineQuery
import com.twitter.cr_mixer.similarity_engine.HnswANNSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.ProducerBasedUserAdGraphSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimClustersANNSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimClustersANNSimilarityEngine.Query
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.TweetBasedUserAdGraphSimilarityEngine
import com.twitter.cr_mixer.thriftscala.LineItemInfo
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.common.ModelVersions
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.EmbeddingType
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.timelines.configapi.Params
import com.twitter.util.Future

import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

@Singleton
case class AdsCandidateSourcesRouter @Inject() (
  activePromotedTweetStore: ReadableStore[TweetId, Seq[LineItemInfo]],
  decider: CrMixerDecider,
  @Named(ModuleNames.SimClustersANNSimilarityEngine) simClustersANNSimilarityEngine: StandardSimilarityEngine[
    Query,
    TweetWithScore
  ],
  @Named(ModuleNames.TweetBasedUserAdGraphSimilarityEngine)
  tweetBasedUserAdGraphSimilarityEngine: StandardSimilarityEngine[
    TweetBasedUserAdGraphSimilarityEngine.Query,
    TweetWithScore
  ],
  @Named(ModuleNames.ConsumersBasedUserAdGraphSimilarityEngine)
  consumersBasedUserAdGraphSimilarityEngine: StandardSimilarityEngine[
    ConsumersBasedUserAdGraphSimilarityEngine.Query,
    TweetWithScore
  ],
  @Named(ModuleNames.ProducerBasedUserAdGraphSimilarityEngine)
  producerBasedUserAdGraphSimilarityEngine: StandardSimilarityEngine[
    ProducerBasedUserAdGraphSimilarityEngine.Query,
    TweetWithScore
  ],
  @Named(ModuleNames.TweetBasedTwHINANNSimilarityEngine)
  tweetBasedTwHINANNSimilarityEngine: HnswANNSimilarityEngine,
  @Named(ModuleNames.ConsumerEmbeddingBasedTwHINANNSimilarityEngine) consumerTwHINANNSimilarityEngine: HnswANNSimilarityEngine,
  @Named(ModuleNames.ConsumerBasedWalsSimilarityEngine)
  consumerBasedWalsSimilarityEngine: StandardSimilarityEngine[
    ConsumerBasedWalsSimilarityEngine.Query,
    TweetWithScore
  ],
  globalStats: StatsReceiver,
) {

  import AdsCandidateSourcesRouter._

  val stats: StatsReceiver = globalStats.scope(this.getClass.getSimpleName)

  def fetchCandidates(
    requestUserId: UserId,
    sourceSignals: Set[SourceInfo],
    realGraphSeeds: Map[UserId, Double],
    params: configapi.Params
  ): Future[Seq[Seq[InitialAdsCandidate]]] = {

    val simClustersANN1ConfigId = params(SimClustersANNParams.SimClustersANN1ConfigId)

    val tweetBasedSANNMinScore = params(
      TweetBasedCandidateGenerationParams.SimClustersMinScoreParam)
    val tweetBasedSANN1Candidates =
      if (params(TweetBasedCandidateGenerationParams.EnableSimClustersANN1Param)) {
        Future.collect(
          CandidateSourcesRouter.getTweetBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getSimClustersANNCandidates(
              requestUserId,
              Some(sourceInfo),
              params,
              simClustersANN1ConfigId,
              tweetBasedSANNMinScore)
          })
      } else Future.value(Seq.empty)

    val simClustersANN2ConfigId = params(SimClustersANNParams.SimClustersANN2ConfigId)
    val tweetBasedSANN2Candidates =
      if (params(TweetBasedCandidateGenerationParams.EnableSimClustersANN2Param)) {
        Future.collect(
          CandidateSourcesRouter.getTweetBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getSimClustersANNCandidates(
              requestUserId,
              Some(sourceInfo),
              params,
              simClustersANN2ConfigId,
              tweetBasedSANNMinScore)
          })
      } else Future.value(Seq.empty)

    val tweetBasedUagCandidates =
      if (params(TweetBasedCandidateGenerationParams.EnableUAGParam)) {
        Future.collect(
          CandidateSourcesRouter.getTweetBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getTweetBasedUserAdGraphCandidates(Some(sourceInfo), params)
          })
      } else Future.value(Seq.empty)

    val realGraphInNetworkBasedUagCandidates =
      if (params(ConsumersBasedUserAdGraphParams.EnableSourceParam)) {
        getRealGraphConsumersBasedUserAdGraphCandidates(realGraphSeeds, params).map(Seq(_))
      } else Future.value(Seq.empty)

    val producerBasedUagCandidates =
      if (params(ProducerBasedCandidateGenerationParams.EnableUAGParam)) {
        Future.collect(
          CandidateSourcesRouter.getProducerBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getProducerBasedUserAdGraphCandidates(Some(sourceInfo), params)
          })
      } else Future.value(Seq.empty)

    val tweetBasedTwhinAdsCandidates =
      if (params(TweetBasedCandidateGenerationParams.EnableTwHINParam)) {
        Future.collect(
          CandidateSourcesRouter.getTweetBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getTwHINAdsCandidates(
              tweetBasedTwHINANNSimilarityEngine,
              SimilarityEngineType.TweetBasedTwHINANN,
              requestUserId,
              Some(sourceInfo),
              ModelConfig.DebuggerDemo)
          })
      } else Future.value(Seq.empty)

    val producerBasedSANNMinScore = params(
      ProducerBasedCandidateGenerationParams.SimClustersMinScoreParam)
    val producerBasedSANN1Candidates =
      if (params(ProducerBasedCandidateGenerationParams.EnableSimClustersANN1Param)) {
        Future.collect(
          CandidateSourcesRouter.getProducerBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getSimClustersANNCandidates(
              requestUserId,
              Some(sourceInfo),
              params,
              simClustersANN1ConfigId,
              producerBasedSANNMinScore)
          })
      } else Future.value(Seq.empty)
    val producerBasedSANN2Candidates =
      if (params(ProducerBasedCandidateGenerationParams.EnableSimClustersANN2Param)) {
        Future.collect(
          CandidateSourcesRouter.getProducerBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getSimClustersANNCandidates(
              requestUserId,
              Some(sourceInfo),
              params,
              simClustersANN2ConfigId,
              producerBasedSANNMinScore)
          })
      } else Future.value(Seq.empty)

    val interestedInMinScore = params(InterestedInParams.MinScoreParam)
    val interestedInSANN1Candidates = if (params(InterestedInParams.EnableSimClustersANN1Param)) {
      getSimClustersANNCandidates(
        requestUserId,
        None,
        params,
        simClustersANN1ConfigId,
        interestedInMinScore).map(Seq(_))
    } else Future.value(Seq.empty)

    val interestedInSANN2Candidates = if (params(InterestedInParams.EnableSimClustersANN2Param)) {
      getSimClustersANNCandidates(
        requestUserId,
        None,
        params,
        simClustersANN2ConfigId,
        interestedInMinScore).map(Seq(_))
    } else Future.value(Seq.empty)

    val consumerTwHINAdsCandidates =
      if (params(ConsumerEmbeddingBasedCandidateGenerationParams.EnableTwHINParam)) {
        getTwHINAdsCandidates(
          consumerTwHINANNSimilarityEngine,
          SimilarityEngineType.ConsumerEmbeddingBasedTwHINANN,
          requestUserId,
          None,
          ModelConfig.DebuggerDemo).map(Seq(_))
      } else Future.value(Seq.empty)

    val consumerBasedWalsCandidates =
      if (params(
          ConsumerBasedWalsParams.EnableSourceParam
        )) {
        getConsumerBasedWalsCandidates(sourceSignals, params)
      }.map {
        Seq(_)
      }
      else Future.value(Seq.empty)

    Future
      .collect(Seq(
        tweetBasedSANN1Candidates,
        tweetBasedSANN2Candidates,
        tweetBasedUagCandidates,
        tweetBasedTwhinAdsCandidates,
        producerBasedUagCandidates,
        producerBasedSANN1Candidates,
        producerBasedSANN2Candidates,
        realGraphInNetworkBasedUagCandidates,
        interestedInSANN1Candidates,
        interestedInSANN2Candidates,
        consumerTwHINAdsCandidates,
        consumerBasedWalsCandidates,
      )).map(_.flatten).map { tweetsWithCGInfoSeq =>
        Future.collect(
          tweetsWithCGInfoSeq.map(candidates => convertToInitialCandidates(candidates, stats)))
      }.flatten.map { candidatesLists =>
        val result = candidatesLists.filter(_.nonEmpty)
        stats.stat("numOfSequences").add(result.size)
        stats.stat("flattenCandidatesWithDup").add(result.flatten.size)
        result
      }
  }

  private[candidate_generation] def convertToInitialCandidates(
    candidates: Seq[TweetWithCandidateGenerationInfo],
    stats: StatsReceiver
  ): Future[Seq[InitialAdsCandidate]] = {
    val tweetIds = candidates.map(_.tweetId).toSet
    stats.stat("initialCandidateSizeBeforeLineItemFilter").add(tweetIds.size)
    Future.collect(activePromotedTweetStore.multiGet(tweetIds)).map { lineItemInfos =>
      /** *
       * If lineItemInfo does not exist, we will filter out the promoted tweet as it cannot be targeted and ranked in admixer
       */
      val filteredCandidates = candidates.collect {
        case candidate if lineItemInfos.getOrElse(candidate.tweetId, None).isDefined =>
          val lineItemInfo = lineItemInfos(candidate.tweetId)
            .getOrElse(throw new IllegalStateException("Check previous line's condition"))

          InitialAdsCandidate(
            tweetId = candidate.tweetId,
            lineItemInfo = lineItemInfo,
            candidate.candidateGenerationInfo
          )
      }
      stats.stat("initialCandidateSizeAfterLineItemFilter").add(filteredCandidates.size)
      filteredCandidates
    }
  }

  private[candidate_generation] def getSimClustersANNCandidates(
    requestUserId: UserId,
    sourceInfo: Option[SourceInfo],
    params: configapi.Params,
    configId: String,
    minScore: Double
  ) = {

    val simClustersModelVersion =
      ModelVersions.Enum.enumToSimClustersModelVersionMap(params(GlobalParams.ModelVersionParam))

    val embeddingType =
      if (sourceInfo.isEmpty) {
        params(InterestedInParams.InterestedInEmbeddingIdParam).embeddingType
      } else getSimClustersANNEmbeddingType(sourceInfo.get)
    val query = SimClustersANNSimilarityEngine.fromParams(
      if (sourceInfo.isEmpty) InternalId.UserId(requestUserId) else sourceInfo.get.internalId,
      embeddingType,
      simClustersModelVersion,
      configId,
      params
    )

    // dark traffic to simclusters-ann-2
    if (decider.isAvailable(DeciderConstants.enableSimClustersANN2DarkTrafficDeciderKey)) {
      val simClustersANN2ConfigId = params(SimClustersANNParams.SimClustersANN2ConfigId)
      val sann2Query = SimClustersANNSimilarityEngine.fromParams(
        if (sourceInfo.isEmpty) InternalId.UserId(requestUserId) else sourceInfo.get.internalId,
        embeddingType,
        simClustersModelVersion,
        simClustersANN2ConfigId,
        params
      )
      simClustersANNSimilarityEngine
        .getCandidates(sann2Query)
    }

    simClustersANNSimilarityEngine
      .getCandidates(query).map(_.getOrElse(Seq.empty)).map(_.filter(_.score > minScore).map {
        tweetWithScore =>
          val similarityEngineInfo = SimClustersANNSimilarityEngine
            .toSimilarityEngineInfo(query, tweetWithScore.score)
          TweetWithCandidateGenerationInfo(
            tweetWithScore.tweetId,
            CandidateGenerationInfo(
              sourceInfo,
              similarityEngineInfo,
              Seq(similarityEngineInfo)
            ))
      })
  }

  private[candidate_generation] def getProducerBasedUserAdGraphCandidates(
    sourceInfo: Option[SourceInfo],
    params: configapi.Params
  ) = {

    val query = ProducerBasedUserAdGraphSimilarityEngine.fromParams(
      sourceInfo.get.internalId,
      params
    )
    producerBasedUserAdGraphSimilarityEngine
      .getCandidates(query).map(_.getOrElse(Seq.empty)).map(_.map { tweetWithScore =>
        val similarityEngineInfo = ProducerBasedUserAdGraphSimilarityEngine
          .toSimilarityEngineInfo(tweetWithScore.score)
        TweetWithCandidateGenerationInfo(
          tweetWithScore.tweetId,
          CandidateGenerationInfo(
            sourceInfo,
            similarityEngineInfo,
            Seq(similarityEngineInfo)
          ))
      })
  }

  private[candidate_generation] def getTweetBasedUserAdGraphCandidates(
    sourceInfo: Option[SourceInfo],
    params: configapi.Params
  ) = {

    val query = TweetBasedUserAdGraphSimilarityEngine.fromParams(
      sourceInfo.get.internalId,
      params
    )
    tweetBasedUserAdGraphSimilarityEngine
      .getCandidates(query).map(_.getOrElse(Seq.empty)).map(_.map { tweetWithScore =>
        val similarityEngineInfo = TweetBasedUserAdGraphSimilarityEngine
          .toSimilarityEngineInfo(tweetWithScore.score)
        TweetWithCandidateGenerationInfo(
          tweetWithScore.tweetId,
          CandidateGenerationInfo(
            sourceInfo,
            similarityEngineInfo,
            Seq(similarityEngineInfo)
          ))
      })
  }

  private[candidate_generation] def getRealGraphConsumersBasedUserAdGraphCandidates(
    realGraphSeeds: Map[UserId, Double],
    params: configapi.Params
  ) = {

    val query = ConsumersBasedUserAdGraphSimilarityEngine
      .fromParams(realGraphSeeds, params)

    // The internalId is a placeholder value. We do not plan to store the full seedUserId set.
    val sourceInfo = SourceInfo(
      sourceType = SourceType.RealGraphIn,
      internalId = InternalId.UserId(0L),
      sourceEventTime = None
    )
    consumersBasedUserAdGraphSimilarityEngine
      .getCandidates(query).map(_.getOrElse(Seq.empty)).map(_.map { tweetWithScore =>
        val similarityEngineInfo = ConsumersBasedUserAdGraphSimilarityEngine
          .toSimilarityEngineInfo(tweetWithScore.score)
        TweetWithCandidateGenerationInfo(
          tweetWithScore.tweetId,
          CandidateGenerationInfo(
            Some(sourceInfo),
            similarityEngineInfo,
            Seq.empty // Atomic Similarity Engine. Hence it has no contributing SEs
          )
        )
      })
  }

  private[candidate_generation] def getTwHINAdsCandidates(
    similarityEngine: HnswANNSimilarityEngine,
    similarityEngineType: SimilarityEngineType,
    requestUserId: UserId,
    sourceInfo: Option[SourceInfo], // if none, then it's consumer-based similarity engine
    model: String
  ): Future[Seq[TweetWithCandidateGenerationInfo]] = {
    val internalId =
      if (sourceInfo.nonEmpty) sourceInfo.get.internalId else InternalId.UserId(requestUserId)
    similarityEngine
      .getCandidates(buildHnswANNQuery(internalId, model)).map(_.getOrElse(Seq.empty)).map(_.map {
        tweetWithScore =>
          val similarityEngineInfo = SimilarityEngineInfo(
            similarityEngineType = similarityEngineType,
            modelId = Some(model),
            score = Some(tweetWithScore.score))
          TweetWithCandidateGenerationInfo(
            tweetWithScore.tweetId,
            CandidateGenerationInfo(
              None,
              similarityEngineInfo,
              Seq(similarityEngineInfo)
            ))
      })
  }

  private[candidate_generation] def getConsumerBasedWalsCandidates(
    sourceSignals: Set[SourceInfo],
    params: configapi.Params
  ): Future[Seq[TweetWithCandidateGenerationInfo]] = {
    // Fetch source signals and filter them based on age.
    val signals = FilterUtil.tweetSourceAgeFilter(
      getConsumerBasedWalsSourceInfo(sourceSignals).toSeq,
      params(ConsumerBasedWalsParams.MaxTweetSignalAgeHoursParam))

    val candidatesOptFut = consumerBasedWalsSimilarityEngine.getCandidates(
      ConsumerBasedWalsSimilarityEngine.fromParams(signals, params)
    )
    val tweetsWithCandidateGenerationInfoOptFut = candidatesOptFut.map {
      _.map { tweetsWithScores =>
        val sortedCandidates = tweetsWithScores.sortBy(-_.score)
        val filteredCandidates =
          FilterUtil.tweetAgeFilter(sortedCandidates, params(GlobalParams.MaxTweetAgeHoursParam))
        consumerBasedWalsSimilarityEngine.getScopedStats
          .stat("filteredCandidates_size").add(filteredCandidates.size)

        val tweetsWithCandidateGenerationInfo = filteredCandidates.map { tweetWithScore =>
          {
            val similarityEngineInfo =
              ConsumerBasedWalsSimilarityEngine.toSimilarityEngineInfo(tweetWithScore.score)
            TweetWithCandidateGenerationInfo(
              tweetWithScore.tweetId,
              CandidateGenerationInfo(
                None,
                similarityEngineInfo,
                Seq.empty // Atomic Similarity Engine. Hence it has no contributing SEs
              )
            )
          }
        }
        val maxCandidateNum = params(GlobalParams.MaxCandidateNumPerSourceKeyParam)
        tweetsWithCandidateGenerationInfo.take(maxCandidateNum)
      }
    }
    for {
      tweetsWithCandidateGenerationInfoOpt <- tweetsWithCandidateGenerationInfoOptFut
    } yield tweetsWithCandidateGenerationInfoOpt.toSeq.flatten
  }
}

object AdsCandidateSourcesRouter {
  def getSimClustersANNEmbeddingType(
    sourceInfo: SourceInfo
  ): EmbeddingType = {
    sourceInfo.sourceType match {
      case SourceType.TweetFavorite | SourceType.Retweet | SourceType.OriginalTweet |
          SourceType.Reply | SourceType.TweetShare | SourceType.NotificationClick |
          SourceType.GoodTweetClick | SourceType.VideoTweetQualityView |
          SourceType.VideoTweetPlayback50 =>
        EmbeddingType.LogFavLongestL2EmbeddingTweet
      case SourceType.UserFollow | SourceType.UserRepeatedProfileVisit | SourceType.RealGraphOon |
          SourceType.FollowRecommendation | SourceType.UserTrafficAttributionProfileVisit |
          SourceType.GoodProfileClick | SourceType.TwiceUserId =>
        EmbeddingType.FavBasedProducer
      case _ => throw new IllegalArgumentException("sourceInfo.sourceType not supported")
    }
  }

  def buildHnswANNQuery(internalId: InternalId, modelId: String): HnswANNEngineQuery = {
    HnswANNEngineQuery(
      sourceId = internalId,
      modelId = modelId,
      params = Params.Empty
    )
  }

  def getConsumerBasedWalsSourceInfo(
    sourceSignals: Set[SourceInfo]
  ): Set[SourceInfo] = {
    val AllowedSourceTypesForConsumerBasedWalsSE = Set(
      SourceType.TweetFavorite.value,
      SourceType.Retweet.value,
      SourceType.TweetDontLike.value, //currently no-op
      SourceType.TweetReport.value, //currently no-op
      SourceType.AccountMute.value, //currently no-op
      SourceType.AccountBlock.value //currently no-op
    )
    sourceSignals.collect {
      case sourceInfo
          if AllowedSourceTypesForConsumerBasedWalsSE.contains(sourceInfo.sourceType.value) =>
        sourceInfo
    }
  }
}
package com.twitter.cr_mixer.candidate_generation

import com.twitter.contentrecommender.thriftscala.TweetInfo
import com.twitter.cr_mixer.filter.PreRankFilterRunner
import com.twitter.cr_mixer.logging.RelatedTweetScribeLogger
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.RelatedTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.similarity_engine.ProducerBasedUnifiedSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.TweetBasedUnifiedSimilarityEngine
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

@Singleton
class RelatedTweetCandidateGenerator @Inject() (
  @Named(ModuleNames.TweetBasedUnifiedSimilarityEngine) tweetBasedUnifiedSimilarityEngine: StandardSimilarityEngine[
    TweetBasedUnifiedSimilarityEngine.Query,
    TweetWithCandidateGenerationInfo
  ],
  @Named(ModuleNames.ProducerBasedUnifiedSimilarityEngine) producerBasedUnifiedSimilarityEngine: StandardSimilarityEngine[
    ProducerBasedUnifiedSimilarityEngine.Query,
    TweetWithCandidateGenerationInfo
  ],
  preRankFilterRunner: PreRankFilterRunner,
  relatedTweetScribeLogger: RelatedTweetScribeLogger,
  tweetInfoStore: ReadableStore[TweetId, TweetInfo],
  globalStats: StatsReceiver) {

  private val stats: StatsReceiver = globalStats.scope(this.getClass.getCanonicalName)
  private val fetchCandidatesStats = stats.scope("fetchCandidates")
  private val preRankFilterStats = stats.scope("preRankFilter")

  def get(
    query: RelatedTweetCandidateGeneratorQuery
  ): Future[Seq[InitialCandidate]] = {

    val allStats = stats.scope("all")
    val perProductStats = stats.scope("perProduct", query.product.toString)
    StatsUtil.trackItemsStats(allStats) {
      StatsUtil.trackItemsStats(perProductStats) {
        for {
          initialCandidates <- StatsUtil.trackBlockStats(fetchCandidatesStats) {
            fetchCandidates(query)
          }
          filteredCandidates <- StatsUtil.trackBlockStats(preRankFilterStats) {
            preRankFilter(query, initialCandidates)
          }
        } yield {
          filteredCandidates.headOption
            .getOrElse(
              throw new UnsupportedOperationException(
                "RelatedTweetCandidateGenerator results invalid")
            ).take(query.maxNumResults)
        }
      }
    }
  }

  def fetchCandidates(
    query: RelatedTweetCandidateGeneratorQuery
  ): Future[Seq[Seq[InitialCandidate]]] = {
    relatedTweetScribeLogger.scribeInitialCandidates(
      query,
      query.internalId match {
        case InternalId.TweetId(_) =>
          getCandidatesFromSimilarityEngine(
            query,
            TweetBasedUnifiedSimilarityEngine.fromParamsForRelatedTweet,
            tweetBasedUnifiedSimilarityEngine.getCandidates)
        case InternalId.UserId(_) =>
          getCandidatesFromSimilarityEngine(
            query,
            ProducerBasedUnifiedSimilarityEngine.fromParamsForRelatedTweet,
            producerBasedUnifiedSimilarityEngine.getCandidates)
        case _ =>
          throw new UnsupportedOperationException(
            "RelatedTweetCandidateGenerator gets invalid InternalId")
      }
    )
  }

  /***
   * fetch Candidates from TweetBased/ProducerBased Unified Similarity Engine,
   * and apply VF filter based on TweetInfoStore
   * To align with the downstream processing (filter, rank), we tend to return a Seq[Seq[InitialCandidate]]
   * instead of a Seq[Candidate] even though we only have a Seq in it.
   */
  private def getCandidatesFromSimilarityEngine[QueryType](
    query: RelatedTweetCandidateGeneratorQuery,
    fromParamsForRelatedTweet: (InternalId, configapi.Params) => QueryType,
    getFunc: QueryType => Future[Option[Seq[TweetWithCandidateGenerationInfo]]]
  ): Future[Seq[Seq[InitialCandidate]]] = {

    /***
     * We wrap the query to be a Seq of queries for the Sim Engine to ensure evolvability of candidate generation
     * and as a result, it will return Seq[Seq[InitialCandidate]]
     */
    val engineQueries =
      Seq(fromParamsForRelatedTweet(query.internalId, query.params))

    Future
      .collect {
        engineQueries.map { query =>
          for {
            candidates <- getFunc(query)
            prefilterCandidates <- convertToInitialCandidates(
              candidates.toSeq.flatten
            )
          } yield prefilterCandidates
        }
      }
  }

  private def preRankFilter(
    query: RelatedTweetCandidateGeneratorQuery,
    candidates: Seq[Seq[InitialCandidate]]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    relatedTweetScribeLogger.scribePreRankFilterCandidates(
      query,
      preRankFilterRunner
        .runSequentialFilters(query, candidates))
  }

  private[candidate_generation] def convertToInitialCandidates(
    candidates: Seq[TweetWithCandidateGenerationInfo],
  ): Future[Seq[InitialCandidate]] = {
    val tweetIds = candidates.map(_.tweetId).toSet
    Future.collect(tweetInfoStore.multiGet(tweetIds)).map { tweetInfos =>
      /***
       * If tweetInfo does not exist, we will filter out this tweet candidate.
       * This tweetInfo filter also acts as the VF filter
       */
      candidates.collect {
        case candidate if tweetInfos.getOrElse(candidate.tweetId, None).isDefined =>
          val tweetInfo = tweetInfos(candidate.tweetId)
            .getOrElse(throw new IllegalStateException("Check previous line's condition"))

          InitialCandidate(
            tweetId = candidate.tweetId,
            tweetInfo = tweetInfo,
            candidate.candidateGenerationInfo
          )
      }
    }
  }
}
package com.twitter.cr_mixer.candidate_generation

import com.twitter.contentrecommender.thriftscala.TweetInfo
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.FrsTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TweetWithAuthor
import com.twitter.cr_mixer.param.FrsParams
import com.twitter.cr_mixer.similarity_engine.EarlybirdSimilarityEngineRouter
import com.twitter.cr_mixer.source_signal.FrsStore
import com.twitter.cr_mixer.source_signal.FrsStore.FrsQueryResult
import com.twitter.cr_mixer.thriftscala.FrsTweet
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.hermit.constants.AlgorithmFeedbackTokens
import com.twitter.hermit.constants.AlgorithmFeedbackTokens.AlgorithmToFeedbackTokenMap
import com.twitter.hermit.model.Algorithm
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi.Params
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

/**
 * TweetCandidateGenerator based on FRS seed users. For now this candidate generator fetches seed
 * users from FRS, and retrieves the seed users' past tweets from Earlybird with Earlybird light
 * ranking models.
 */
@Singleton
class FrsTweetCandidateGenerator @Inject() (
  @Named(ModuleNames.FrsStore) frsStore: ReadableStore[FrsStore.Query, Seq[FrsQueryResult]],
  frsBasedSimilarityEngine: EarlybirdSimilarityEngineRouter,
  tweetInfoStore: ReadableStore[TweetId, TweetInfo],
  timeoutConfig: TimeoutConfig,
  globalStats: StatsReceiver) {
  import FrsTweetCandidateGenerator._

  private val timer = DefaultTimer
  private val stats: StatsReceiver = globalStats.scope(this.getClass.getCanonicalName)
  private val fetchSeedsStats = stats.scope("fetchSeeds")
  private val fetchCandidatesStats = stats.scope("fetchCandidates")
  private val filterCandidatesStats = stats.scope("filterCandidates")
  private val hydrateCandidatesStats = stats.scope("hydrateCandidates")
  private val getCandidatesStats = stats.scope("getCandidates")

  /**
   * The function retrieves the candidate for the given user as follows:
   * 1. Seed user fetch from FRS.
   * 2. Candidate fetch from Earlybird.
   * 3. Filtering.
   * 4. Candidate hydration.
   * 5. Truncation.
   */
  def get(
    frsTweetCandidateGeneratorQuery: FrsTweetCandidateGeneratorQuery
  ): Future[Seq[FrsTweet]] = {
    val userId = frsTweetCandidateGeneratorQuery.userId
    val product = frsTweetCandidateGeneratorQuery.product
    val allStats = stats.scope("all")
    val perProductStats = stats.scope("perProduct", product.name)
    StatsUtil.trackItemsStats(allStats) {
      StatsUtil.trackItemsStats(perProductStats) {
        val result = for {
          seedAuthorWithScores <- StatsUtil.trackOptionItemMapStats(fetchSeedsStats) {
            fetchSeeds(
              userId,
              frsTweetCandidateGeneratorQuery.impressedUserList,
              frsTweetCandidateGeneratorQuery.languageCodeOpt,
              frsTweetCandidateGeneratorQuery.countryCodeOpt,
              frsTweetCandidateGeneratorQuery.params,
            )
          }
          tweetCandidates <- StatsUtil.trackOptionItemsStats(fetchCandidatesStats) {
            fetchCandidates(
              userId,
              seedAuthorWithScores.map(_.keys.toSeq).getOrElse(Seq.empty),
              frsTweetCandidateGeneratorQuery.impressedTweetList,
              seedAuthorWithScores.map(_.mapValues(_.score)).getOrElse(Map.empty),
              frsTweetCandidateGeneratorQuery.params
            )
          }
          filteredTweetCandidates <- StatsUtil.trackOptionItemsStats(filterCandidatesStats) {
            filterCandidates(
              tweetCandidates,
              frsTweetCandidateGeneratorQuery.params
            )
          }
          hydratedTweetCandidates <- StatsUtil.trackOptionItemsStats(hydrateCandidatesStats) {
            hydrateCandidates(
              seedAuthorWithScores,
              filteredTweetCandidates
            )
          }
        } yield {
          hydratedTweetCandidates
            .map(_.take(frsTweetCandidateGeneratorQuery.maxNumResults)).getOrElse(Seq.empty)
        }
        result.raiseWithin(timeoutConfig.frsBasedTweetEndpointTimeout)(timer)
      }
    }
  }

  /**
   * Fetch recommended seed users from FRS
   */
  private def fetchSeeds(
    userId: UserId,
    userDenyList: Set[UserId],
    languageCodeOpt: Option[String],
    countryCodeOpt: Option[String],
    params: Params
  ): Future[Option[Map[UserId, FrsQueryResult]]] = {
    frsStore
      .get(
        FrsStore.Query(
          userId,
          params(FrsParams.FrsBasedCandidateGenerationMaxSeedsNumParam),
          params(FrsParams.FrsBasedCandidateGenerationDisplayLocationParam).displayLocation,
          userDenyList.toSeq,
          languageCodeOpt,
          countryCodeOpt
        )).map {
        _.map { seedAuthors =>
          seedAuthors.map(user => user.userId -> user).toMap
        }
      }
  }

  /**
   * Fetch tweet candidates from Earlybird
   */
  private def fetchCandidates(
    searcherUserId: UserId,
    seedAuthors: Seq[UserId],
    impressedTweetList: Set[TweetId],
    frsUserToScores: Map[UserId, Double],
    params: Params
  ): Future[Option[Seq[TweetWithAuthor]]] = {
    if (seedAuthors.nonEmpty) {
      // call earlybird
      val query = EarlybirdSimilarityEngineRouter.queryFromParams(
        Some(searcherUserId),
        seedAuthors,
        impressedTweetList,
        frsUserToScoresForScoreAdjustment = Some(frsUserToScores),
        params
      )
      frsBasedSimilarityEngine.get(query)
    } else Future.None
  }

  /**
   * Filter candidates that do not pass visibility filter policy
   */
  private def filterCandidates(
    candidates: Option[Seq[TweetWithAuthor]],
    params: Params
  ): Future[Option[Seq[TweetWithAuthor]]] = {
    val tweetIds = candidates.map(_.map(_.tweetId).toSet).getOrElse(Set.empty)
    if (params(FrsParams.FrsBasedCandidateGenerationEnableVisibilityFilteringParam))
      Future
        .collect(tweetInfoStore.multiGet(tweetIds)).map { tweetInfos =>
          candidates.map {
            // If tweetInfo does not exist, we will filter out this tweet candidate.
            _.filter(candidate => tweetInfos.getOrElse(candidate.tweetId, None).isDefined)
          }
        }
    else {
      Future.value(candidates)
    }
  }

  /**
   * Hydrate the candidates with the FRS candidate sources and scores
   */
  private def hydrateCandidates(
    frsAuthorWithScores: Option[Map[UserId, FrsQueryResult]],
    candidates: Option[Seq[TweetWithAuthor]]
  ): Future[Option[Seq[FrsTweet]]] = {
    Future.value {
      candidates.map {
        _.map { tweetWithAuthor =>
          val frsQueryResult = frsAuthorWithScores.flatMap(_.get(tweetWithAuthor.authorId))
          FrsTweet(
            tweetId = tweetWithAuthor.tweetId,
            authorId = tweetWithAuthor.authorId,
            frsPrimarySource = frsQueryResult.flatMap(_.primarySource),
            frsAuthorScore = frsQueryResult.map(_.score),
            frsCandidateSourceScores = frsQueryResult.flatMap { result =>
              result.sourceWithScores.map {
                _.collect {
                  // see TokenStrToAlgorithmMap @ https://sourcegraph.twitter.biz/git.twitter.biz/source/-/blob/hermit/hermit-core/src/main/scala/com/twitter/hermit/constants/AlgorithmFeedbackTokens.scala
                  // see Algorithm @ https://sourcegraph.twitter.biz/git.twitter.biz/source/-/blob/hermit/hermit-core/src/main/scala/com/twitter/hermit/model/Algorithm.scala
                  case (candidateSourceAlgoStr, score)
                      if AlgorithmFeedbackTokens.TokenStrToAlgorithmMap.contains(
                        candidateSourceAlgoStr) =>
                    AlgorithmToFeedbackTokenMap.getOrElse(
                      AlgorithmFeedbackTokens.TokenStrToAlgorithmMap
                        .getOrElse(candidateSourceAlgoStr, DefaultAlgo),
                      DefaultAlgoToken) -> score
                }
              }
            }
          )
        }
      }
    }
  }

}

object FrsTweetCandidateGenerator {
  val DefaultAlgo: Algorithm.Value = Algorithm.Other
  // 9999 is the token for Algorithm.Other
  val DefaultAlgoToken: Int = AlgorithmToFeedbackTokenMap.getOrElse(DefaultAlgo, 9999)
}
package com.twitter.cr_mixer.candidate_generation

import com.twitter.cr_mixer.candidate_generation.CustomizedRetrievalCandidateGeneration.Query
import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.CustomizedRetrievalBasedCandidateGenerationParams._
import com.twitter.cr_mixer.param.CustomizedRetrievalBasedTwhinParams._
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.similarity_engine.DiffusionBasedSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.LookupEngineQuery
import com.twitter.cr_mixer.similarity_engine.LookupSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.TwhinCollabFilterSimilarityEngine
import com.twitter.cr_mixer.util.InterleaveUtil
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.base.CandidateSource
import com.twitter.frigate.common.base.Stats
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.timelines.configapi
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton
import scala.collection.mutable.ArrayBuffer

/**
 * A candidate generator that fetches similar tweets from multiple customized retrieval based candidate sources
 *
 * Different from [[TweetBasedCandidateGeneration]], this store returns candidates from different
 * similarity engines without blending. In other words, this class shall not be thought of as a
 * Unified Similarity Engine. It is a CG that calls multiple singular Similarity Engines.
 */
@Singleton
case class CustomizedRetrievalCandidateGeneration @Inject() (
  @Named(ModuleNames.TwhinCollabFilterSimilarityEngine)
  twhinCollabFilterSimilarityEngine: LookupSimilarityEngine[
    TwhinCollabFilterSimilarityEngine.Query,
    TweetWithScore
  ],
  @Named(ModuleNames.DiffusionBasedSimilarityEngine)
  diffusionBasedSimilarityEngine: LookupSimilarityEngine[
    DiffusionBasedSimilarityEngine.Query,
    TweetWithScore
  ],
  statsReceiver: StatsReceiver)
    extends CandidateSource[
      Query,
      Seq[TweetWithCandidateGenerationInfo]
    ] {

  override def name: String = this.getClass.getSimpleName

  private val stats = statsReceiver.scope(name)
  private val fetchCandidatesStat = stats.scope("fetchCandidates")

  /**
   * For each Similarity Engine Model, return a list of tweet candidates
   */
  override def get(
    query: Query
  ): Future[Option[Seq[Seq[TweetWithCandidateGenerationInfo]]]] = {
    query.internalId match {
      case InternalId.UserId(_) =>
        Stats.trackOption(fetchCandidatesStat) {
          val twhinCollabFilterForFollowCandidatesFut = if (query.enableTwhinCollabFilter) {
            twhinCollabFilterSimilarityEngine.getCandidates(query.twhinCollabFilterFollowQuery)
          } else Future.None

          val twhinCollabFilterForEngagementCandidatesFut =
            if (query.enableTwhinCollabFilter) {
              twhinCollabFilterSimilarityEngine.getCandidates(
                query.twhinCollabFilterEngagementQuery)
            } else Future.None

          val twhinMultiClusterForFollowCandidatesFut = if (query.enableTwhinMultiCluster) {
            twhinCollabFilterSimilarityEngine.getCandidates(query.twhinMultiClusterFollowQuery)
          } else Future.None

          val twhinMultiClusterForEngagementCandidatesFut =
            if (query.enableTwhinMultiCluster) {
              twhinCollabFilterSimilarityEngine.getCandidates(
                query.twhinMultiClusterEngagementQuery)
            } else Future.None

          val diffusionBasedSimilarityEngineCandidatesFut = if (query.enableRetweetBasedDiffusion) {
            diffusionBasedSimilarityEngine.getCandidates(query.diffusionBasedSimilarityEngineQuery)
          } else Future.None

          Future
            .join(
              twhinCollabFilterForFollowCandidatesFut,
              twhinCollabFilterForEngagementCandidatesFut,
              twhinMultiClusterForFollowCandidatesFut,
              twhinMultiClusterForEngagementCandidatesFut,
              diffusionBasedSimilarityEngineCandidatesFut
            ).map {
              case (
                    twhinCollabFilterForFollowCandidates,
                    twhinCollabFilterForEngagementCandidates,
                    twhinMultiClusterForFollowCandidates,
                    twhinMultiClusterForEngagementCandidates,
                    diffusionBasedSimilarityEngineCandidates) =>
                val maxCandidateNumPerSourceKey = 200
                val twhinCollabFilterForFollowWithCGInfo =
                  getTwhinCollabCandidatesWithCGInfo(
                    twhinCollabFilterForFollowCandidates,
                    maxCandidateNumPerSourceKey,
                    query.twhinCollabFilterFollowQuery,
                  )
                val twhinCollabFilterForEngagementWithCGInfo =
                  getTwhinCollabCandidatesWithCGInfo(
                    twhinCollabFilterForEngagementCandidates,
                    maxCandidateNumPerSourceKey,
                    query.twhinCollabFilterEngagementQuery,
                  )
                val twhinMultiClusterForFollowWithCGInfo =
                  getTwhinCollabCandidatesWithCGInfo(
                    twhinMultiClusterForFollowCandidates,
                    maxCandidateNumPerSourceKey,
                    query.twhinMultiClusterFollowQuery,
                  )
                val twhinMultiClusterForEngagementWithCGInfo =
                  getTwhinCollabCandidatesWithCGInfo(
                    twhinMultiClusterForEngagementCandidates,
                    maxCandidateNumPerSourceKey,
                    query.twhinMultiClusterEngagementQuery,
                  )
                val retweetBasedDiffusionWithCGInfo =
                  getDiffusionBasedCandidatesWithCGInfo(
                    diffusionBasedSimilarityEngineCandidates,
                    maxCandidateNumPerSourceKey,
                    query.diffusionBasedSimilarityEngineQuery,
                  )

                val twhinCollabCandidateSourcesToBeInterleaved =
                  ArrayBuffer[Seq[TweetWithCandidateGenerationInfo]](
                    twhinCollabFilterForFollowWithCGInfo,
                    twhinCollabFilterForEngagementWithCGInfo,
                  )

                val twhinMultiClusterCandidateSourcesToBeInterleaved =
                  ArrayBuffer[Seq[TweetWithCandidateGenerationInfo]](
                    twhinMultiClusterForFollowWithCGInfo,
                    twhinMultiClusterForEngagementWithCGInfo,
                  )

                val interleavedTwhinCollabCandidates =
                  InterleaveUtil.interleave(twhinCollabCandidateSourcesToBeInterleaved)

                val interleavedTwhinMultiClusterCandidates =
                  InterleaveUtil.interleave(twhinMultiClusterCandidateSourcesToBeInterleaved)

                val twhinCollabFilterResults =
                  if (interleavedTwhinCollabCandidates.nonEmpty) {
                    Some(interleavedTwhinCollabCandidates.take(maxCandidateNumPerSourceKey))
                  } else None

                val twhinMultiClusterResults =
                  if (interleavedTwhinMultiClusterCandidates.nonEmpty) {
                    Some(interleavedTwhinMultiClusterCandidates.take(maxCandidateNumPerSourceKey))
                  } else None

                val diffusionResults =
                  if (retweetBasedDiffusionWithCGInfo.nonEmpty) {
                    Some(retweetBasedDiffusionWithCGInfo.take(maxCandidateNumPerSourceKey))
                  } else None

                Some(
                  Seq(
                    twhinCollabFilterResults,
                    twhinMultiClusterResults,
                    diffusionResults
                  ).flatten)
            }
        }
      case _ =>
        throw new IllegalArgumentException("sourceId_is_not_userId_cnt")
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

  /**
   * AgeFilters tweetCandidates with stats
   * Only age filter logic is effective here (through tweetAgeFilter). This function acts mostly for metric logging.
   */
  private def ageFilterWithStats(
    offlineInterestedInCandidates: Seq[TweetWithScore],
    maxTweetAgeHours: Duration,
    scopedStatsReceiver: StatsReceiver
  ): Seq[TweetWithScore] = {
    scopedStatsReceiver.stat("size").add(offlineInterestedInCandidates.size)
    val candidates = offlineInterestedInCandidates.map { candidate =>
      TweetWithScore(candidate.tweetId, candidate.score)
    }
    val filteredCandidates = tweetAgeFilter(candidates, maxTweetAgeHours)
    scopedStatsReceiver.stat(f"filtered_size").add(filteredCandidates.size)
    if (filteredCandidates.isEmpty) scopedStatsReceiver.counter(f"empty").incr()

    filteredCandidates
  }

  private def getTwhinCollabCandidatesWithCGInfo(
    tweetCandidates: Option[Seq[TweetWithScore]],
    maxCandidateNumPerSourceKey: Int,
    twhinCollabFilterQuery: LookupEngineQuery[
      TwhinCollabFilterSimilarityEngine.Query
    ],
  ): Seq[TweetWithCandidateGenerationInfo] = {
    val twhinTweets = tweetCandidates match {
      case Some(tweetsWithScores) =>
        tweetsWithScores.map { tweetWithScore =>
          TweetWithCandidateGenerationInfo(
            tweetWithScore.tweetId,
            CandidateGenerationInfo(
              None,
              TwhinCollabFilterSimilarityEngine
                .toSimilarityEngineInfo(twhinCollabFilterQuery, tweetWithScore.score),
              Seq.empty
            )
          )
        }
      case _ => Seq.empty
    }
    twhinTweets.take(maxCandidateNumPerSourceKey)
  }

  private def getDiffusionBasedCandidatesWithCGInfo(
    tweetCandidates: Option[Seq[TweetWithScore]],
    maxCandidateNumPerSourceKey: Int,
    diffusionBasedSimilarityEngineQuery: LookupEngineQuery[
      DiffusionBasedSimilarityEngine.Query
    ],
  ): Seq[TweetWithCandidateGenerationInfo] = {
    val diffusionTweets = tweetCandidates match {
      case Some(tweetsWithScores) =>
        tweetsWithScores.map { tweetWithScore =>
          TweetWithCandidateGenerationInfo(
            tweetWithScore.tweetId,
            CandidateGenerationInfo(
              None,
              DiffusionBasedSimilarityEngine
                .toSimilarityEngineInfo(diffusionBasedSimilarityEngineQuery, tweetWithScore.score),
              Seq.empty
            )
          )
        }
      case _ => Seq.empty
    }
    diffusionTweets.take(maxCandidateNumPerSourceKey)
  }
}

object CustomizedRetrievalCandidateGeneration {

  case class Query(
    internalId: InternalId,
    maxCandidateNumPerSourceKey: Int,
    maxTweetAgeHours: Duration,
    // twhinCollabFilter
    enableTwhinCollabFilter: Boolean,
    twhinCollabFilterFollowQuery: LookupEngineQuery[
      TwhinCollabFilterSimilarityEngine.Query
    ],
    twhinCollabFilterEngagementQuery: LookupEngineQuery[
      TwhinCollabFilterSimilarityEngine.Query
    ],
    // twhinMultiCluster
    enableTwhinMultiCluster: Boolean,
    twhinMultiClusterFollowQuery: LookupEngineQuery[
      TwhinCollabFilterSimilarityEngine.Query
    ],
    twhinMultiClusterEngagementQuery: LookupEngineQuery[
      TwhinCollabFilterSimilarityEngine.Query
    ],
    enableRetweetBasedDiffusion: Boolean,
    diffusionBasedSimilarityEngineQuery: LookupEngineQuery[
      DiffusionBasedSimilarityEngine.Query
    ],
  )

  def fromParams(
    internalId: InternalId,
    params: configapi.Params
  ): Query = {
    val twhinCollabFilterFollowQuery =
      TwhinCollabFilterSimilarityEngine.fromParams(
        internalId,
        params(CustomizedRetrievalBasedTwhinCollabFilterFollowSource),
        params)

    val twhinCollabFilterEngagementQuery =
      TwhinCollabFilterSimilarityEngine.fromParams(
        internalId,
        params(CustomizedRetrievalBasedTwhinCollabFilterEngagementSource),
        params)

    val twhinMultiClusterFollowQuery =
      TwhinCollabFilterSimilarityEngine.fromParams(
        internalId,
        params(CustomizedRetrievalBasedTwhinMultiClusterFollowSource),
        params)

    val twhinMultiClusterEngagementQuery =
      TwhinCollabFilterSimilarityEngine.fromParams(
        internalId,
        params(CustomizedRetrievalBasedTwhinMultiClusterEngagementSource),
        params)

    val diffusionBasedSimilarityEngineQuery =
      DiffusionBasedSimilarityEngine.fromParams(
        internalId,
        params(CustomizedRetrievalBasedRetweetDiffusionSource),
        params)

    Query(
      internalId = internalId,
      maxCandidateNumPerSourceKey = params(GlobalParams.MaxCandidateNumPerSourceKeyParam),
      maxTweetAgeHours = params(GlobalParams.MaxTweetAgeHoursParam),
      // twhinCollabFilter
      enableTwhinCollabFilter = params(EnableTwhinCollabFilterClusterParam),
      twhinCollabFilterFollowQuery = twhinCollabFilterFollowQuery,
      twhinCollabFilterEngagementQuery = twhinCollabFilterEngagementQuery,
      enableTwhinMultiCluster = params(EnableTwhinMultiClusterParam),
      twhinMultiClusterFollowQuery = twhinMultiClusterFollowQuery,
      twhinMultiClusterEngagementQuery = twhinMultiClusterEngagementQuery,
      enableRetweetBasedDiffusion = params(EnableRetweetBasedDiffusionParam),
      diffusionBasedSimilarityEngineQuery = diffusionBasedSimilarityEngineQuery
    )
  }
}
package com.twitter.cr_mixer.featureswitch

import com.twitter.finagle.Filter
import javax.inject.Inject
import javax.inject.Singleton
import scala.collection.concurrent.TrieMap
import com.twitter.abdecider.Bucket
import com.twitter.finagle.Service

@Singleton
class SetImpressedBucketsLocalContextFilter @Inject() () extends Filter.TypeAgnostic {
  override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
    (request: Req, service: Service[Req, Rep]) => {

      val concurrentTrieMap = TrieMap
        .empty[Bucket, Boolean] // Trie map has no locks and O(1) inserts
      CrMixerImpressedBuckets.localImpressedBucketsMap.let(concurrentTrieMap) {
        service(request)
      }
    }

}
