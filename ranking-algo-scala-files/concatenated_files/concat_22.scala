package com.twitter.cr_mixer.param

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSEnumParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param
import com.twitter.usersignalservice.thriftscala.SignalType

object GoodTweetClickParams {

  object ClickMinDwellTimeParam extends Enumeration {
    protected case class SignalTypeValue(signalType: SignalType) extends super.Val
    import scala.language.implicitConversions
    implicit def valueToSignalTypeValue(x: Value): SignalTypeValue =
      x.asInstanceOf[SignalTypeValue]

    val TotalDwellTime2s = SignalTypeValue(SignalType.GoodTweetClick)
    val TotalDwellTime5s = SignalTypeValue(SignalType.GoodTweetClick5s)
    val TotalDwellTime10s = SignalTypeValue(SignalType.GoodTweetClick10s)
    val TotalDwellTime30s = SignalTypeValue(SignalType.GoodTweetClick30s)

  }

  object EnableSourceParam
      extends FSParam[Boolean](
        name = "signal_good_tweet_clicks_enable_source",
        default = false
      )

  object ClickMinDwellTimeType
      extends FSEnumParam[ClickMinDwellTimeParam.type](
        name = "signal_good_tweet_clicks_min_dwelltime_type_id",
        default = ClickMinDwellTimeParam.TotalDwellTime2s,
        enum = ClickMinDwellTimeParam
      )

  object MaxSignalNumParam
      extends FSBoundedParam[Int](
        name = "signal_good_tweet_clicks_max_signal_num",
        default = 15,
        min = 0,
        max = 15
      )

  val AllParams: Seq[Param[_] with FSName] =
    Seq(EnableSourceParam, ClickMinDwellTimeType, MaxSignalNumParam)

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam
    )

    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
      ClickMinDwellTimeType
    )

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MaxSignalNumParam
    )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(enumOverrides: _*)
      .set(intOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.model

import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.util.Time

/***
 * Tweet-level attributes. Represents the source used in candidate generation
 * Due to legacy reason, SourceType used to represent both SourceType and SimilarityEngineType
 * Moving forward, SourceType will be used for SourceType ONLY. eg., TweetFavorite, UserFollow, TwiceUserId
 * At the same time, We create a new SimilarityEngineType to separate them. eg., SimClustersANN
 *
 * Currently, one special case is that we have TwiceUserId as a source, which is not necessarily a "signal"
 * @param sourceType, e.g., SourceType.TweetFavorite, SourceType.UserFollow, SourceType.TwiceUserId
 * @param internalId, e.g., UserId(0L), TweetId(0L)
 */
case class SourceInfo(
  sourceType: SourceType,
  internalId: InternalId,
  sourceEventTime: Option[Time])

/***
 * Tweet-level attributes. Represents the source User Graph used in candidate generation
 * It is an intermediate product, and will not be stored, unlike SourceInfo.
 * Essentially, CrMixer queries a graph, and the graph returns a list of users to be used as sources.
 * For instance, RealGraph, EarlyBird, FRS, Stp, etc. The underlying similarity engines such as
 * UTG or UTEG will leverage these sources to build candidates.
 *
 * We extended the definition of SourceType to cover both "Source Signal" and "Source Graph"
 * See [CrMixer] Graph Based Source Fetcher Abstraction Proposal:
 *
 * consider making both SourceInfo and GraphSourceInfo extends the same trait to
 * have a unified interface.
 */
case class GraphSourceInfo(
  sourceType: SourceType,
  seedWithScores: Map[UserId, Double])

/***
 * Tweet-level attributes. Represents the similarity engine (the algorithm) used for
 * candidate generation along with their metadata.
 * @param similarityEngineType, e.g., SimClustersANN, UserTweetGraph
 * @param modelId. e.g., UserTweetGraphConsumerEmbedding_ALL_20210708
 * @param score - a score generated by this sim engine
 */
case class SimilarityEngineInfo(
  similarityEngineType: SimilarityEngineType,
  modelId: Option[String], // ModelId can be a None. e.g., UTEG, UnifiedTweetBasedSE. etc
  score: Option[Double])

/****
 * Tweet-level attributes. A combination for both SourceInfo and SimilarityEngineInfo
 * SimilarityEngine is a composition, and it can be composed by many leaf Similarity Engines.
 * For instance, the TweetBasedUnified SE could be a composition of both UserTweetGraph SE, SimClustersANN SE.
 * Note that a SimilarityEngine (Composite) may call other SimilarityEngines (Atomic, Contributing)
 * to contribute to its final candidate list. We track these Contributing SEs in the contributingSimilarityEngines list
 *
 * @param sourceInfoOpt - this is optional as many consumerBased CG does not have a source
 * @param similarityEngineInfo - the similarity engine used in Candidate Generation (eg., TweetBasedUnifiedSE). It can be an atomic SE or an composite SE
 * @param contributingSimilarityEngines - only composite SE will have it (e.g., SANNN, UTG). Otherwise it is an empty Seq. All contributing SEs mst be atomic
 */
case class CandidateGenerationInfo(
  sourceInfoOpt: Option[SourceInfo],
  similarityEngineInfo: SimilarityEngineInfo,
  contributingSimilarityEngines: Seq[SimilarityEngineInfo])
package com.twitter.cr_mixer.param

import com.twitter.conversions.DurationOps.richDurationFromInt
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.DurationConversion
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.HasDurationConversion
import com.twitter.timelines.configapi.Param
import com.twitter.util.Duration

object ConsumerBasedWalsParams {

  object EnableSourceParam
      extends FSParam[Boolean](
        name = "consumer_based_wals_enable_source",
        default = false
      )

  object ModelNameParam
      extends FSParam[String](
        name = "consumer_based_wals_model_name",
        default = "model_0"
      )

  object WilyNsNameParam
      extends FSParam[String](
        name = "consumer_based_wals_wily_ns_name",
        default = ""
      )

  object ModelInputNameParam
      extends FSParam[String](
        name = "consumer_based_wals_model_input_name",
        default = "examples"
      )

  object ModelOutputNameParam
      extends FSParam[String](
        name = "consumer_based_wals_model_output_name",
        default = "all_tweet_ids"
      )

  object ModelSignatureNameParam
      extends FSParam[String](
        name = "consumer_based_wals_model_signature_name",
        default = "serving_default"
      )

  object MaxTweetSignalAgeHoursParam
      extends FSBoundedParam[Duration](
        name = "consumer_based_wals_max_tweet_signal_age_hours",
        default = 72.hours,
        min = 1.hours,
        max = 720.hours
      )
      with HasDurationConversion {

    override val durationConversion: DurationConversion = DurationConversion.FromHours
  }

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableSourceParam,
    ModelNameParam,
    ModelInputNameParam,
    ModelOutputNameParam,
    ModelSignatureNameParam,
    MaxTweetSignalAgeHoursParam,
    WilyNsNameParam,
  )

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam,
    )
    val stringOverrides = FeatureSwitchOverrideUtil.getStringFSOverrides(
      ModelNameParam,
      ModelInputNameParam,
      ModelOutputNameParam,
      ModelSignatureNameParam,
      WilyNsNameParam
    )

    val boundedDurationFSOverrides =
      FeatureSwitchOverrideUtil.getBoundedDurationFSOverrides(MaxTweetSignalAgeHoursParam)

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(stringOverrides: _*)
      .set(boundedDurationFSOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.CompositeConfig
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.Param

object CrMixerParamConfig {

  lazy val config: CompositeConfig = new CompositeConfig(
    configs = Seq(
      AdsParams.config,
      BlenderParams.config,
      BypassInterleaveAndRankParams.config,
      RankerParams.config,
      ConsumerBasedWalsParams.config,
      ConsumerEmbeddingBasedCandidateGenerationParams.config,
      ConsumerEmbeddingBasedTripParams.config,
      ConsumerEmbeddingBasedTwHINParams.config,
      ConsumerEmbeddingBasedTwoTowerParams.config,
      ConsumersBasedUserAdGraphParams.config,
      ConsumersBasedUserTweetGraphParams.config,
      ConsumersBasedUserVideoGraphParams.config,
      CustomizedRetrievalBasedCandidateGenerationParams.config,
      CustomizedRetrievalBasedOfflineInterestedInParams.config,
      CustomizedRetrievalBasedFTROfflineInterestedInParams.config,
      CustomizedRetrievalBasedTwhinParams.config,
      EarlybirdFrsBasedCandidateGenerationParams.config,
      FrsParams.config,
      GlobalParams.config,
      InterestedInParams.config,
      ProducerBasedCandidateGenerationParams.config,
      ProducerBasedUserAdGraphParams.config,
      ProducerBasedUserTweetGraphParams.config,
      RecentFollowsParams.config,
      RecentNegativeSignalParams.config,
      RecentNotificationsParams.config,
      RecentOriginalTweetsParams.config,
      RecentReplyTweetsParams.config,
      RecentRetweetsParams.config,
      RecentTweetFavoritesParams.config,
      RelatedTweetGlobalParams.config,
      RelatedVideoTweetGlobalParams.config,
      RelatedTweetProducerBasedParams.config,
      RelatedTweetTweetBasedParams.config,
      RelatedVideoTweetTweetBasedParams.config,
      RealGraphInParams.config,
      RealGraphOonParams.config,
      RepeatedProfileVisitsParams.config,
      SimClustersANNParams.config,
      TopicTweetParams.config,
      TweetBasedCandidateGenerationParams.config,
      TweetBasedUserAdGraphParams.config,
      TweetBasedUserTweetGraphParams.config,
      TweetBasedUserVideoGraphParams.config,
      TweetSharesParams.config,
      TweetBasedTwHINParams.config,
      RealGraphOonParams.config,
      GoodTweetClickParams.config,
      GoodProfileClickParams.config,
      UtegTweetGlobalParams.config,
      VideoTweetFilterParams.config,
      VideoViewTweetsParams.config,
      UnifiedUSSSignalParams.config,
    ),
    simpleName = "CrMixerConfig"
  )

  val allParams: Seq[Param[_] with FSName] = {
    AdsParams.AllParams ++
      BlenderParams.AllParams ++
      BypassInterleaveAndRankParams.AllParams ++
      RankerParams.AllParams ++
      ConsumerBasedWalsParams.AllParams ++
      ConsumerEmbeddingBasedCandidateGenerationParams.AllParams ++
      ConsumerEmbeddingBasedTripParams.AllParams ++
      ConsumerEmbeddingBasedTwHINParams.AllParams ++
      ConsumerEmbeddingBasedTwoTowerParams.AllParams ++
      ConsumersBasedUserAdGraphParams.AllParams ++
      ConsumersBasedUserTweetGraphParams.AllParams ++
      ConsumersBasedUserVideoGraphParams.AllParams ++
      CustomizedRetrievalBasedCandidateGenerationParams.AllParams ++
      CustomizedRetrievalBasedOfflineInterestedInParams.AllParams ++
      CustomizedRetrievalBasedFTROfflineInterestedInParams.AllParams ++
      CustomizedRetrievalBasedTwhinParams.AllParams ++
      EarlybirdFrsBasedCandidateGenerationParams.AllParams ++
      FrsParams.AllParams ++
      GlobalParams.AllParams ++
      InterestedInParams.AllParams ++
      ProducerBasedCandidateGenerationParams.AllParams ++
      ProducerBasedUserAdGraphParams.AllParams ++
      ProducerBasedUserTweetGraphParams.AllParams ++
      RecentFollowsParams.AllParams ++
      RecentNegativeSignalParams.AllParams ++
      RecentNotificationsParams.AllParams ++
      RecentOriginalTweetsParams.AllParams ++
      RecentReplyTweetsParams.AllParams ++
      RecentRetweetsParams.AllParams ++
      RecentTweetFavoritesParams.AllParams ++
      RelatedTweetGlobalParams.AllParams ++
      RelatedVideoTweetGlobalParams.AllParams ++
      RelatedTweetProducerBasedParams.AllParams ++
      RelatedTweetTweetBasedParams.AllParams ++
      RelatedVideoTweetTweetBasedParams.AllParams ++
      RepeatedProfileVisitsParams.AllParams ++
      SimClustersANNParams.AllParams ++
      TopicTweetParams.AllParams ++
      TweetBasedCandidateGenerationParams.AllParams ++
      TweetBasedUserAdGraphParams.AllParams ++
      TweetBasedUserTweetGraphParams.AllParams ++
      TweetBasedUserVideoGraphParams.AllParams ++
      TweetSharesParams.AllParams ++
      TweetBasedTwHINParams.AllParams ++
      RealGraphOonParams.AllParams ++
      RealGraphInParams.AllParams ++
      GoodTweetClickParams.AllParams ++
      GoodProfileClickParams.AllParams ++
      UtegTweetGlobalParams.AllParams ++
      VideoTweetFilterParams.AllParams ++
      VideoViewTweetsParams.AllParams ++
      UnifiedUSSSignalParams.AllParams
  }
}
package com.twitter.cr_mixer.param

import com.twitter.cr_mixer.model.ModelConfig
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.Param

import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil

object ConsumerEmbeddingBasedTwHINParams {
  object ModelIdParam
      extends FSParam[String](
        name = "consumer_embedding_based_twhin_model_id",
        default = ModelConfig.ConsumerBasedTwHINRegularUpdateAll20221024,
      ) // Note: this default value does not match with ModelIds yet. This FS is a placeholder

  val AllParams: Seq[Param[_] with FSName] = Seq(
    ModelIdParam
  )

  lazy val config: BaseConfig = {
    val stringFSOverrides =
      FeatureSwitchOverrideUtil.getStringFSOverrides(
        ModelIdParam
      )

    BaseConfigBuilder()
      .set(stringFSOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.conversions.DurationOps._
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.DurationConversion
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.HasDurationConversion
import com.twitter.timelines.configapi.Param
import com.twitter.util.Duration

object UtegTweetGlobalParams {

  object MaxUtegCandidatesToRequestParam
      extends FSBoundedParam[Int](
        name = "max_uteg_candidates_to_request",
        default = 800,
        min = 10,
        max = 200
      )

  object CandidateRefreshSinceTimeOffsetHoursParam
      extends FSBoundedParam[Duration](
        name = "candidate_refresh_since_time_offset_hours",
        default = 48.hours,
        min = 1.hours,
        max = 96.hours
      )
      with HasDurationConversion {
    override val durationConversion: DurationConversion = DurationConversion.FromHours
  }

  object EnableTLRHealthFilterParam
      extends FSParam[Boolean](
        name = "enable_uteg_tlr_health_filter",
        default = true
      )

  object EnableRepliesToNonFollowedUsersFilterParam
      extends FSParam[Boolean](
        name = "enable_uteg_replies_to_non_followed_users_filter",
        default = false
      )

  object EnableRetweetFilterParam
      extends FSParam[Boolean](
        name = "enable_uteg_retweet_filter",
        default = true
      )

  object EnableInNetworkFilterParam
      extends FSParam[Boolean](
        name = "enable_uteg_in_network_filter",
        default = true
      )

  val AllParams: Seq[Param[_] with FSName] =
    Seq(
      MaxUtegCandidatesToRequestParam,
      CandidateRefreshSinceTimeOffsetHoursParam,
      EnableTLRHealthFilterParam,
      EnableRepliesToNonFollowedUsersFilterParam,
      EnableRetweetFilterParam,
      EnableInNetworkFilterParam
    )

  lazy val config: BaseConfig = {

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MaxUtegCandidatesToRequestParam
    )

    val durationFSOverrides =
      FeatureSwitchOverrideUtil.getDurationFSOverrides(
        CandidateRefreshSinceTimeOffsetHoursParam
      )

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableTLRHealthFilterParam,
      EnableRepliesToNonFollowedUsersFilterParam,
      EnableRetweetFilterParam,
      EnableInNetworkFilterParam
    )

    BaseConfigBuilder()
      .set(intOverrides: _*)
      .set(durationFSOverrides: _*)
      .set(booleanOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object ConsumerEmbeddingBasedCandidateGenerationParams {

  object EnableTwHINParam
      extends FSParam[Boolean](
        name = "consumer_embedding_based_candidate_generation_enable_twhin",
        default = false
      )

  object EnableTwoTowerParam
      extends FSParam[Boolean](
        name = "consumer_embedding_based_candidate_generation_enable_two_tower",
        default = false
      )

  object EnableLogFavBasedSimClustersTripParam
      extends FSParam[Boolean](
        name = "consumer_embedding_based_candidate_generation_enable_logfav_based_simclusters_trip",
        default = false
      )

  object EnableFollowBasedSimClustersTripParam
      extends FSParam[Boolean](
        name = "consumer_embedding_based_candidate_generation_enable_follow_based_simclusters_trip",
        default = false
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableTwHINParam,
    EnableTwoTowerParam,
    EnableFollowBasedSimClustersTripParam,
    EnableLogFavBasedSimClustersTripParam
  )

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableTwHINParam,
      EnableTwoTowerParam,
      EnableFollowBasedSimClustersTripParam,
      EnableLogFavBasedSimClustersTripParam
    )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object TweetSharesParams {
  object EnableSourceParam
      extends FSParam[Boolean](
        name = "twistly_tweetshares_enable_source",
        default = false
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(EnableSourceParam)

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam,
    )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .build()
  }

}
package com.twitter.cr_mixer.param

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object TweetBasedCandidateGenerationParams {

  // Source params. Not being used. It is always set to true in prod
  object EnableSourceParam
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_source",
        default = false
      )

  // UTG params
  object EnableUTGParam
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_utg",
        default = true
      )

  // SimClusters params
  object EnableSimClustersANNParam
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_simclusters",
        default = true
      )

  // Experimental SimClusters ANN params
  object EnableExperimentalSimClustersANNParam
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_experimental_simclusters_ann",
        default = false
      )

  // SimClusters ANN cluster 1 params
  object EnableSimClustersANN1Param
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_simclusters_ann_1",
        default = false
      )

  // SimClusters ANN cluster 2 params
  object EnableSimClustersANN2Param
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_simclusters_ann_2",
        default = false
      )

  // SimClusters ANN cluster 3 params
  object EnableSimClustersANN3Param
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_simclusters_ann_3",
        default = false
      )

  // SimClusters ANN cluster 3 params
  object EnableSimClustersANN5Param
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_simclusters_ann_5",
        default = false
      )

  // SimClusters ANN cluster 4 params
  object EnableSimClustersANN4Param
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_simclusters_ann_4",
        default = false
      )
  // TwHIN params
  object EnableTwHINParam
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_twhin",
        default = false
      )

  // QIG params
  object EnableQigSimilarTweetsParam
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_qig_similar_tweets",
        default = false
      )

  object QigMaxNumSimilarTweetsParam
      extends FSBoundedParam[Int](
        name = "tweet_based_candidate_generation_qig_max_num_similar_tweets",
        default = 100,
        min = 10,
        max = 100
      )

  // UVG params
  object EnableUVGParam
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_uvg",
        default = false
      )

  // UAG params
  object EnableUAGParam
      extends FSParam[Boolean](
        name = "tweet_based_candidate_generation_enable_uag",
        default = false
      )

  // Filter params
  object SimClustersMinScoreParam
      extends FSBoundedParam[Double](
        name = "tweet_based_candidate_generation_filter_simclusters_min_score",
        default = 0.5,
        min = 0.0,
        max = 1.0
      )

  // for learning DDG that has a higher threshold for video based SANN
  object SimClustersVideoBasedMinScoreParam
      extends FSBoundedParam[Double](
        name = "tweet_based_candidate_generation_filter_simclusters_video_based_min_score",
        default = 0.5,
        min = 0.0,
        max = 1.0
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableSourceParam,
    EnableTwHINParam,
    EnableQigSimilarTweetsParam,
    EnableUTGParam,
    EnableUVGParam,
    EnableUAGParam,
    EnableSimClustersANNParam,
    EnableSimClustersANN1Param,
    EnableSimClustersANN2Param,
    EnableSimClustersANN3Param,
    EnableSimClustersANN5Param,
    EnableSimClustersANN4Param,
    EnableExperimentalSimClustersANNParam,
    SimClustersMinScoreParam,
    SimClustersVideoBasedMinScoreParam,
    QigMaxNumSimilarTweetsParam,
  )

  lazy val config: BaseConfig = {

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam,
      EnableTwHINParam,
      EnableQigSimilarTweetsParam,
      EnableUTGParam,
      EnableUVGParam,
      EnableUAGParam,
      EnableSimClustersANNParam,
      EnableSimClustersANN1Param,
      EnableSimClustersANN2Param,
      EnableSimClustersANN3Param,
      EnableSimClustersANN5Param,
      EnableSimClustersANN4Param,
      EnableExperimentalSimClustersANNParam,
    )

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(
        SimClustersMinScoreParam,
        SimClustersVideoBasedMinScoreParam)

    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
    )

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      QigMaxNumSimilarTweetsParam
    )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .set(enumOverrides: _*)
      .set(intOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object RecentTweetFavoritesParams {
  // Source params
  object EnableSourceParam
      extends FSParam[Boolean](
        name = "twistly_recenttweetfavorites_enable_source",
        default = true
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(EnableSourceParam)

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam
    )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .build()
  }
}