package com.twitter.cr_mixer.param

import com.twitter.cr_mixer.model.ModelConfig.TwoTowerFavALL20220808
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object ConsumerEmbeddingBasedTwoTowerParams {
  object ModelIdParam
      extends FSParam[String](
        name = "consumer_embedding_based_two_tower_model_id",
        default = TwoTowerFavALL20220808,
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

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object BypassInterleaveAndRankParams {
  object EnableTwhinCollabFilterBypassParam
      extends FSParam[Boolean](
        name = "bypass_interleave_and_rank_twhin_collab_filter",
        default = false
      )

  object EnableTwoTowerBypassParam
      extends FSParam[Boolean](
        name = "bypass_interleave_and_rank_two_tower",
        default = false
      )

  object EnableConsumerBasedTwhinBypassParam
      extends FSParam[Boolean](
        name = "bypass_interleave_and_rank_consumer_based_twhin",
        default = false
      )

  object EnableConsumerBasedWalsBypassParam
      extends FSParam[Boolean](
        name = "bypass_interleave_and_rank_consumer_based_wals",
        default = false
      )

  object TwhinCollabFilterBypassPercentageParam
      extends FSBoundedParam[Double](
        name = "bypass_interleave_and_rank_twhin_collab_filter_percentage",
        default = 0.0,
        min = 0.0,
        max = 1.0
      )

  object TwoTowerBypassPercentageParam
      extends FSBoundedParam[Double](
        name = "bypass_interleave_and_rank_two_tower_percentage",
        default = 0.0,
        min = 0.0,
        max = 1.0
      )

  object ConsumerBasedTwhinBypassPercentageParam
      extends FSBoundedParam[Double](
        name = "bypass_interleave_and_rank_consumer_based_twhin_percentage",
        default = 0.0,
        min = 0.0,
        max = 1.0
      )

  object ConsumerBasedWalsBypassPercentageParam
      extends FSBoundedParam[Double](
        name = "bypass_interleave_and_rank_consumer_based_wals_percentage",
        default = 0.0,
        min = 0.0,
        max = 1.0
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableTwhinCollabFilterBypassParam,
    EnableTwoTowerBypassParam,
    EnableConsumerBasedTwhinBypassParam,
    EnableConsumerBasedWalsBypassParam,
    TwhinCollabFilterBypassPercentageParam,
    TwoTowerBypassPercentageParam,
    ConsumerBasedTwhinBypassPercentageParam,
    ConsumerBasedWalsBypassPercentageParam,
  )

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableTwhinCollabFilterBypassParam,
      EnableTwoTowerBypassParam,
      EnableConsumerBasedTwhinBypassParam,
      EnableConsumerBasedWalsBypassParam,
    )

    val doubleOverrides = FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(
      TwhinCollabFilterBypassPercentageParam,
      TwoTowerBypassPercentageParam,
      ConsumerBasedTwhinBypassPercentageParam,
      ConsumerBasedWalsBypassPercentageParam,
    )
    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
import com.twitter.usersignalservice.thriftscala.SignalType
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSEnumParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object RepeatedProfileVisitsParams {
  object ProfileMinVisitParam extends Enumeration {
    protected case class SignalTypeValue(signalType: SignalType) extends super.Val
    import scala.language.implicitConversions
    implicit def valueToSignalTypeValue(x: Value): SignalTypeValue =
      x.asInstanceOf[SignalTypeValue]

    val TotalVisitsInPast180Days = SignalTypeValue(SignalType.RepeatedProfileVisit180dMinVisit6V1)
    val TotalVisitsInPast90Days = SignalTypeValue(SignalType.RepeatedProfileVisit90dMinVisit6V1)
    val TotalVisitsInPast14Days = SignalTypeValue(SignalType.RepeatedProfileVisit14dMinVisit2V1)
    val TotalVisitsInPast180DaysNoNegative = SignalTypeValue(
      SignalType.RepeatedProfileVisit180dMinVisit6V1NoNegative)
    val TotalVisitsInPast90DaysNoNegative = SignalTypeValue(
      SignalType.RepeatedProfileVisit90dMinVisit6V1NoNegative)
    val TotalVisitsInPast14DaysNoNegative = SignalTypeValue(
      SignalType.RepeatedProfileVisit14dMinVisit2V1NoNegative)
  }

  object EnableSourceParam
      extends FSParam[Boolean](
        name = "twistly_repeatedprofilevisits_enable_source",
        default = true
      )

  object MinScoreParam
      extends FSBoundedParam[Double](
        name = "twistly_repeatedprofilevisits_min_score",
        default = 0.5,
        min = 0.0,
        max = 1.0
      )

  object ProfileMinVisitType
      extends FSEnumParam[ProfileMinVisitParam.type](
        name = "twistly_repeatedprofilevisits_min_visit_type_id",
        default = ProfileMinVisitParam.TotalVisitsInPast14Days,
        enum = ProfileMinVisitParam
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(EnableSourceParam, ProfileMinVisitType)

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam
    )

    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
      ProfileMinVisitType
    )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(enumOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.model

/**
 * Define name annotated module names here
 */
object ModuleNames {

  final val FrsStore = "FrsStore"
  final val UssStore = "UssStore"
  final val UssStratoColumn = "UssStratoColumn"
  final val RsxStore = "RsxStore"
  final val RmsTweetLogFavLongestL2EmbeddingStore = "RmsTweetLogFavLongestL2EmbeddingStore"
  final val RmsUserFavBasedProducerEmbeddingStore = "RmsUserFavBasedProducerEmbeddingStore"
  final val RmsUserLogFavInterestedInEmbeddingStore = "RmsUserLogFavInterestedInEmbeddingStore"
  final val RmsUserFollowInterestedInEmbeddingStore = "RmsUserFollowInterestedInEmbeddingStore"
  final val StpStore = "StpStore"
  final val TwiceClustersMembersStore = "TwiceClustersMembersStore"
  final val TripCandidateStore = "TripCandidateStore"

  final val ConsumerEmbeddingBasedTripSimilarityEngine =
    "ConsumerEmbeddingBasedTripSimilarityEngine"
  final val ConsumerEmbeddingBasedTwHINANNSimilarityEngine =
    "ConsumerEmbeddingBasedTwHINANNSimilarityEngine"
  final val ConsumerEmbeddingBasedTwoTowerANNSimilarityEngine =
    "ConsumerEmbeddingBasedTwoTowerANNSimilarityEngine"
  final val ConsumersBasedUserAdGraphSimilarityEngine =
    "ConsumersBasedUserAdGraphSimilarityEngine"
  final val ConsumersBasedUserVideoGraphSimilarityEngine =
    "ConsumersBasedUserVideoGraphSimilarityEngine"

  final val ConsumerBasedWalsSimilarityEngine = "ConsumerBasedWalsSimilarityEngine"

  final val TweetBasedTwHINANNSimilarityEngine = "TweetBasedTwHINANNSimilarityEngine"

  final val SimClustersANNSimilarityEngine = "SimClustersANNSimilarityEngine"

  final val ProdSimClustersANNServiceClientName = "ProdSimClustersANNServiceClient"
  final val ExperimentalSimClustersANNServiceClientName = "ExperimentalSimClustersANNServiceClient"
  final val SimClustersANNServiceClientName1 = "SimClustersANNServiceClient1"
  final val SimClustersANNServiceClientName2 = "SimClustersANNServiceClient2"
  final val SimClustersANNServiceClientName3 = "SimClustersANNServiceClient3"
  final val SimClustersANNServiceClientName5 = "SimClustersANNServiceClient5"
  final val SimClustersANNServiceClientName4 = "SimClustersANNServiceClient4"
  final val UnifiedCache = "unifiedCache"
  final val MLScoreCache = "mlScoreCache"
  final val TweetRecommendationResultsCache = "tweetRecommendationResultsCache"
  final val EarlybirdTweetsCache = "earlybirdTweetsCache"
  final val EarlybirdRecencyBasedWithoutRetweetsRepliesTweetsCache =
    "earlybirdTweetsWithoutRetweetsRepliesCacheStore"
  final val EarlybirdRecencyBasedWithRetweetsRepliesTweetsCache =
    "earlybirdTweetsWithRetweetsRepliesCacheStore"

  final val AbDeciderLogger = "abDeciderLogger"
  final val TopLevelApiDdgMetricsLogger = "topLevelApiDdgMetricsLogger"
  final val TweetRecsLogger = "tweetRecsLogger"
  final val BlueVerifiedTweetRecsLogger = "blueVerifiedTweetRecsLogger"
  final val RelatedTweetsLogger = "relatedTweetsLogger"
  final val UtegTweetsLogger = "utegTweetsLogger"
  final val AdsRecommendationsLogger = "adsRecommendationLogger"

  final val OfflineSimClustersANNInterestedInSimilarityEngine =
    "OfflineSimClustersANNInterestedInSimilarityEngine"

  final val RealGraphOonStore = "RealGraphOonStore"
  final val RealGraphInStore = "RealGraphInStore"

  final val OfflineTweet2020CandidateStore = "OfflineTweet2020CandidateStore"
  final val OfflineTweet2020Hl0El15CandidateStore = "OfflineTweet2020Hl0El15CandidateStore"
  final val OfflineTweet2020Hl2El15CandidateStore = "OfflineTweet2020Hl2El15CandidateStore"
  final val OfflineTweet2020Hl2El50CandidateStore = "OfflineTweet2020Hl2El50CandidateStore"
  final val OfflineTweet2020Hl8El50CandidateStore = "OfflineTweet2020Hl8El50CandidateStore"
  final val OfflineTweetMTSCandidateStore = "OfflineTweetMTSCandidateStore"

  final val OfflineFavDecayedSumCandidateStore = "OfflineFavDecayedSumCandidateStore"
  final val OfflineFtrAt5Pop1000RankDecay11CandidateStore =
    "OfflineFtrAt5Pop1000RankDecay11CandidateStore"
  final val OfflineFtrAt5Pop10000RankDecay11CandidateStore =
    "OfflineFtrAt5Pop10000RankDecay11CandidateStore"

  final val TwhinCollabFilterStratoStoreForFollow = "TwhinCollabFilterStratoStoreForFollow"
  final val TwhinCollabFilterStratoStoreForEngagement = "TwhinCollabFilterStratoStoreForEngagement"
  final val TwhinMultiClusterStratoStoreForFollow = "TwhinMultiClusterStratoStoreForFollow"
  final val TwhinMultiClusterStratoStoreForEngagement = "TwhinMultiClusterStratoStoreForEngagement"

  final val ProducerBasedUserAdGraphSimilarityEngine =
    "ProducerBasedUserAdGraphSimilarityEngine"
  final val ProducerBasedUserTweetGraphSimilarityEngine =
    "ProducerBasedUserTweetGraphSimilarityEngine"
  final val ProducerBasedUnifiedSimilarityEngine = "ProducerBasedUnifiedSimilarityEngine"

  final val TweetBasedUserAdGraphSimilarityEngine = "TweetBasedUserAdGraphSimilarityEngine"
  final val TweetBasedUserTweetGraphSimilarityEngine = "TweetBasedUserTweetGraphSimilarityEngine"
  final val TweetBasedUserVideoGraphSimilarityEngine = "TweetBasedUserVideoGraphSimilarityEngine"
  final val TweetBasedQigSimilarityEngine = "TweetBasedQigSimilarityEngine"
  final val TweetBasedUnifiedSimilarityEngine = "TweetBasedUnifiedSimilarityEngine"

  final val TwhinCollabFilterSimilarityEngine = "TwhinCollabFilterSimilarityEngine"

  final val ConsumerBasedUserTweetGraphStore = "ConsumerBasedUserTweetGraphStore"
  final val ConsumerBasedUserVideoGraphStore = "ConsumerBasedUserVideoGraphStore"
  final val ConsumerBasedUserAdGraphStore = "ConsumerBasedUserAdGraphStore"

  final val UserTweetEntityGraphSimilarityEngine =
    "UserTweetEntityGraphSimilarityEngine"

  final val CertoTopicTweetSimilarityEngine = "CertoTopicTweetSimilarityEngine"
  final val CertoStratoStoreName = "CertoStratoStore"

  final val SkitTopicTweetSimilarityEngine = "SkitTopicTweetSimilarityEngine"
  final val SkitHighPrecisionTopicTweetSimilarityEngine =
    "SkitHighPrecisionTopicTweetSimilarityEngine"
  final val SkitStratoStoreName = "SkitStratoStore"

  final val HomeNaviGRPCClient = "HomeNaviGRPCClient"
  final val AdsFavedNaviGRPCClient = "AdsFavedNaviGRPCClient"
  final val AdsMonetizableNaviGRPCClient = "AdsMonetizableNaviGRPCClient"

  final val RetweetBasedDiffusionRecsMhStore = "RetweetBasedDiffusionRecsMhStore"
  final val DiffusionBasedSimilarityEngine = "DiffusionBasedSimilarityEngine"

  final val BlueVerifiedAnnotationStore = "BlueVerifiedAnnotationStore"
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object ProducerBasedUserAdGraphParams {

  object MinCoOccurrenceParam
      extends FSBoundedParam[Int](
        name = "producer_based_user_ad_graph_min_co_occurrence",
        default = 2,
        min = 0,
        max = 500
      )

  object MinScoreParam
      extends FSBoundedParam[Double](
        name = "producer_based_user_ad_graph_min_score",
        default = 3.0,
        min = 0.0,
        max = 10.0
      )

  object MaxNumFollowersParam
      extends FSBoundedParam[Int](
        name = "producer_based_user_ad_graph_max_num_followers",
        default = 500,
        min = 100,
        max = 1000
      )

  val AllParams: Seq[Param[_] with FSName] =
    Seq(MinCoOccurrenceParam, MaxNumFollowersParam, MinScoreParam)

  lazy val config: BaseConfig = {

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MinCoOccurrenceParam,
      MaxNumFollowersParam,
    )

    val doubleOverrides = FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(MinScoreParam)

    BaseConfigBuilder()
      .set(intOverrides: _*)
      .set(doubleOverrides: _*)
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

/**
 * ConsumersBasedUserTweetGraph Params, there are multiple ways (e.g. FRS, RealGraphOon) to generate consumersSeedSet for ConsumersBasedUserTweetGraph
 * for now we allow flexibility in tuning UTG params for different consumersSeedSet generation algo by giving the param name {consumerSeedSetAlgo}{ParamName}
 */

object ConsumersBasedUserTweetGraphParams {

  object EnableSourceParam
      extends FSParam[Boolean](
        name = "consumers_based_user_tweet_graph_enable_source",
        default = false
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableSourceParam,
  )

  lazy val config: BaseConfig = {

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides()

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides()

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam
    )

    BaseConfigBuilder()
      .set(intOverrides: _*)
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
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

object TopicTweetParams {
  object MaxTweetAge
      extends FSBoundedParam[Duration](
        name = "topic_tweet_candidate_generation_max_tweet_age_hours",
        default = 24.hours,
        min = 12.hours,
        max = 48.hours
      )
      with HasDurationConversion {
    override val durationConversion: DurationConversion = DurationConversion.FromHours
  }

  object MaxTopicTweetCandidatesParam
      extends FSBoundedParam[Int](
        name = "topic_tweet_max_candidates_num",
        default = 200,
        min = 0,
        max = 1000
      )

  object MaxSkitTfgCandidatesParam
      extends FSBoundedParam[Int](
        name = "topic_tweet_skit_tfg_max_candidates_num",
        default = 100,
        min = 0,
        max = 1000
      )

  object MaxSkitHighPrecisionCandidatesParam
      extends FSBoundedParam[Int](
        name = "topic_tweet_skit_high_precision_max_candidates_num",
        default = 100,
        min = 0,
        max = 1000
      )

  object MaxCertoCandidatesParam
      extends FSBoundedParam[Int](
        name = "topic_tweet_certo_max_candidates_num",
        default = 100,
        min = 0,
        max = 1000
      )

  // The min prod score for Certo L2-normalized cosine candidates
  object CertoScoreThresholdParam
      extends FSBoundedParam[Double](
        name = "topic_tweet_certo_score_threshold",
        default = 0.015,
        min = 0,
        max = 1
      )

  object SemanticCoreVersionIdParam
      extends FSParam[Long](
        name = "semantic_core_version_id",
        default = 1380520918896713735L
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    CertoScoreThresholdParam,
    MaxTopicTweetCandidatesParam,
    MaxTweetAge,
    MaxCertoCandidatesParam,
    MaxSkitTfgCandidatesParam,
    MaxSkitHighPrecisionCandidatesParam,
    SemanticCoreVersionIdParam
  )

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides()

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(CertoScoreThresholdParam)

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MaxCertoCandidatesParam,
      MaxSkitTfgCandidatesParam,
      MaxSkitHighPrecisionCandidatesParam,
      MaxTopicTweetCandidatesParam
    )

    val longOverrides = FeatureSwitchOverrideUtil.getLongFSOverrides(SemanticCoreVersionIdParam)

    val durationFSOverrides = FeatureSwitchOverrideUtil.getDurationFSOverrides(MaxTweetAge)

    val enumOverrides =
      FeatureSwitchOverrideUtil.getEnumFSOverrides(NullStatsReceiver, Logger(getClass))

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .set(intOverrides: _*)
      .set(longOverrides: _*)
      .set(enumOverrides: _*)
      .set(durationFSOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object AdsParams {
  object AdsCandidateGenerationMaxCandidatesNumParam
      extends FSBoundedParam[Int](
        name = "ads_candidate_generation_max_candidates_num",
        default = 400,
        min = 0,
        max = 2000
      )

  object EnableScoreBoost
      extends FSParam[Boolean](
        name = "ads_candidate_generation_enable_score_boost",
        default = false
      )

  object AdsCandidateGenerationScoreBoostFactor
      extends FSBoundedParam[Double](
        name = "ads_candidate_generation_score_boost_factor",
        default = 10000.0,
        min = 1.0,
        max = 100000.0
      )

  object EnableScribe
      extends FSParam[Boolean](
        name = "ads_candidate_generation_enable_scribe",
        default = false
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    AdsCandidateGenerationMaxCandidatesNumParam,
    EnableScoreBoost,
    AdsCandidateGenerationScoreBoostFactor
  )

  lazy val config: BaseConfig = {
    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      AdsCandidateGenerationMaxCandidatesNumParam)

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableScoreBoost,
      EnableScribe
    )

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(AdsCandidateGenerationScoreBoostFactor)

    BaseConfigBuilder()
      .set(intOverrides: _*)
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType_ModelBased
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType_RecencyBased
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType_TensorflowBased
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.DurationConversion
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSEnumParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.HasDurationConversion
import com.twitter.timelines.configapi.Param
import com.twitter.util.Duration

object EarlybirdFrsBasedCandidateGenerationParams {
  object CandidateGenerationEarlybirdSimilarityEngineType extends Enumeration {
    protected case class SimilarityEngineType(rankingMode: EarlybirdSimilarityEngineType)
        extends super.Val
    import scala.language.implicitConversions
    implicit def valueToEarlybirdRankingMode(x: Value): SimilarityEngineType =
      x.asInstanceOf[SimilarityEngineType]

    val EarlybirdRankingMode_RecencyBased: SimilarityEngineType = SimilarityEngineType(
      EarlybirdSimilarityEngineType_RecencyBased)
    val EarlybirdRankingMode_ModelBased: SimilarityEngineType = SimilarityEngineType(
      EarlybirdSimilarityEngineType_ModelBased)
    val EarlybirdRankingMode_TensorflowBased: SimilarityEngineType = SimilarityEngineType(
      EarlybirdSimilarityEngineType_TensorflowBased)
  }

  object FrsBasedCandidateGenerationEarlybirdSimilarityEngineTypeParam
      extends FSEnumParam[CandidateGenerationEarlybirdSimilarityEngineType.type](
        name = "frs_based_candidate_generation_earlybird_ranking_mode_id",
        default =
          CandidateGenerationEarlybirdSimilarityEngineType.EarlybirdRankingMode_RecencyBased,
        enum = CandidateGenerationEarlybirdSimilarityEngineType
      )

  object FrsBasedCandidateGenerationRecencyBasedEarlybirdMaxTweetsPerUser
      extends FSBoundedParam[Int](
        name = "frs_based_candidate_generation_earlybird_max_tweets_per_user",
        default = 100,
        min = 0,
        /**
         * Note max should be equal to EarlybirdRecencyBasedCandidateStoreModule.DefaultMaxNumTweetPerUser.
         * Which is the size of the memcached result list.
         */
        max = 100
      )

  object FrsBasedCandidateGenerationEarlybirdMaxTweetAge
      extends FSBoundedParam[Duration](
        name = "frs_based_candidate_generation_earlybird_max_tweet_age_hours",
        default = 24.hours,
        min = 12.hours,
        /**
         * Note max could be related to EarlybirdRecencyBasedCandidateStoreModule.DefaultMaxNumTweetPerUser.
         * Which is the size of the memcached result list for recency based earlybird candidate source.
         * E.g. if max = 720.hours, we may want to increase the DefaultMaxNumTweetPerUser.
         */
        max = 96.hours
      )
      with HasDurationConversion {
    override val durationConversion: DurationConversion = DurationConversion.FromHours
  }

  object FrsBasedCandidateGenerationEarlybirdFilterOutRetweetsAndReplies
      extends FSParam[Boolean](
        name = "frs_based_candidate_generation_earlybird_filter_out_retweets_and_replies",
        default = true
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    FrsBasedCandidateGenerationEarlybirdSimilarityEngineTypeParam,
    FrsBasedCandidateGenerationRecencyBasedEarlybirdMaxTweetsPerUser,
    FrsBasedCandidateGenerationEarlybirdMaxTweetAge,
    FrsBasedCandidateGenerationEarlybirdFilterOutRetweetsAndReplies,
  )

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      FrsBasedCandidateGenerationEarlybirdFilterOutRetweetsAndReplies,
    )

    val doubleOverrides = FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides()

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      FrsBasedCandidateGenerationRecencyBasedEarlybirdMaxTweetsPerUser
    )

    val durationFSOverrides =
      FeatureSwitchOverrideUtil.getDurationFSOverrides(
        FrsBasedCandidateGenerationEarlybirdMaxTweetAge
      )

    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
      FrsBasedCandidateGenerationEarlybirdSimilarityEngineTypeParam,
    )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .set(intOverrides: _*)
      .set(enumOverrides: _*)
      .set(durationFSOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object TweetBasedUserAdGraphParams {

  object MinCoOccurrenceParam
      extends FSBoundedParam[Int](
        name = "tweet_based_user_ad_graph_min_co_occurrence",
        default = 1,
        min = 0,
        max = 500
      )

  object ConsumersBasedMinScoreParam
      extends FSBoundedParam[Double](
        name = "tweet_based_user_ad_graph_consumers_based_min_score",
        default = 0.0,
        min = 0.0,
        max = 10.0
      )

  object MaxConsumerSeedsNumParam
      extends FSBoundedParam[Int](
        name = "tweet_based_user_ad_graph_max_user_seeds_num",
        default = 100,
        min = 0,
        max = 300
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    MinCoOccurrenceParam,
    MaxConsumerSeedsNumParam,
    ConsumersBasedMinScoreParam
  )

  lazy val config: BaseConfig = {

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MinCoOccurrenceParam,
      MaxConsumerSeedsNumParam
    )

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(ConsumersBasedMinScoreParam)

    BaseConfigBuilder()
      .set(intOverrides: _*)
      .set(doubleOverrides: _*)
      .build()
  }

}
