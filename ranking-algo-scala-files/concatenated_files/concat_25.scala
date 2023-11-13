package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object TweetBasedUserTweetGraphParams {

  object MinCoOccurrenceParam
      extends FSBoundedParam[Int](
        name = "tweet_based_user_tweet_graph_min_co_occurrence",
        default = 3,
        min = 0,
        max = 500
      )

  object TweetBasedMinScoreParam
      extends FSBoundedParam[Double](
        name = "tweet_based_user_tweet_graph_tweet_based_min_score",
        default = 0.5,
        min = 0.0,
        max = 10.0
      )

  object ConsumersBasedMinScoreParam
      extends FSBoundedParam[Double](
        name = "tweet_based_user_tweet_graph_consumers_based_min_score",
        default = 4.0,
        min = 0.0,
        max = 10.0
      )
  object MaxConsumerSeedsNumParam
      extends FSBoundedParam[Int](
        name = "tweet_based_user_tweet_graph_max_user_seeds_num",
        default = 100,
        min = 0,
        max = 300
      )

  object EnableCoverageExpansionOldTweetParam
      extends FSParam[Boolean](
        name = "tweet_based_user_tweet_graph_enable_coverage_expansion_old_tweet",
        default = false
      )

  object EnableCoverageExpansionAllTweetParam
      extends FSParam[Boolean](
        name = "tweet_based_user_tweet_graph_enable_coverage_expansion_all_tweet",
        default = false
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableCoverageExpansionAllTweetParam,
    EnableCoverageExpansionOldTweetParam,
    MinCoOccurrenceParam,
    MaxConsumerSeedsNumParam,
    TweetBasedMinScoreParam,
    ConsumersBasedMinScoreParam
  )

  lazy val config: BaseConfig = {

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableCoverageExpansionAllTweetParam,
      EnableCoverageExpansionOldTweetParam
    )

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MinCoOccurrenceParam,
      MaxConsumerSeedsNumParam
    )

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(
        TweetBasedMinScoreParam,
        ConsumersBasedMinScoreParam)

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(intOverrides: _*)
      .set(doubleOverrides: _*)
      .build()
  }

}
package com.twitter.cr_mixer.param.decider

import com.twitter.decider.Decider
import com.twitter.decider.RandomRecipient
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future
import javax.inject.Inject
import scala.util.control.NoStackTrace

/*
  Provides deciders-controlled load shedding for a given Product from a given endpoint.
  The format of the decider keys is:

    enable_loadshedding_<endpoint name>_<product name>
  E.g.:
    enable_loadshedding_getTweetRecommendations_Notifications

  Deciders are fractional, so a value of 50.00 will drop 50% of responses. If a decider key is not
  defined for a particular endpoint/product combination, those requests will always be
  served.

  We should therefore aim to define keys for the endpoints/product we care most about in decider.yml,
  so that we can control them during incidents.
 */
case class EndpointLoadShedder @Inject() (
  decider: Decider,
  statsReceiver: StatsReceiver) {
  import EndpointLoadShedder._

  // Fall back to False for any undefined key
  private val deciderWithFalseFallback: Decider = decider.orElse(Decider.False)
  private val keyPrefix = "enable_loadshedding"
  private val scopedStats = statsReceiver.scope("EndpointLoadShedder")

  def apply[T](endpointName: String, product: String)(serve: => Future[T]): Future[T] = {
    /*
    Checks if either per-product or top-level load shedding is enabled
    If both are enabled at different percentages, load shedding will not be perfectly calculable due
    to salting of hash (i.e. 25% load shed for Product x + 25% load shed for overall does not
    result in 50% load shed for x)
     */
    val keyTyped = s"${keyPrefix}_${endpointName}_$product"
    val keyTopLevel = s"${keyPrefix}_${endpointName}"

    if (deciderWithFalseFallback.isAvailable(keyTopLevel, recipient = Some(RandomRecipient))) {
      scopedStats.counter(keyTopLevel).incr
      Future.exception(LoadSheddingException)
    } else if (deciderWithFalseFallback.isAvailable(keyTyped, recipient = Some(RandomRecipient))) {
      scopedStats.counter(keyTyped).incr
      Future.exception(LoadSheddingException)
    } else serve
  }
}

object EndpointLoadShedder {
  object LoadSheddingException extends Exception with NoStackTrace
}
package com.twitter.cr_mixer.param.decider

import com.twitter.servo.decider.DeciderKeyEnum

object DeciderConstants {
  val enableHealthSignalsScoreDeciderKey = "enable_tweet_health_score"
  val enableUTGRealTimeTweetEngagementScoreDeciderKey = "enable_utg_realtime_tweet_engagement_score"
  val enableUserAgathaScoreDeciderKey = "enable_user_agatha_score"
  val enableUserTweetEntityGraphTrafficDeciderKey = "enable_user_tweet_entity_graph_traffic"
  val enableUserTweetGraphTrafficDeciderKey = "enable_user_tweet_graph_traffic"
  val enableUserVideoGraphTrafficDeciderKey = "enable_user_video_graph_traffic"
  val enableUserAdGraphTrafficDeciderKey = "enable_user_ad_graph_traffic"
  val enableSimClustersANN2DarkTrafficDeciderKey = "enable_simclusters_ann_2_dark_traffic"
  val enableQigSimilarTweetsTrafficDeciderKey = "enable_qig_similar_tweets_traffic"
  val enableFRSTrafficDeciderKey = "enable_frs_traffic"
  val upperFunnelPerStepScribeRate = "upper_funnel_per_step_scribe_rate"
  val kafkaMessageScribeSampleRate = "kafka_message_scribe_sample_rate"
  val enableRealGraphMhStoreDeciderKey = "enable_real_graph_mh_store"
  val topLevelApiDdgMetricsScribeRate = "top_level_api_ddg_metrics_scribe_rate"
  val adsRecommendationsPerExperimentScribeRate = "ads_recommendations_per_experiment_scribe_rate"
  val enableScribeForBlueVerifiedTweetCandidates =
    "enable_scribe_for_blue_verified_tweet_candidates"

  val enableUserStateStoreDeciderKey = "enable_user_state_store"
  val enableUserMediaRepresentationStoreDeciderKey =
    "enable_user_media_representation_store"
  val enableMagicRecsRealTimeAggregatesStoreDeciderKey =
    "enable_magic_recs_real_time_aggregates_store"

  val enableEarlybirdTrafficDeciderKey = "enable_earlybird_traffic"

  val enableTopicTweetTrafficDeciderKey = "enable_topic_tweet_traffic"

  val getTweetRecommendationsCacheRate = "get_tweet_recommendations_cache_rate"
}

object DeciderKey extends DeciderKeyEnum {

  val enableHealthSignalsScoreDeciderKey: Value = Value(
    DeciderConstants.enableHealthSignalsScoreDeciderKey
  )

  val enableUtgRealTimeTweetEngagementScoreDeciderKey: Value = Value(
    DeciderConstants.enableUTGRealTimeTweetEngagementScoreDeciderKey
  )
  val enableUserAgathaScoreDeciderKey: Value = Value(
    DeciderConstants.enableUserAgathaScoreDeciderKey
  )
  val enableUserMediaRepresentationStoreDeciderKey: Value = Value(
    DeciderConstants.enableUserMediaRepresentationStoreDeciderKey
  )

  val enableMagicRecsRealTimeAggregatesStore: Value = Value(
    DeciderConstants.enableMagicRecsRealTimeAggregatesStoreDeciderKey
  )

  val enableUserStateStoreDeciderKey: Value = Value(
    DeciderConstants.enableUserStateStoreDeciderKey
  )

  val enableRealGraphMhStoreDeciderKey: Value = Value(
    DeciderConstants.enableRealGraphMhStoreDeciderKey
  )

  val enableEarlybirdTrafficDeciderKey: Value = Value(
    DeciderConstants.enableEarlybirdTrafficDeciderKey)
}
package com.twitter.cr_mixer.param.decider

import com.twitter.decider.Decider
import com.twitter.decider.RandomRecipient
import com.twitter.decider.Recipient
import com.twitter.decider.SimpleRecipient
import com.twitter.simclusters_v2.common.DeciderGateBuilderWithIdHashing
import javax.inject.Inject

case class CrMixerDecider @Inject() (decider: Decider) {

  def isAvailable(feature: String, recipient: Option[Recipient]): Boolean = {
    decider.isAvailable(feature, recipient)
  }

  lazy val deciderGateBuilder = new DeciderGateBuilderWithIdHashing(decider)

  /**
   * When useRandomRecipient is set to false, the decider is either completely on or off.
   * When useRandomRecipient is set to true, the decider is on for the specified % of traffic.
   */
  def isAvailable(feature: String, useRandomRecipient: Boolean = true): Boolean = {
    if (useRandomRecipient) isAvailable(feature, Some(RandomRecipient))
    else isAvailable(feature, None)
  }

  /***
   * Decide whether the decider is available for a specific id using SimpleRecipient(id).
   */
  def isAvailableForId(
    id: Long,
    deciderConstants: String
  ): Boolean = {
    // Note: SimpleRecipient does expose a `val isUser = true` field which is not correct if the Id is not a user Id.
    // However this field does not appear to be used anywhere in source.
    decider.isAvailable(deciderConstants, Some(SimpleRecipient(id)))
  }

}
package com.twitter.cr_mixer.model

import com.twitter.simclusters_v2.common.TweetId
import com.twitter.recos.recos_common.thriftscala.SocialProofType

/***
 * Bind a tweetId with a raw score and social proofs by type
 */
case class TweetWithScoreAndSocialProof(
  tweetId: TweetId,
  score: Double,
  socialProofByType: Map[SocialProofType, Seq[Long]])
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object RecentNotificationsParams {
  object EnableSourceParam
      extends FSParam[Boolean](
        name = "twistly_recentnotifications_enable_source",
        default = false
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
package com.twitter.cr_mixer.param

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSEnumParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param
import com.twitter.usersignalservice.thriftscala.SignalType

object GoodProfileClickParams {

  object ClickMinDwellTimeParam extends Enumeration {
    protected case class SignalTypeValue(signalType: SignalType) extends super.Val
    import scala.language.implicitConversions
    implicit def valueToSignalTypeValue(x: Value): SignalTypeValue =
      x.asInstanceOf[SignalTypeValue]

    val TotalDwellTime10s = SignalTypeValue(SignalType.GoodProfileClick)
    val TotalDwellTime20s = SignalTypeValue(SignalType.GoodProfileClick20s)
    val TotalDwellTime30s = SignalTypeValue(SignalType.GoodProfileClick30s)

  }

  object EnableSourceParam
      extends FSParam[Boolean](
        name = "signal_good_profile_clicks_enable_source",
        default = false
      )

  object ClickMinDwellTimeType
      extends FSEnumParam[ClickMinDwellTimeParam.type](
        name = "signal_good_profile_clicks_min_dwelltime_type_id",
        default = ClickMinDwellTimeParam.TotalDwellTime10s,
        enum = ClickMinDwellTimeParam
      )

  val AllParams: Seq[Param[_] with FSName] =
    Seq(EnableSourceParam, ClickMinDwellTimeType)

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam
    )

    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
      ClickMinDwellTimeType
    )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(enumOverrides: _*)
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
import com.twitter.follow_recommendations.thriftscala.DisplayLocation
import com.twitter.timelines.configapi.FSEnumParam
import com.twitter.logging.Logger
import com.twitter.finagle.stats.NullStatsReceiver

object FrsParams {
  object EnableSourceParam
      extends FSParam[Boolean](
        name = "signal_frs_enable_source",
        default = false
      )

  object EnableSourceGraphParam
      extends FSParam[Boolean](
        name = "graph_frs_enable_source",
        default = false
      )

  object MinScoreParam
      extends FSBoundedParam[Double](
        name = "signal_frs_min_score",
        default = 0.4,
        min = 0.0,
        max = 1.0
      )

  object MaxConsumerSeedsNumParam
      extends FSBoundedParam[Int](
        name = "graph_frs_max_user_seeds_num",
        default = 200,
        min = 0,
        max = 1000
      )

  /**
   * These params below are only used for FrsTweetCandidateGenerator and shouldn't be used in other endpoints
   *    * FrsBasedCandidateGenerationMaxSeedsNumParam
   *    * FrsCandidateGenerationDisplayLocationParam
   *    * FrsCandidateGenerationDisplayLocation
   *    * FrsBasedCandidateGenerationMaxCandidatesNumParam
   */
  object FrsBasedCandidateGenerationEnableVisibilityFilteringParam
      extends FSParam[Boolean](
        name = "frs_based_candidate_generation_enable_vf",
        default = true
      )

  object FrsBasedCandidateGenerationMaxSeedsNumParam
      extends FSBoundedParam[Int](
        name = "frs_based_candidate_generation_max_seeds_num",
        default = 100,
        min = 0,
        max = 800
      )

  object FrsBasedCandidateGenerationDisplayLocation extends Enumeration {
    protected case class FrsDisplayLocationValue(displayLocation: DisplayLocation) extends super.Val
    import scala.language.implicitConversions
    implicit def valueToDisplayLocationValue(x: Value): FrsDisplayLocationValue =
      x.asInstanceOf[FrsDisplayLocationValue]

    val DisplayLocation_ContentRecommender: FrsDisplayLocationValue = FrsDisplayLocationValue(
      DisplayLocation.ContentRecommender)
    val DisplayLocation_Home: FrsDisplayLocationValue = FrsDisplayLocationValue(
      DisplayLocation.HomeTimelineTweetRecs)
    val DisplayLocation_Notifications: FrsDisplayLocationValue = FrsDisplayLocationValue(
      DisplayLocation.TweetNotificationRecs)
  }

  object FrsBasedCandidateGenerationDisplayLocationParam
      extends FSEnumParam[FrsBasedCandidateGenerationDisplayLocation.type](
        name = "frs_based_candidate_generation_display_location_id",
        default = FrsBasedCandidateGenerationDisplayLocation.DisplayLocation_Home,
        enum = FrsBasedCandidateGenerationDisplayLocation
      )

  object FrsBasedCandidateGenerationMaxCandidatesNumParam
      extends FSBoundedParam[Int](
        name = "frs_based_candidate_generation_max_candidates_num",
        default = 100,
        min = 0,
        max = 2000
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableSourceParam,
    EnableSourceGraphParam,
    MinScoreParam,
    MaxConsumerSeedsNumParam,
    FrsBasedCandidateGenerationMaxSeedsNumParam,
    FrsBasedCandidateGenerationDisplayLocationParam,
    FrsBasedCandidateGenerationMaxCandidatesNumParam,
    FrsBasedCandidateGenerationEnableVisibilityFilteringParam
  )

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam,
      EnableSourceGraphParam,
      FrsBasedCandidateGenerationEnableVisibilityFilteringParam
    )

    val doubleOverrides = FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(MinScoreParam)

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MaxConsumerSeedsNumParam,
      FrsBasedCandidateGenerationMaxSeedsNumParam,
      FrsBasedCandidateGenerationMaxCandidatesNumParam)

    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
      FrsBasedCandidateGenerationDisplayLocationParam,
    )
    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .set(intOverrides: _*)
      .set(enumOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object RecentNegativeSignalParams {
  object EnableSourceParam
      extends FSParam[Boolean](
        name = "twistly_recentnegativesignals_enable_source",
        default = false
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableSourceParam
  )

  lazy val config: BaseConfig = {
    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
    )

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam
    )

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides()

    BaseConfigBuilder()
      .set(booleanOverrides: _*).set(doubleOverrides: _*).set(enumOverrides: _*).build()
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.storehaus.ReadableStore
import com.twitter.strato.client.{Client => StratoClient}
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.source_signal.UssStore
import com.twitter.cr_mixer.source_signal.UssStore.Query
import com.twitter.frigate.common.store.strato.StratoFetchableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.usersignalservice.thriftscala.BatchSignalRequest
import com.twitter.usersignalservice.thriftscala.BatchSignalResponse
import com.twitter.usersignalservice.thriftscala.SignalType
import com.twitter.usersignalservice.thriftscala.{Signal => UssSignal}
import javax.inject.Named

object UserSignalServiceStoreModule extends TwitterModule {

  private val UssColumnPath = "recommendations/user-signal-service/signals"

  @Provides
  @Singleton
  @Named(ModuleNames.UssStore)
  def providesUserSignalServiceStore(
    statsReceiver: StatsReceiver,
    stratoClient: StratoClient,
  ): ReadableStore[Query, Seq[(SignalType, Seq[UssSignal])]] = {
    ObservedReadableStore(
      UssStore(
        StratoFetchableStore
          .withUnitView[BatchSignalRequest, BatchSignalResponse](stratoClient, UssColumnPath),
        statsReceiver))(statsReceiver.scope("user_signal_service_store"))
  }
}
