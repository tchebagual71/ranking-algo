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
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.EarlybirdClientId
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.FacetsToFetch
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.GetCollectorTerminationParams
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.GetEarlybirdQuery
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.MetadataOptions
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.SeqLongInjection
import com.twitter.hashing.KeyHasher
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.search.common.query.thriftjava.thriftscala.CollectorParams
import com.twitter.search.earlybird.thriftscala.EarlybirdRequest
import com.twitter.search.earlybird.thriftscala.EarlybirdResponseCode
import com.twitter.search.earlybird.thriftscala.EarlybirdService
import com.twitter.search.earlybird.thriftscala.ThriftSearchQuery
import com.twitter.search.earlybird.thriftscala.ThriftSearchRankingMode
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Duration
import com.twitter.util.Future
import javax.inject.Named

object EarlybirdRecencyBasedCandidateStoreModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.EarlybirdRecencyBasedWithoutRetweetsRepliesTweetsCache)
  def providesEarlybirdRecencyBasedWithoutRetweetsRepliesCandidateStore(
    statsReceiver: StatsReceiver,
    earlybirdSearchClient: EarlybirdService.MethodPerEndpoint,
    @Named(ModuleNames.EarlybirdTweetsCache) earlybirdRecencyBasedTweetsCache: MemcachedClient,
    timeoutConfig: TimeoutConfig
  ): ReadableStore[UserId, Seq[TweetId]] = {
    val stats = statsReceiver.scope("EarlybirdRecencyBasedWithoutRetweetsRepliesCandidateStore")
    val underlyingStore = new ReadableStore[UserId, Seq[TweetId]] {
      override def get(userId: UserId): Future[Option[Seq[TweetId]]] = {
        // Home based EB filters out retweets and replies
        val earlybirdRequest =
          buildEarlybirdRequest(
            userId,
            FilterOutRetweetsAndReplies,
            DefaultMaxNumTweetPerUser,
            timeoutConfig.earlybirdServerTimeout)
        getEarlybirdSearchResult(earlybirdSearchClient, earlybirdRequest, stats)
      }
    }
    ObservedMemcachedReadableStore.fromCacheClient(
      backingStore = underlyingStore,
      cacheClient = earlybirdRecencyBasedTweetsCache,
      ttl = MemcacheKeyTimeToLiveDuration,
      asyncUpdate = true
    )(
      valueInjection = SeqLongInjection,
      statsReceiver = statsReceiver.scope("earlybird_recency_based_tweets_home_memcache"),
      keyToString = { k =>
        f"uEBRBHM:${keyHasher.hashKey(k.toString.getBytes)}%X" // prefix = EarlyBirdRecencyBasedHoMe
      }
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.EarlybirdRecencyBasedWithRetweetsRepliesTweetsCache)
  def providesEarlybirdRecencyBasedWithRetweetsRepliesCandidateStore(
    statsReceiver: StatsReceiver,
    earlybirdSearchClient: EarlybirdService.MethodPerEndpoint,
    @Named(ModuleNames.EarlybirdTweetsCache) earlybirdRecencyBasedTweetsCache: MemcachedClient,
    timeoutConfig: TimeoutConfig
  ): ReadableStore[UserId, Seq[TweetId]] = {
    val stats = statsReceiver.scope("EarlybirdRecencyBasedWithRetweetsRepliesCandidateStore")
    val underlyingStore = new ReadableStore[UserId, Seq[TweetId]] {
      override def get(userId: UserId): Future[Option[Seq[TweetId]]] = {
        val earlybirdRequest = buildEarlybirdRequest(
          userId,
          // Notifications based EB keeps retweets and replies
          NotFilterOutRetweetsAndReplies,
          DefaultMaxNumTweetPerUser,
          processingTimeout = timeoutConfig.earlybirdServerTimeout
        )
        getEarlybirdSearchResult(earlybirdSearchClient, earlybirdRequest, stats)
      }
    }
    ObservedMemcachedReadableStore.fromCacheClient(
      backingStore = underlyingStore,
      cacheClient = earlybirdRecencyBasedTweetsCache,
      ttl = MemcacheKeyTimeToLiveDuration,
      asyncUpdate = true
    )(
      valueInjection = SeqLongInjection,
      statsReceiver = statsReceiver.scope("earlybird_recency_based_tweets_notifications_memcache"),
      keyToString = { k =>
        f"uEBRBN:${keyHasher.hashKey(k.toString.getBytes)}%X" // prefix = EarlyBirdRecencyBasedNotifications
      }
    )
  }

  private val keyHasher: KeyHasher = KeyHasher.FNV1A_64

  /**
   * Note the DefaultMaxNumTweetPerUser is used to adjust the result size per cache entry.
   * If the value changes, it will increase the size of the memcache.
   */
  private val DefaultMaxNumTweetPerUser: Int = 100
  private val FilterOutRetweetsAndReplies = true
  private val NotFilterOutRetweetsAndReplies = false
  private val MemcacheKeyTimeToLiveDuration: Duration = Duration.fromMinutes(15)

  private def buildEarlybirdRequest(
    seedUserId: UserId,
    filterOutRetweetsAndReplies: Boolean,
    maxNumTweetsPerSeedUser: Int,
    processingTimeout: Duration
  ): EarlybirdRequest =
    EarlybirdRequest(
      searchQuery = getThriftSearchQuery(
        seedUserId = seedUserId,
        filterOutRetweetsAndReplies = filterOutRetweetsAndReplies,
        maxNumTweetsPerSeedUser = maxNumTweetsPerSeedUser,
        processingTimeout = processingTimeout
      ),
      clientId = Some(EarlybirdClientId),
      timeoutMs = processingTimeout.inMilliseconds.intValue(),
      getOlderResults = Some(false),
      adjustedProtectedRequestParams = None,
      adjustedFullArchiveRequestParams = None,
      getProtectedTweetsOnly = Some(false),
      skipVeryRecentTweets = true,
    )

  private def getThriftSearchQuery(
    seedUserId: UserId,
    filterOutRetweetsAndReplies: Boolean,
    maxNumTweetsPerSeedUser: Int,
    processingTimeout: Duration
  ): ThriftSearchQuery = ThriftSearchQuery(
    serializedQuery = GetEarlybirdQuery(
      None,
      None,
      Set.empty,
      filterOutRetweetsAndReplies
    ).map(_.serialize),
    fromUserIDFilter64 = Some(Seq(seedUserId)),
    numResults = maxNumTweetsPerSeedUser,
    rankingMode = ThriftSearchRankingMode.Recency,
    collectorParams = Some(
      CollectorParams(
        // numResultsToReturn defines how many results each EB shard will return to search root
        numResultsToReturn = maxNumTweetsPerSeedUser,
        // terminationParams.maxHitsToProcess is used for early terminating per shard results fetching.
        terminationParams =
          GetCollectorTerminationParams(maxNumTweetsPerSeedUser, processingTimeout)
      )),
    facetFieldNames = Some(FacetsToFetch),
    resultMetadataOptions = Some(MetadataOptions),
    searchStatusIds = None
  )

  private def getEarlybirdSearchResult(
    earlybirdSearchClient: EarlybirdService.MethodPerEndpoint,
    request: EarlybirdRequest,
    statsReceiver: StatsReceiver
  ): Future[Option[Seq[TweetId]]] = earlybirdSearchClient
    .search(request)
    .map { response =>
      response.responseCode match {
        case EarlybirdResponseCode.Success =>
          val earlybirdSearchResult =
            response.searchResults
              .map {
                _.results
                  .map(searchResult => searchResult.id)
              }
          statsReceiver.scope("result").stat("size").add(earlybirdSearchResult.size)
          earlybirdSearchResult
        case e =>
          statsReceiver.scope("failures").counter(e.getClass.getSimpleName).incr()
          Some(Seq.empty)
      }
    }

}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.inject.TwitterModule
import com.twitter.recos.user_ad_graph.thriftscala.ConsumersBasedRelatedAdRequest
import com.twitter.recos.user_ad_graph.thriftscala.RelatedAdResponse
import com.twitter.recos.user_ad_graph.thriftscala.UserAdGraph
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import javax.inject.Named
import javax.inject.Singleton

object ConsumersBasedUserAdGraphStoreModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.ConsumerBasedUserAdGraphStore)
  def providesConsumerBasedUserAdGraphStore(
    userAdGraphService: UserAdGraph.MethodPerEndpoint
  ): ReadableStore[ConsumersBasedRelatedAdRequest, RelatedAdResponse] = {
    new ReadableStore[ConsumersBasedRelatedAdRequest, RelatedAdResponse] {
      override def get(
        k: ConsumersBasedRelatedAdRequest
      ): Future[Option[RelatedAdResponse]] = {
        userAdGraphService.consumersBasedRelatedAds(k).map(Some(_))
      }
    }
  }
}
package com.twitter.cr_mixer.module.core

import com.twitter.inject.TwitterModule
import com.google.inject.Provides
import javax.inject.Singleton
import com.twitter.util.Duration
import com.twitter.app.Flag
import com.twitter.cr_mixer.config.TimeoutConfig

/**
 * All timeout settings in CrMixer.
 * Timeout numbers are defined in source/cr-mixer/server/config/deploy.aurora
 */
object TimeoutConfigModule extends TwitterModule {

  /**
   * Flag names for client timeout
   * These are used in modules extending ThriftMethodBuilderClientModule
   * which cannot accept injection of TimeoutConfig
   */
  val EarlybirdClientTimeoutFlagName = "earlybird.client.timeout"
  val FrsClientTimeoutFlagName = "frsSignalFetch.client.timeout"
  val QigRankerClientTimeoutFlagName = "qigRanker.client.timeout"
  val TweetypieClientTimeoutFlagName = "tweetypie.client.timeout"
  val UserTweetGraphClientTimeoutFlagName = "userTweetGraph.client.timeout"
  val UserTweetGraphPlusClientTimeoutFlagName = "userTweetGraphPlus.client.timeout"
  val UserAdGraphClientTimeoutFlagName = "userAdGraph.client.timeout"
  val UserVideoGraphClientTimeoutFlagName = "userVideoGraph.client.timeout"
  val UtegClientTimeoutFlagName = "uteg.client.timeout"
  val NaviRequestTimeoutFlagName = "navi.client.request.timeout"

  /**
   * Flags for timeouts
   * These are defined and initialized only in this file
   */
  // timeout for the service
  private val serviceTimeout: Flag[Duration] =
    flag("service.timeout", "service total timeout")

  // timeout for signal fetch
  private val signalFetchTimeout: Flag[Duration] =
    flag[Duration]("signalFetch.timeout", "signal fetch timeout")

  // timeout for similarity engine
  private val similarityEngineTimeout: Flag[Duration] =
    flag[Duration]("similarityEngine.timeout", "similarity engine timeout")
  private val annServiceClientTimeout: Flag[Duration] =
    flag[Duration]("annService.client.timeout", "annQueryService client timeout")

  // timeout for user affinities fetcher
  private val userStateUnderlyingStoreTimeout: Flag[Duration] =
    flag[Duration]("userStateUnderlyingStore.timeout", "user state underlying store timeout")

  private val userStateStoreTimeout: Flag[Duration] =
    flag[Duration]("userStateStore.timeout", "user state store timeout")

  private val utegSimilarityEngineTimeout: Flag[Duration] =
    flag[Duration]("uteg.similarityEngine.timeout", "uteg similarity engine timeout")

  private val earlybirdServerTimeout: Flag[Duration] =
    flag[Duration]("earlybird.server.timeout", "earlybird server timeout")

  private val earlybirdSimilarityEngineTimeout: Flag[Duration] =
    flag[Duration]("earlybird.similarityEngine.timeout", "Earlybird similarity engine timeout")

  private val frsBasedTweetEndpointTimeout: Flag[Duration] =
    flag[Duration](
      "frsBasedTweet.endpoint.timeout",
      "frsBasedTweet endpoint timeout"
    )

  private val topicTweetEndpointTimeout: Flag[Duration] =
    flag[Duration](
      "topicTweet.endpoint.timeout",
      "topicTweet endpoint timeout"
    )

  // timeout for Navi client
  private val naviRequestTimeout: Flag[Duration] =
    flag[Duration](
      NaviRequestTimeoutFlagName,
      Duration.fromMilliseconds(2000),
      "Request timeout for a single RPC Call",
    )

  @Provides
  @Singleton
  def provideTimeoutBudget(): TimeoutConfig =
    TimeoutConfig(
      serviceTimeout = serviceTimeout(),
      signalFetchTimeout = signalFetchTimeout(),
      similarityEngineTimeout = similarityEngineTimeout(),
      annServiceClientTimeout = annServiceClientTimeout(),
      utegSimilarityEngineTimeout = utegSimilarityEngineTimeout(),
      userStateUnderlyingStoreTimeout = userStateUnderlyingStoreTimeout(),
      userStateStoreTimeout = userStateStoreTimeout(),
      earlybirdServerTimeout = earlybirdServerTimeout(),
      earlybirdSimilarityEngineTimeout = earlybirdSimilarityEngineTimeout(),
      frsBasedTweetEndpointTimeout = frsBasedTweetEndpointTimeout(),
      topicTweetEndpointTimeout = topicTweetEndpointTimeout(),
      naviRequestTimeout = naviRequestTimeout()
    )

}
package com.twitter.cr_mixer.module.core

import com.google.inject.Provides
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.scribe.ScribeCategories
import com.twitter.cr_mixer.scribe.ScribeCategory
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.logging.BareFormatter
import com.twitter.logging.Level
import com.twitter.logging.Logger
import com.twitter.logging.NullHandler
import com.twitter.logging.QueueingHandler
import com.twitter.logging.ScribeHandler
import com.twitter.logging.{LoggerFactory => TwitterLoggerFactory}
import javax.inject.Named
import javax.inject.Singleton

object LoggerFactoryModule extends TwitterModule {

  private val DefaultQueueSize = 10000

  @Provides
  @Singleton
  @Named(ModuleNames.AbDeciderLogger)
  def provideAbDeciderLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.AbDecider,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.TopLevelApiDdgMetricsLogger)
  def provideTopLevelApiDdgMetricsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.TopLevelApiDdgMetrics,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.TweetRecsLogger)
  def provideTweetRecsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.TweetsRecs,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.BlueVerifiedTweetRecsLogger)
  def provideVITTweetRecsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.VITTweetsRecs,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.RelatedTweetsLogger)
  def provideRelatedTweetsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.RelatedTweets,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.UtegTweetsLogger)
  def provideUtegTweetsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.UtegTweets,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.AdsRecommendationsLogger)
  def provideAdsRecommendationsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.AdsRecommendations,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  private def buildLoggerFactory(
    category: ScribeCategory,
    environment: String,
    statsReceiver: StatsReceiver
  ): TwitterLoggerFactory = {
    environment match {
      case "prod" =>
        TwitterLoggerFactory(
          node = category.getProdLoggerFactoryNode,
          level = Some(Level.INFO),
          useParents = false,
          handlers = List(
            QueueingHandler(
              maxQueueSize = DefaultQueueSize,
              handler = ScribeHandler(
                category = category.scribeCategory,
                formatter = BareFormatter,
                statsReceiver = statsReceiver.scope(category.getProdLoggerFactoryNode)
              )
            )
          )
        )
      case _ =>
        TwitterLoggerFactory(
          node = category.getStagingLoggerFactoryNode,
          level = Some(Level.DEBUG),
          useParents = false,
          handlers = List(
            { () => NullHandler }
          )
        )
    }
  }
}
package com.twitter.cr_mixer.module.core

import com.twitter.inject.TwitterModule

object CrMixerFlagName {
  val SERVICE_FLAG = "cr_mixer.flag"
  val DarkTrafficFilterDeciderKey = "thrift.dark.traffic.filter.decider_key"
}

object CrMixerFlagModule extends TwitterModule {
  import CrMixerFlagName._

  flag[Boolean](name = SERVICE_FLAG, default = false, help = "This is a CR Mixer flag")

  flag[String](
    name = DarkTrafficFilterDeciderKey,
    default = "dark_traffic_filter",
    help = "Dark traffic filter decider key"
  )
}
