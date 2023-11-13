package com.twitter.cr_mixer.module.core

import com.google.inject.Provides
import com.twitter.cr_mixer.featureswitch.CrMixerLoggingABDecider
import com.twitter.featureswitches.v2.FeatureSwitches
import com.twitter.featureswitches.v2.builder.FeatureSwitchesBuilder
import com.twitter.featureswitches.v2.experimentation.NullBucketImpressor
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.inject.annotations.Flag
import com.twitter.util.Duration
import javax.inject.Singleton

object FeatureSwitchesModule extends TwitterModule {

  flag(
    name = "featureswitches.path",
    default = "/features/cr-mixer/main",
    help = "path to the featureswitch configuration directory"
  )
  flag(
    "use_config_repo_mirror.bool",
    false,
    "If true, read config from a different directory, to facilitate testing.")

  val DefaultFastRefresh: Boolean = false
  val AddServiceDetailsFromAurora: Boolean = true
  val ImpressExperiments: Boolean = true

  @Provides
  @Singleton
  def providesFeatureSwitches(
    @Flag("featureswitches.path") featureSwitchDirectory: String,
    @Flag("use_config_repo_mirror.bool") useConfigRepoMirrorFlag: Boolean,
    abDecider: CrMixerLoggingABDecider,
    statsReceiver: StatsReceiver
  ): FeatureSwitches = {
    val configRepoAbsPath =
      getConfigRepoAbsPath(useConfigRepoMirrorFlag)
    val fastRefresh =
      shouldFastRefresh(useConfigRepoMirrorFlag)

    val featureSwitches = FeatureSwitchesBuilder()
      .abDecider(abDecider)
      .statsReceiver(statsReceiver.scope("featureswitches-v2"))
      .configRepoAbsPath(configRepoAbsPath)
      .featuresDirectory(featureSwitchDirectory)
      .limitToReferencedExperiments(shouldLimit = true)
      .experimentImpressionStatsEnabled(true)

    if (!ImpressExperiments) featureSwitches.experimentBucketImpressor(NullBucketImpressor)
    if (AddServiceDetailsFromAurora) featureSwitches.serviceDetailsFromAurora()
    if (fastRefresh) featureSwitches.refreshPeriod(Duration.fromSeconds(10))

    featureSwitches.build()
  }

  private def getConfigRepoAbsPath(
    useConfigRepoMirrorFlag: Boolean
  ): String = {
    if (useConfigRepoMirrorFlag)
      "config_repo_mirror/"
    else "/usr/local/config"
  }

  private def shouldFastRefresh(
    useConfigRepoMirrorFlag: Boolean
  ): Boolean = {
    if (useConfigRepoMirrorFlag)
      true
    else DefaultFastRefresh
  }

}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.bijection.thrift.CompactThriftCodec
import com.twitter.ads.entities.db.thriftscala.LineItemObjective
import com.twitter.bijection.Injection
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.thriftscala.LineItemInfo
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.hermit.store.common.ObservedCachedReadableStore
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.ml.api.DataRecord
import com.twitter.ml.api.DataType
import com.twitter.ml.api.Feature
import com.twitter.ml.api.GeneralTensor
import com.twitter.ml.api.RichDataRecord
import com.twitter.relevance_platform.common.injection.LZ4Injection
import com.twitter.relevance_platform.common.injection.SeqObjectInjection
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.storage.client.manhattan.kv.ManhattanKVClientMtlsParams
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus_internal.manhattan.ManhattanRO
import com.twitter.storehaus_internal.manhattan.ManhattanROConfig
import com.twitter.storehaus_internal.manhattan.Revenue
import com.twitter.storehaus_internal.util.ApplicationID
import com.twitter.storehaus_internal.util.DatasetName
import com.twitter.storehaus_internal.util.HDFSPath
import com.twitter.util.Future
import javax.inject.Named
import scala.collection.JavaConverters._

object ActivePromotedTweetStoreModule extends TwitterModule {

  case class ActivePromotedTweetStore(
    activePromotedTweetMHStore: ReadableStore[String, DataRecord],
    statsReceiver: StatsReceiver)
      extends ReadableStore[TweetId, Seq[LineItemInfo]] {
    override def get(tweetId: TweetId): Future[Option[Seq[LineItemInfo]]] = {
      activePromotedTweetMHStore.get(tweetId.toString).map {
        _.map { dataRecord =>
          val richDataRecord = new RichDataRecord(dataRecord)
          val lineItemIdsFeature: Feature[GeneralTensor] =
            new Feature.Tensor("active_promoted_tweets.line_item_ids", DataType.INT64)

          val lineItemObjectivesFeature: Feature[GeneralTensor] =
            new Feature.Tensor("active_promoted_tweets.line_item_objectives", DataType.INT64)

          val lineItemIdsTensor: GeneralTensor = richDataRecord.getFeatureValue(lineItemIdsFeature)
          val lineItemObjectivesTensor: GeneralTensor =
            richDataRecord.getFeatureValue(lineItemObjectivesFeature)

          val lineItemIds: Seq[Long] =
            if (lineItemIdsTensor.getSetField == GeneralTensor._Fields.INT64_TENSOR && lineItemIdsTensor.getInt64Tensor.isSetLongs) {
              lineItemIdsTensor.getInt64Tensor.getLongs.asScala.map(_.toLong)
            } else Seq.empty

          val lineItemObjectives: Seq[LineItemObjective] =
            if (lineItemObjectivesTensor.getSetField == GeneralTensor._Fields.INT64_TENSOR && lineItemObjectivesTensor.getInt64Tensor.isSetLongs) {
              lineItemObjectivesTensor.getInt64Tensor.getLongs.asScala.map(objective =>
                LineItemObjective(objective.toInt))
            } else Seq.empty

          val lineItemInfo =
            if (lineItemIds.size == lineItemObjectives.size) {
              lineItemIds.zipWithIndex.map {
                case (lineItemId, index) =>
                  LineItemInfo(
                    lineItemId = lineItemId,
                    lineItemObjective = lineItemObjectives(index)
                  )
              }
            } else Seq.empty

          lineItemInfo
        }
      }
    }
  }

  @Provides
  @Singleton
  def providesActivePromotedTweetStore(
    manhattanKVClientMtlsParams: ManhattanKVClientMtlsParams,
    @Named(ModuleNames.UnifiedCache) crMixerUnifiedCacheClient: MemcachedClient,
    crMixerStatsReceiver: StatsReceiver
  ): ReadableStore[TweetId, Seq[LineItemInfo]] = {

    val mhConfig = new ManhattanROConfig {
      val hdfsPath = HDFSPath("")
      val applicationID = ApplicationID("ads_bigquery_features")
      val datasetName = DatasetName("active_promoted_tweets")
      val cluster = Revenue

      override def statsReceiver: StatsReceiver =
        crMixerStatsReceiver.scope("active_promoted_tweets_mh")
    }
    val mhStore: ReadableStore[String, DataRecord] =
      ManhattanRO
        .getReadableStoreWithMtls[String, DataRecord](
          mhConfig,
          manhattanKVClientMtlsParams
        )(
          implicitly[Injection[String, Array[Byte]]],
          CompactThriftCodec[DataRecord]
        )

    val underlyingStore =
      ActivePromotedTweetStore(mhStore, crMixerStatsReceiver.scope("ActivePromotedTweetStore"))
    val memcachedStore = ObservedMemcachedReadableStore.fromCacheClient(
      backingStore = underlyingStore,
      cacheClient = crMixerUnifiedCacheClient,
      ttl = 60.minutes,
      asyncUpdate = false
    )(
      valueInjection = LZ4Injection.compose(SeqObjectInjection[LineItemInfo]()),
      statsReceiver = crMixerStatsReceiver.scope("memCachedActivePromotedTweetStore"),
      keyToString = { k: TweetId => s"apt/$k" }
    )

    ObservedCachedReadableStore.from(
      memcachedStore,
      ttl = 30.minutes,
      maxKeys = 250000, // size of promoted tweet is around 200,000
      windowSize = 10000L,
      cacheName = "active_promoted_tweet_cache",
      maxMultiGetSize = 20
    )(crMixerStatsReceiver.scope("inMemoryCachedActivePromotedTweetStore"))

  }

}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.bijection.Injection
import com.twitter.bijection.scrooge.BinaryScalaCodec
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.thriftscala.TweetsWithScore
import com.twitter.storage.client.manhattan.kv.ManhattanKVClientMtlsParams
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus_internal.manhattan.Apollo
import com.twitter.storehaus_internal.manhattan.ManhattanRO
import com.twitter.storehaus_internal.manhattan.ManhattanROConfig
import com.twitter.storehaus_internal.util.ApplicationID
import com.twitter.storehaus_internal.util.DatasetName
import com.twitter.storehaus_internal.util.HDFSPath
import javax.inject.Named
import javax.inject.Singleton

object DiffusionStoreModule extends TwitterModule {
  type UserId = Long
  implicit val longCodec = implicitly[Injection[Long, Array[Byte]]]
  implicit val tweetRecsInjection: Injection[TweetsWithScore, Array[Byte]] =
    BinaryScalaCodec(TweetsWithScore)

  @Provides
  @Singleton
  @Named(ModuleNames.RetweetBasedDiffusionRecsMhStore)
  def retweetBasedDiffusionRecsMhStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[Long, TweetsWithScore] = {
    val manhattanROConfig = ManhattanROConfig(
      HDFSPath(""), // not needed
      ApplicationID("cr_mixer_apollo"),
      DatasetName("diffusion_retweet_tweet_recs"),
      Apollo
    )

    buildTweetRecsStore(serviceIdentifier, manhattanROConfig)
  }

  private def buildTweetRecsStore(
    serviceIdentifier: ServiceIdentifier,
    manhattanROConfig: ManhattanROConfig
  ): ReadableStore[Long, TweetsWithScore] = {

    ManhattanRO
      .getReadableStoreWithMtls[Long, TweetsWithScore](
        manhattanROConfig,
        ManhattanKVClientMtlsParams(serviceIdentifier)
      )(longCodec, tweetRecsInjection)
  }
}
package com.twitter.cr_mixer.module

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.common.SimClustersEmbedding
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.strato.client.{Client => StratoClient}
import com.twitter.representation_manager.thriftscala.SimClustersEmbeddingView
import com.twitter.simclusters_v2.thriftscala.EmbeddingType
import com.twitter.simclusters_v2.thriftscala.ModelVersion
import com.google.inject.Provides
import com.google.inject.Singleton
import javax.inject.Named
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.frigate.common.store.strato.StratoFetchableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.simclusters_v2.thriftscala.{SimClustersEmbedding => ThriftSimClustersEmbedding}

object RepresentationManagerModule extends TwitterModule {
  private val ColPathPrefix = "recommendations/representation_manager/"
  private val SimclustersTweetColPath = ColPathPrefix + "simClustersEmbedding.Tweet"
  private val SimclustersUserColPath = ColPathPrefix + "simClustersEmbedding.User"

  @Provides
  @Singleton
  @Named(ModuleNames.RmsTweetLogFavLongestL2EmbeddingStore)
  def providesRepresentationManagerTweetStore(
    statsReceiver: StatsReceiver,
    stratoClient: StratoClient,
  ): ReadableStore[TweetId, SimClustersEmbedding] = {
    ObservedReadableStore(
      StratoFetchableStore
        .withView[Long, SimClustersEmbeddingView, ThriftSimClustersEmbedding](
          stratoClient,
          SimclustersTweetColPath,
          SimClustersEmbeddingView(
            EmbeddingType.LogFavLongestL2EmbeddingTweet,
            ModelVersion.Model20m145k2020))
        .mapValues(SimClustersEmbedding(_)))(
      statsReceiver.scope("rms_tweet_log_fav_longest_l2_store"))
  }

  @Provides
  @Singleton
  @Named(ModuleNames.RmsUserFavBasedProducerEmbeddingStore)
  def providesRepresentationManagerUserFavBasedProducerEmbeddingStore(
    statsReceiver: StatsReceiver,
    stratoClient: StratoClient,
  ): ReadableStore[UserId, SimClustersEmbedding] = {
    ObservedReadableStore(
      StratoFetchableStore
        .withView[Long, SimClustersEmbeddingView, ThriftSimClustersEmbedding](
          stratoClient,
          SimclustersUserColPath,
          SimClustersEmbeddingView(
            EmbeddingType.FavBasedProducer,
            ModelVersion.Model20m145k2020
          )
        )
        .mapValues(SimClustersEmbedding(_)))(
      statsReceiver.scope("rms_user_fav_based_producer_store"))
  }

  @Provides
  @Singleton
  @Named(ModuleNames.RmsUserLogFavInterestedInEmbeddingStore)
  def providesRepresentationManagerUserLogFavConsumerEmbeddingStore(
    statsReceiver: StatsReceiver,
    stratoClient: StratoClient,
  ): ReadableStore[UserId, SimClustersEmbedding] = {
    ObservedReadableStore(
      StratoFetchableStore
        .withView[Long, SimClustersEmbeddingView, ThriftSimClustersEmbedding](
          stratoClient,
          SimclustersUserColPath,
          SimClustersEmbeddingView(
            EmbeddingType.LogFavBasedUserInterestedIn,
            ModelVersion.Model20m145k2020
          )
        )
        .mapValues(SimClustersEmbedding(_)))(
      statsReceiver.scope("rms_user_log_fav_interestedin_store"))
  }

  @Provides
  @Singleton
  @Named(ModuleNames.RmsUserFollowInterestedInEmbeddingStore)
  def providesRepresentationManagerUserFollowInterestedInEmbeddingStore(
    statsReceiver: StatsReceiver,
    stratoClient: StratoClient,
  ): ReadableStore[UserId, SimClustersEmbedding] = {
    ObservedReadableStore(
      StratoFetchableStore
        .withView[Long, SimClustersEmbeddingView, ThriftSimClustersEmbedding](
          stratoClient,
          SimclustersUserColPath,
          SimClustersEmbeddingView(
            EmbeddingType.FollowBasedUserInterestedIn,
            ModelVersion.Model20m145k2020
          )
        )
        .mapValues(SimClustersEmbedding(_)))(
      statsReceiver.scope("rms_user_follow_interestedin_store"))
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.app.Flag
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.storehaus_internal.memcache.MemcacheStore
import com.twitter.storehaus_internal.util.ClientName
import com.twitter.storehaus_internal.util.ZkEndPoint
import javax.inject.Named

object UnifiedCacheClient extends TwitterModule {

  private val TIME_OUT = 20.milliseconds

  val crMixerUnifiedCacheDest: Flag[String] = flag[String](
    name = "crMixer.unifiedCacheDest",
    default = "/s/cache/content_recommender_unified_v2",
    help = "Wily path to Content Recommender unified cache"
  )

  val tweetRecommendationResultsCacheDest: Flag[String] = flag[String](
    name = "tweetRecommendationResults.CacheDest",
    default = "/s/cache/tweet_recommendation_results",
    help = "Wily path to CrMixer getTweetRecommendations() results cache"
  )

  val earlybirdTweetsCacheDest: Flag[String] = flag[String](
    name = "earlybirdTweets.CacheDest",
    default = "/s/cache/crmixer_earlybird_tweets",
    help = "Wily path to CrMixer Earlybird Recency Based Similarity Engine result cache"
  )

  @Provides
  @Singleton
  @Named(ModuleNames.UnifiedCache)
  def provideUnifiedCacheClient(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver,
  ): Client =
    MemcacheStore.memcachedClient(
      name = ClientName("memcache-content-recommender-unified"),
      dest = ZkEndPoint(crMixerUnifiedCacheDest()),
      statsReceiver = statsReceiver.scope("cache_client"),
      serviceIdentifier = serviceIdentifier,
      timeout = TIME_OUT
    )

  @Provides
  @Singleton
  @Named(ModuleNames.TweetRecommendationResultsCache)
  def providesTweetRecommendationResultsCache(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver,
  ): Client =
    MemcacheStore.memcachedClient(
      name = ClientName("memcache-tweet-recommendation-results"),
      dest = ZkEndPoint(tweetRecommendationResultsCacheDest()),
      statsReceiver = statsReceiver.scope("cache_client"),
      serviceIdentifier = serviceIdentifier,
      timeout = TIME_OUT
    )

  @Provides
  @Singleton
  @Named(ModuleNames.EarlybirdTweetsCache)
  def providesEarlybirdTweetsCache(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver,
  ): Client =
    MemcacheStore.memcachedClient(
      name = ClientName("memcache-crmixer-earlybird-tweets"),
      dest = ZkEndPoint(earlybirdTweetsCacheDest()),
      statsReceiver = statsReceiver.scope("cache_client"),
      serviceIdentifier = serviceIdentifier,
      timeout = TIME_OUT
    )
}
package com.twitter.cr_mixer.model

object HealthThreshold {
  object Enum extends Enumeration {
    val Off: Value = Value(1)
    val Moderate: Value = Value(2)
    val Strict: Value = Value(3)
    val Stricter: Value = Value(4)
    val StricterPlus: Value = Value(5)
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
import com.twitter.frigate.common.store.strato.StratoFetchableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.usersignalservice.thriftscala.BatchSignalRequest
import com.twitter.usersignalservice.thriftscala.BatchSignalResponse
import javax.inject.Named

object UserSignalServiceColumnModule extends TwitterModule {
  private val UssColumnPath = "recommendations/user-signal-service/signals"

  @Provides
  @Singleton
  @Named(ModuleNames.UssStratoColumn)
  def providesUserSignalServiceStore(
    statsReceiver: StatsReceiver,
    stratoClient: StratoClient,
  ): ReadableStore[BatchSignalRequest, BatchSignalResponse] = {
    ObservedReadableStore(
      StratoFetchableStore
        .withUnitView[BatchSignalRequest, BatchSignalResponse](stratoClient, UssColumnPath))(
      statsReceiver.scope("user_signal_service_store"))
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.bijection.Injection
import com.twitter.bijection.scrooge.CompactScalaCodec
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.thriftscala.CandidateTweetsList
import com.twitter.storage.client.manhattan.kv.ManhattanKVClientMtlsParams
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus_internal.manhattan.Apollo
import com.twitter.storehaus_internal.manhattan.ManhattanRO
import com.twitter.storehaus_internal.manhattan.ManhattanROConfig
import com.twitter.storehaus_internal.util.ApplicationID
import com.twitter.storehaus_internal.util.DatasetName
import com.twitter.storehaus_internal.util.HDFSPath
import javax.inject.Named
import javax.inject.Singleton

object OfflineCandidateStoreModule extends TwitterModule {
  type UserId = Long
  implicit val tweetCandidatesInjection: Injection[CandidateTweetsList, Array[Byte]] =
    CompactScalaCodec(CandidateTweetsList)

  @Provides
  @Singleton
  @Named(ModuleNames.OfflineTweet2020CandidateStore)
  def offlineTweet2020CandidateMhStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[UserId, CandidateTweetsList] = {
    buildOfflineCandidateStore(
      serviceIdentifier,
      datasetName = "offline_tweet_recommendations_from_interestedin_2020"
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.OfflineTweet2020Hl0El15CandidateStore)
  def offlineTweet2020Hl0El15CandidateMhStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[UserId, CandidateTweetsList] = {
    buildOfflineCandidateStore(
      serviceIdentifier,
      datasetName = "offline_tweet_recommendations_from_interestedin_2020_hl_0_el_15"
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.OfflineTweet2020Hl2El15CandidateStore)
  def offlineTweet2020Hl2El15CandidateMhStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[UserId, CandidateTweetsList] = {
    buildOfflineCandidateStore(
      serviceIdentifier,
      datasetName = "offline_tweet_recommendations_from_interestedin_2020_hl_2_el_15"
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.OfflineTweet2020Hl2El50CandidateStore)
  def offlineTweet2020Hl2El50CandidateMhStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[UserId, CandidateTweetsList] = {
    buildOfflineCandidateStore(
      serviceIdentifier,
      datasetName = "offline_tweet_recommendations_from_interestedin_2020_hl_2_el_50"
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.OfflineTweet2020Hl8El50CandidateStore)
  def offlineTweet2020Hl8El50CandidateMhStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[UserId, CandidateTweetsList] = {
    buildOfflineCandidateStore(
      serviceIdentifier,
      datasetName = "offline_tweet_recommendations_from_interestedin_2020_hl_8_el_50"
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.OfflineTweetMTSCandidateStore)
  def offlineTweetMTSCandidateMhStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[UserId, CandidateTweetsList] = {
    buildOfflineCandidateStore(
      serviceIdentifier,
      datasetName = "offline_tweet_recommendations_from_mts_consumer_embeddings"
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.OfflineFavDecayedSumCandidateStore)
  def offlineFavDecayedSumCandidateStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[UserId, CandidateTweetsList] = {
    buildOfflineCandidateStore(
      serviceIdentifier,
      datasetName = "offline_tweet_recommendations_from_decayed_sum"
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.OfflineFtrAt5Pop1000RankDecay11CandidateStore)
  def offlineFtrAt5Pop1000RankDecay11CandidateStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[UserId, CandidateTweetsList] = {
    buildOfflineCandidateStore(
      serviceIdentifier,
      datasetName = "offline_tweet_recommendations_from_ftrat5_pop1000_rank_decay_1_1"
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.OfflineFtrAt5Pop10000RankDecay11CandidateStore)
  def offlineFtrAt5Pop10000RankDecay11CandidateStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[UserId, CandidateTweetsList] = {
    buildOfflineCandidateStore(
      serviceIdentifier,
      datasetName = "offline_tweet_recommendations_from_ftrat5_pop10000_rank_decay_1_1"
    )
  }

  private def buildOfflineCandidateStore(
    serviceIdentifier: ServiceIdentifier,
    datasetName: String
  ): ReadableStore[UserId, CandidateTweetsList] = {
    ManhattanRO
      .getReadableStoreWithMtls[Long, CandidateTweetsList](
        ManhattanROConfig(
          HDFSPath(""), // not needed
          ApplicationID("multi_type_simclusters"),
          DatasetName(datasetName),
          Apollo
        ),
        ManhattanKVClientMtlsParams(serviceIdentifier)
      )
  }

}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.app.Flag
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.store.strato.StratoFetchableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.storehaus.ReadableStore
import com.twitter.strato.client.{Client => StratoClient}
import com.twitter.twistly.thriftscala.TweetRecentEngagedUsers

object TweetRecentEngagedUserStoreModule extends TwitterModule {

  private val tweetRecentEngagedUsersStoreDefaultVersion =
    0 // DefaultVersion for tweetEngagedUsersStore, whose key = (tweetId, DefaultVersion)
  private val tweetRecentEngagedUsersColumnPath: Flag[String] = flag[String](
    name = "crMixer.tweetRecentEngagedUsersColumnPath",
    default = "recommendations/twistly/tweetRecentEngagedUsers",
    help = "Strato column path for TweetRecentEngagedUsersStore"
  )
  private type Version = Long

  @Provides
  @Singleton
  def providesTweetRecentEngagedUserStore(
    statsReceiver: StatsReceiver,
    stratoClient: StratoClient,
  ): ReadableStore[TweetId, TweetRecentEngagedUsers] = {
    val tweetRecentEngagedUsersStratoFetchableStore = StratoFetchableStore
      .withUnitView[(TweetId, Version), TweetRecentEngagedUsers](
        stratoClient,
        tweetRecentEngagedUsersColumnPath()).composeKeyMapping[TweetId](tweetId =>
        (tweetId, tweetRecentEngagedUsersStoreDefaultVersion))

    ObservedReadableStore(
      tweetRecentEngagedUsersStratoFetchableStore
    )(statsReceiver.scope("tweet_recent_engaged_users_store"))
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.inject.TwitterModule
import com.twitter.recos.user_video_graph.thriftscala.ConsumersBasedRelatedTweetRequest
import com.twitter.recos.user_video_graph.thriftscala.RelatedTweetResponse
import com.twitter.recos.user_video_graph.thriftscala.UserVideoGraph
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import javax.inject.Named
import javax.inject.Singleton

object ConsumersBasedUserVideoGraphStoreModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.ConsumerBasedUserVideoGraphStore)
  def providesConsumerBasedUserVideoGraphStore(
    userVideoGraphService: UserVideoGraph.MethodPerEndpoint
  ): ReadableStore[ConsumersBasedRelatedTweetRequest, RelatedTweetResponse] = {
    new ReadableStore[ConsumersBasedRelatedTweetRequest, RelatedTweetResponse] {
      override def get(
        k: ConsumersBasedRelatedTweetRequest
      ): Future[Option[RelatedTweetResponse]] = {
        userVideoGraphService.consumersBasedRelatedTweets(k).map(Some(_))
      }
    }
  }
}
