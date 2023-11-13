package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.keyHasher
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.hermit.store.common.ObservedCachedReadableStore
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.relevance_platform.common.injection.LZ4Injection
import com.twitter.relevance_platform.common.injection.SeqObjectInjection
import com.twitter.simclusters_v2.thriftscala.TopicId
import com.twitter.storehaus.ReadableStore
import com.twitter.strato.client.Client
import com.twitter.topic_recos.stores.CertoTopicTopKTweetsStore
import com.twitter.topic_recos.thriftscala.TweetWithScores

object CertoStratoStoreModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.CertoStratoStoreName)
  def providesCertoStratoStore(
    @Named(ModuleNames.UnifiedCache) crMixerUnifiedCacheClient: MemcachedClient,
    stratoClient: Client,
    statsReceiver: StatsReceiver
  ): ReadableStore[TopicId, Seq[TweetWithScores]] = {
    val certoStore = ObservedReadableStore(CertoTopicTopKTweetsStore.prodStore(stratoClient))(
      statsReceiver.scope(ModuleNames.CertoStratoStoreName)).mapValues { topKTweetsWithScores =>
      topKTweetsWithScores.topTweetsByFollowerL2NormalizedCosineSimilarityScore
    }

    val memCachedStore = ObservedMemcachedReadableStore
      .fromCacheClient(
        backingStore = certoStore,
        cacheClient = crMixerUnifiedCacheClient,
        ttl = 10.minutes
      )(
        valueInjection = LZ4Injection.compose(SeqObjectInjection[TweetWithScores]()),
        statsReceiver = statsReceiver.scope("memcached_certo_store"),
        keyToString = { k => s"certo:${keyHasher.hashKey(k.toString.getBytes)}" }
      )

    ObservedCachedReadableStore.from[TopicId, Seq[TweetWithScores]](
      memCachedStore,
      ttl = 5.minutes,
      maxKeys = 100000, // ~150MB max
      cacheName = "certo_in_memory_cache",
      windowSize = 10000L
    )(statsReceiver.scope("certo_in_memory_cache"))
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.bijection.Bufferable
import com.twitter.bijection.Injection
import com.twitter.bijection.scrooge.BinaryScalaCodec
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.common.UserId
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.storage.client.manhattan.kv.ManhattanKVClientMtlsParams
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus_internal.manhattan.ManhattanRO
import com.twitter.storehaus_internal.manhattan.ManhattanROConfig
import com.twitter.storehaus_internal.util.HDFSPath
import com.twitter.core_workflows.user_model.thriftscala.UserState
import com.twitter.core_workflows.user_model.thriftscala.CondensedUserState
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderKey
import com.twitter.hermit.store.common.DeciderableReadableStore
import com.twitter.storehaus_internal.manhattan.Apollo
import com.twitter.storehaus_internal.util.ApplicationID
import com.twitter.storehaus_internal.util.DatasetName
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.JavaTimer
import com.twitter.util.Time
import com.twitter.util.TimeoutException
import com.twitter.util.Timer
import javax.inject.Named

object UserStateStoreModule extends TwitterModule {
  implicit val timer: Timer = new JavaTimer(true)
  final val NewUserCreateDaysThreshold = 7
  final val DefaultUnknownUserStateValue = 100

  // Convert CondensedUserState to UserState Enum
  // If CondensedUserState is None, back fill by checking whether the user is new user
  class UserStateStore(
    userStateStore: ReadableStore[UserId, CondensedUserState],
    timeout: Duration,
    statsReceiver: StatsReceiver)
      extends ReadableStore[UserId, UserState] {
    override def get(userId: UserId): Future[Option[UserState]] = {
      userStateStore
        .get(userId).map(_.flatMap(_.userState)).map {
          case Some(userState) => Some(userState)
          case None =>
            val isNewUser = SnowflakeId.timeFromIdOpt(userId).exists { userCreateTime =>
              Time.now - userCreateTime < Duration.fromDays(NewUserCreateDaysThreshold)
            }
            if (isNewUser) Some(UserState.New)
            else Some(UserState.EnumUnknownUserState(DefaultUnknownUserStateValue))

        }.raiseWithin(timeout)(timer).rescue {
          case _: TimeoutException =>
            statsReceiver.counter("TimeoutException").incr()
            Future.None
        }
    }
  }

  @Provides
  @Singleton
  def providesUserStateStore(
    crMixerDecider: CrMixerDecider,
    statsReceiver: StatsReceiver,
    manhattanKVClientMtlsParams: ManhattanKVClientMtlsParams,
    @Named(ModuleNames.UnifiedCache) crMixerUnifiedCacheClient: MemcachedClient,
    timeoutConfig: TimeoutConfig
  ): ReadableStore[UserId, UserState] = {

    val underlyingStore = new UserStateStore(
      ManhattanRO
        .getReadableStoreWithMtls[UserId, CondensedUserState](
          ManhattanROConfig(
            HDFSPath(""),
            ApplicationID("cr_mixer_apollo"),
            DatasetName("condensed_user_state"),
            Apollo),
          manhattanKVClientMtlsParams
        )(
          implicitly[Injection[Long, Array[Byte]]],
          BinaryScalaCodec(CondensedUserState)
        ),
      timeoutConfig.userStateStoreTimeout,
      statsReceiver.scope("UserStateStore")
    ).mapValues(_.value) // Read the value of Enum so that we only caches the Int

    val memCachedStore = ObservedMemcachedReadableStore
      .fromCacheClient(
        backingStore = underlyingStore,
        cacheClient = crMixerUnifiedCacheClient,
        ttl = 24.hours,
      )(
        valueInjection = Bufferable.injectionOf[Int], // Cache Value is Enum Value for UserState
        statsReceiver = statsReceiver.scope("memCachedUserStateStore"),
        keyToString = { k: UserId => s"uState/$k" }
      ).mapValues(value => UserState.getOrUnknown(value))

    DeciderableReadableStore(
      memCachedStore,
      crMixerDecider.deciderGateBuilder.idGate(DeciderKey.enableUserStateStoreDeciderKey),
      statsReceiver.scope("UserStateStore")
    )
  }
}
package com.twitter.cr_mixer.module.grpc_client

import com.google.inject.Provides
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.finagle.Http
import com.twitter.finagle.grpc.FinagleChannelBuilder
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.finagle.mtls.client.MtlsStackClient.MtlsStackClientSyntax
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.util.Duration
import io.grpc.ManagedChannel
import javax.inject.Named
import javax.inject.Singleton

object NaviGRPCClientModule extends TwitterModule {

  val maxRetryAttempts = 3

  @Provides
  @Singleton
  @Named(ModuleNames.HomeNaviGRPCClient)
  def providesHomeNaviGRPCClient(
    serviceIdentifier: ServiceIdentifier,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
  ): ManagedChannel = {
    val label = "navi-wals-recommended-tweets-home-client"
    val dest = "/s/ads-prediction/navi-wals-recommended-tweets-home"
    buildClient(serviceIdentifier, timeoutConfig, statsReceiver, dest, label)
  }

  @Provides
  @Singleton
  @Named(ModuleNames.AdsFavedNaviGRPCClient)
  def providesAdsFavedNaviGRPCClient(
    serviceIdentifier: ServiceIdentifier,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
  ): ManagedChannel = {
    val label = "navi-wals-ads-faved-tweets"
    val dest = "/s/ads-prediction/navi-wals-ads-faved-tweets"
    buildClient(serviceIdentifier, timeoutConfig, statsReceiver, dest, label)
  }

  @Provides
  @Singleton
  @Named(ModuleNames.AdsMonetizableNaviGRPCClient)
  def providesAdsMonetizableNaviGRPCClient(
    serviceIdentifier: ServiceIdentifier,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
  ): ManagedChannel = {
    val label = "navi-wals-ads-monetizable-tweets"
    val dest = "/s/ads-prediction/navi-wals-ads-monetizable-tweets"
    buildClient(serviceIdentifier, timeoutConfig, statsReceiver, dest, label)
  }

  private def buildClient(
    serviceIdentifier: ServiceIdentifier,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
    dest: String,
    label: String
  ): ManagedChannel = {

    val stats = statsReceiver.scope("clnt").scope(label)

    val client = Http.client
      .withLabel(label)
      .withMutualTls(serviceIdentifier)
      .withRequestTimeout(timeoutConfig.naviRequestTimeout)
      .withTransport.connectTimeout(Duration.fromMilliseconds(10000))
      .withSession.acquisitionTimeout(Duration.fromMilliseconds(20000))
      .withStatsReceiver(stats)
      .withHttpStats

    FinagleChannelBuilder
      .forTarget(dest)
      .overrideAuthority("rustserving")
      .maxRetryAttempts(maxRetryAttempts)
      .enableRetryForStatus(io.grpc.Status.RESOURCE_EXHAUSTED)
      .enableRetryForStatus(io.grpc.Status.UNKNOWN)
      .enableUnsafeFullyBufferingMode()
      .httpClient(client)
      .build()

  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.twitter.inject.TwitterModule
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.data_pipeline.scalding.thriftscala.BlueVerifiedAnnotationsV2
import com.twitter.storage.client.manhattan.kv.ManhattanKVClientMtlsParams
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus_internal.manhattan.Athena
import com.twitter.storehaus_internal.manhattan.ManhattanRO
import com.twitter.storehaus_internal.manhattan.ManhattanROConfig
import com.twitter.storehaus_internal.util.ApplicationID
import com.twitter.storehaus_internal.util.DatasetName
import com.twitter.storehaus_internal.util.HDFSPath
import com.twitter.bijection.scrooge.BinaryScalaCodec
import com.twitter.hermit.store.common.ObservedCachedReadableStore

object BlueVerifiedAnnotationStoreModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.BlueVerifiedAnnotationStore)
  def providesBlueVerifiedAnnotationStore(
    statsReceiver: StatsReceiver,
    manhattanKVClientMtlsParams: ManhattanKVClientMtlsParams,
  ): ReadableStore[String, BlueVerifiedAnnotationsV2] = {

    implicit val valueCodec = new BinaryScalaCodec(BlueVerifiedAnnotationsV2)

    val underlyingStore = ManhattanRO
      .getReadableStoreWithMtls[String, BlueVerifiedAnnotationsV2](
        ManhattanROConfig(
          HDFSPath(""),
          ApplicationID("content_recommender_athena"),
          DatasetName("blue_verified_annotations"),
          Athena),
        manhattanKVClientMtlsParams
      )

    ObservedCachedReadableStore.from(
      underlyingStore,
      ttl = 24.hours,
      maxKeys = 100000,
      windowSize = 10000L,
      cacheName = "blue_verified_annotation_cache"
    )(statsReceiver.scope("inMemoryCachedBlueVerifiedAnnotationStore"))
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.timelines.configapi.Config
import com.twitter.cr_mixer.param.CrMixerParamConfig
import com.twitter.inject.TwitterModule
import javax.inject.Singleton

object CrMixerParamConfigModule extends TwitterModule {

  @Provides
  @Singleton
  def provideConfig(): Config = {
    CrMixerParamConfig.config
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.app.Flag
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.store.strato.StratoFetchableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.strato.client.{Client => StratoClient}
import com.twitter.simclusters_v2.thriftscala.OrderedClustersAndMembers
import javax.inject.Named

object TwiceClustersMembersStoreModule extends TwitterModule {

  private val twiceClustersMembersColumnPath: Flag[String] = flag[String](
    name = "crMixer.twiceClustersMembersColumnPath",
    default =
      "recommendations/simclusters_v2/embeddings/TwiceClustersMembersLargestDimApeSimilarity",
    help = "Strato column path for TweetRecentEngagedUsersStore"
  )

  @Provides
  @Singleton
  @Named(ModuleNames.TwiceClustersMembersStore)
  def providesTweetRecentEngagedUserStore(
    statsReceiver: StatsReceiver,
    stratoClient: StratoClient,
  ): ReadableStore[UserId, OrderedClustersAndMembers] = {
    val twiceClustersMembersStratoFetchableStore = StratoFetchableStore
      .withUnitView[UserId, OrderedClustersAndMembers](
        stratoClient,
        twiceClustersMembersColumnPath())

    ObservedReadableStore(
      twiceClustersMembersStratoFetchableStore
    )(statsReceiver.scope("twice_clusters_members_largestDimApe_similarity_store"))
  }
}
package com.twitter.cr_mixer

import com.twitter.finagle.thrift.ClientId
import com.twitter.finatra.thrift.routing.ThriftWarmup
import com.twitter.inject.Logging
import com.twitter.inject.utils.Handler
import com.twitter.product_mixer.core.{thriftscala => pt}
import com.twitter.cr_mixer.{thriftscala => st}
import com.twitter.scrooge.Request
import com.twitter.scrooge.Response
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class CrMixerThriftServerWarmupHandler @Inject() (warmup: ThriftWarmup)
    extends Handler
    with Logging {

  private val clientId = ClientId("thrift-warmup-client")

  def handle(): Unit = {
    val testIds = Seq(1, 2, 3)
    try {
      clientId.asCurrent {
        testIds.foreach { id =>
          val warmupReq = warmupQuery(id)
          info(s"Sending warm-up request to service with query: $warmupReq")
          warmup.sendRequest(
            method = st.CrMixer.GetTweetRecommendations,
            req = Request(st.CrMixer.GetTweetRecommendations.Args(warmupReq)))(assertWarmupResponse)
        }
      }
    } catch {
      case e: Throwable =>
        // we don't want a warmup failure to prevent start-up
        error(e.getMessage, e)
    }
    info("Warm-up done.")
  }

  private def warmupQuery(userId: Long): st.CrMixerTweetRequest = {
    val clientContext = pt.ClientContext(
      userId = Some(userId),
      guestId = None,
      appId = Some(258901L),
      ipAddress = Some("0.0.0.0"),
      userAgent = Some("FAKE_USER_AGENT_FOR_WARMUPS"),
      countryCode = Some("US"),
      languageCode = Some("en"),
      isTwoffice = None,
      userRoles = None,
      deviceId = Some("FAKE_DEVICE_ID_FOR_WARMUPS")
    )
    st.CrMixerTweetRequest(
      clientContext = clientContext,
      product = st.Product.Home,
      productContext = Some(st.ProductContext.HomeContext(st.HomeContext())),
    )
  }

  private def assertWarmupResponse(
    result: Try[Response[st.CrMixer.GetTweetRecommendations.SuccessType]]
  ): Unit = {
    // we collect and log any exceptions from the result.
    result match {
      case Return(_) => // ok
      case Throw(exception) =>
        warn("Error performing warm-up request.")
        error(exception.getMessage, exception)
    }
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.bijection.scrooge.BinaryScalaCodec
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.thriftscala.CrMixerTweetResponse
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.hermit.store.common.ReadableWritableStore
import com.twitter.hermit.store.common.ObservedReadableWritableMemcacheStore
import com.twitter.simclusters_v2.common.UserId
import javax.inject.Named

object TweetRecommendationResultsStoreModule extends TwitterModule {
  @Provides
  @Singleton
  def providesTweetRecommendationResultsStore(
    @Named(ModuleNames.TweetRecommendationResultsCache) tweetRecommendationResultsCacheClient: MemcachedClient,
    statsReceiver: StatsReceiver
  ): ReadableWritableStore[UserId, CrMixerTweetResponse] = {
    ObservedReadableWritableMemcacheStore.fromCacheClient(
      cacheClient = tweetRecommendationResultsCacheClient,
      ttl = 24.hours)(
      valueInjection = BinaryScalaCodec(CrMixerTweetResponse),
      statsReceiver = statsReceiver.scope("TweetRecommendationResultsMemcacheStore"),
      keyToString = { k: UserId => k.toString }
    )
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.inject.TwitterModule
import com.twitter.recos.user_tweet_graph.thriftscala.ConsumersBasedRelatedTweetRequest
import com.twitter.recos.user_tweet_graph.thriftscala.RelatedTweetResponse
import com.twitter.recos.user_tweet_graph.thriftscala.UserTweetGraph
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import javax.inject.Named
import javax.inject.Singleton

object ConsumersBasedUserTweetGraphStoreModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.ConsumerBasedUserTweetGraphStore)
  def providesConsumerBasedUserTweetGraphStore(
    userTweetGraphService: UserTweetGraph.MethodPerEndpoint
  ): ReadableStore[ConsumersBasedRelatedTweetRequest, RelatedTweetResponse] = {
    new ReadableStore[ConsumersBasedRelatedTweetRequest, RelatedTweetResponse] {
      override def get(
        k: ConsumersBasedRelatedTweetRequest
      ): Future[Option[RelatedTweetResponse]] = {
        userTweetGraphService.consumersBasedRelatedTweets(k).map(Some(_))
      }
    }
  }
}
package com.twitter.cr_mixer.module

import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.thriftscala.EmbeddingType
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.simclusters_v2.thriftscala.ModelVersion
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.store.strato.StratoFetchableStore
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.strato.client.{Client => StratoClient}
import com.twitter.storehaus.ReadableStore
import com.twitter.simclusters_v2.thriftscala.ScoringAlgorithm
import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.hermit.store.common.ObservedReadableStore
import javax.inject.Named
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.representationscorer.thriftscala.ListScoreId

object RepresentationScorerModule extends TwitterModule {

  private val rsxColumnPath = "recommendations/representation_scorer/listScore"

  private final val SimClusterModelVersion = ModelVersion.Model20m145k2020
  private final val TweetEmbeddingType = EmbeddingType.LogFavBasedTweet

  @Provides
  @Singleton
  @Named(ModuleNames.RsxStore)
  def providesRepresentationScorerStore(
    statsReceiver: StatsReceiver,
    stratoClient: StratoClient,
  ): ReadableStore[(UserId, TweetId), Double] = {
    ObservedReadableStore(
      StratoFetchableStore
        .withUnitView[ListScoreId, Double](stratoClient, rsxColumnPath).composeKeyMapping[(
          UserId,
          TweetId
        )] { key =>
          representationScorerStoreKeyMapping(key._1, key._2)
        }
    )(statsReceiver.scope("rsx_store"))
  }

  private def representationScorerStoreKeyMapping(t1: TweetId, t2: TweetId): ListScoreId = {
    ListScoreId(
      algorithm = ScoringAlgorithm.PairEmbeddingLogCosineSimilarity,
      modelVersion = SimClusterModelVersion,
      targetEmbeddingType = TweetEmbeddingType,
      targetId = InternalId.TweetId(t1),
      candidateEmbeddingType = TweetEmbeddingType,
      candidateIds = Seq(InternalId.TweetId(t2))
    )
  }
}

package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.keyHasher
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.store.strato.StratoFetchableStore
import com.twitter.hermit.store.common.ObservedCachedReadableStore
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.relevance_platform.common.injection.LZ4Injection
import com.twitter.relevance_platform.common.injection.SeqObjectInjection
import com.twitter.storehaus.ReadableStore
import com.twitter.strato.client.Client
import com.twitter.topic_recos.thriftscala.TopicTopTweets
import com.twitter.topic_recos.thriftscala.TopicTweet
import com.twitter.topic_recos.thriftscala.TopicTweetPartitionFlatKey

/**
 * Strato store that wraps the topic top tweets pipeline indexed from a Summingbird job
 */
object SkitStratoStoreModule extends TwitterModule {

  val column = "recommendations/topic_recos/topicTopTweets"

  @Provides
  @Singleton
  @Named(ModuleNames.SkitStratoStoreName)
  def providesSkitStratoStore(
    @Named(ModuleNames.UnifiedCache) crMixerUnifiedCacheClient: MemcachedClient,
    stratoClient: Client,
    statsReceiver: StatsReceiver
  ): ReadableStore[TopicTweetPartitionFlatKey, Seq[TopicTweet]] = {
    val skitStore = ObservedReadableStore(
      StratoFetchableStore
        .withUnitView[TopicTweetPartitionFlatKey, TopicTopTweets](stratoClient, column))(
      statsReceiver.scope(ModuleNames.SkitStratoStoreName)).mapValues { topicTopTweets =>
      topicTopTweets.topTweets
    }

    val memCachedStore = ObservedMemcachedReadableStore
      .fromCacheClient(
        backingStore = skitStore,
        cacheClient = crMixerUnifiedCacheClient,
        ttl = 10.minutes
      )(
        valueInjection = LZ4Injection.compose(SeqObjectInjection[TopicTweet]()),
        statsReceiver = statsReceiver.scope("memcached_skit_store"),
        keyToString = { k => s"skit:${keyHasher.hashKey(k.toString.getBytes)}" }
      )

    ObservedCachedReadableStore.from[TopicTweetPartitionFlatKey, Seq[TopicTweet]](
      memCachedStore,
      ttl = 5.minutes,
      maxKeys = 100000, // ~150MB max
      cacheName = "skit_in_memory_cache",
      windowSize = 10000L
    )(statsReceiver.scope("skit_in_memory_cache"))
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.source_signal.FrsStore
import com.twitter.cr_mixer.source_signal.FrsStore.FrsQueryResult
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.follow_recommendations.thriftscala.FollowRecommendationsThriftService
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.storehaus.ReadableStore
import javax.inject.Named

object FrsStoreModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.FrsStore)
  def providesFrsStore(
    frsClient: FollowRecommendationsThriftService.MethodPerEndpoint,
    statsReceiver: StatsReceiver,
    decider: CrMixerDecider
  ): ReadableStore[FrsStore.Query, Seq[FrsQueryResult]] = {
    ObservedReadableStore(FrsStore(frsClient, statsReceiver, decider))(
      statsReceiver.scope("follow_recommendations_store"))
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.store.strato.StratoFetchableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.storehaus.ReadableStore
import com.twitter.strato.client.{Client => StratoClient}
import com.twitter.trends.trip_v1.trip_tweets.thriftscala.TripTweet
import com.twitter.trends.trip_v1.trip_tweets.thriftscala.TripTweets
import com.twitter.trends.trip_v1.trip_tweets.thriftscala.TripDomain
import javax.inject.Named

object TripCandidateStoreModule extends TwitterModule {
  private val stratoColumn = "trends/trip/tripTweetsDataflowProd"

  @Provides
  @Named(ModuleNames.TripCandidateStore)
  def providesSimClustersTripCandidateStore(
    statsReceiver: StatsReceiver,
    stratoClient: StratoClient
  ): ReadableStore[TripDomain, Seq[TripTweet]] = {
    val tripCandidateStratoFetchableStore =
      StratoFetchableStore
        .withUnitView[TripDomain, TripTweets](stratoClient, stratoColumn)
        .mapValues(_.tweets)

    ObservedReadableStore(
      tripCandidateStratoFetchableStore
    )(statsReceiver.scope("simclusters_trip_candidate_store"))
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.app.Flag
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.store.strato.StratoFetchableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.common.UserId
import com.twitter.hermit.stp.thriftscala.STPResult
import com.twitter.storehaus.ReadableStore
import com.twitter.strato.client.{Client => StratoClient}
import javax.inject.Named

object StrongTiePredictionStoreModule extends TwitterModule {

  private val strongTiePredictionColumnPath: Flag[String] = flag[String](
    name = "crMixer.strongTiePredictionColumnPath",
    default = "onboarding/userrecs/strong_tie_prediction_big",
    help = "Strato column path for StrongTiePredictionStore"
  )

  @Provides
  @Singleton
  @Named(ModuleNames.StpStore)
  def providesStrongTiePredictionStore(
    statsReceiver: StatsReceiver,
    stratoClient: StratoClient,
  ): ReadableStore[UserId, STPResult] = {
    val strongTiePredictionStratoFetchableStore = StratoFetchableStore
      .withUnitView[UserId, STPResult](stratoClient, strongTiePredictionColumnPath())

    ObservedReadableStore(
      strongTiePredictionStratoFetchableStore
    )(statsReceiver.scope("strong_tie_prediction_big_store"))
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.bijection.Injection
import com.twitter.bijection.scrooge.BinaryScalaCodec
import com.twitter.bijection.scrooge.CompactScalaCodec
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.inject.TwitterModule
import com.twitter.ml.api.{thriftscala => api}
import com.twitter.simclusters_v2.thriftscala.CandidateTweetsList
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.thriftscala.InternalId
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

object EmbeddingStoreModule extends TwitterModule {
  type UserId = Long
  implicit val mbcgUserEmbeddingInjection: Injection[api.Embedding, Array[Byte]] =
    CompactScalaCodec(api.Embedding)
  implicit val tweetCandidatesInjection: Injection[CandidateTweetsList, Array[Byte]] =
    CompactScalaCodec(CandidateTweetsList)

  final val TwHINEmbeddingRegularUpdateMhStoreName = "TwHINEmbeddingRegularUpdateMhStore"
  @Provides
  @Singleton
  @Named(TwHINEmbeddingRegularUpdateMhStoreName)
  def twHINEmbeddingRegularUpdateMhStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[InternalId, api.Embedding] = {
    val binaryEmbeddingInjection: Injection[api.Embedding, Array[Byte]] =
      BinaryScalaCodec(api.Embedding)

    val longCodec = implicitly[Injection[Long, Array[Byte]]]

    ManhattanRO
      .getReadableStoreWithMtls[TweetId, api.Embedding](
        ManhattanROConfig(
          HDFSPath(""), // not needed
          ApplicationID("cr_mixer_apollo"),
          DatasetName("twhin_regular_update_tweet_embedding_apollo"),
          Apollo
        ),
        ManhattanKVClientMtlsParams(serviceIdentifier)
      )(longCodec, binaryEmbeddingInjection).composeKeyMapping[InternalId] {
        case InternalId.TweetId(tweetId) =>
          tweetId
        case _ =>
          throw new UnsupportedOperationException("Invalid Internal Id")
      }
  }

  final val ConsumerBasedTwHINEmbeddingRegularUpdateMhStoreName =
    "ConsumerBasedTwHINEmbeddingRegularUpdateMhStore"
  @Provides
  @Singleton
  @Named(ConsumerBasedTwHINEmbeddingRegularUpdateMhStoreName)
  def consumerBasedTwHINEmbeddingRegularUpdateMhStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[InternalId, api.Embedding] = {
    val binaryEmbeddingInjection: Injection[api.Embedding, Array[Byte]] =
      BinaryScalaCodec(api.Embedding)

    val longCodec = implicitly[Injection[Long, Array[Byte]]]

    ManhattanRO
      .getReadableStoreWithMtls[UserId, api.Embedding](
        ManhattanROConfig(
          HDFSPath(""), // not needed
          ApplicationID("cr_mixer_apollo"),
          DatasetName("twhin_user_embedding_regular_update_apollo"),
          Apollo
        ),
        ManhattanKVClientMtlsParams(serviceIdentifier)
      )(longCodec, binaryEmbeddingInjection).composeKeyMapping[InternalId] {
        case InternalId.UserId(userId) =>
          userId
        case _ =>
          throw new UnsupportedOperationException("Invalid Internal Id")
      }
  }

  final val TwoTowerFavConsumerEmbeddingMhStoreName = "TwoTowerFavConsumerEmbeddingMhStore"
  @Provides
  @Singleton
  @Named(TwoTowerFavConsumerEmbeddingMhStoreName)
  def twoTowerFavConsumerEmbeddingMhStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[InternalId, api.Embedding] = {
    val binaryEmbeddingInjection: Injection[api.Embedding, Array[Byte]] =
      BinaryScalaCodec(api.Embedding)

    val longCodec = implicitly[Injection[Long, Array[Byte]]]

    ManhattanRO
      .getReadableStoreWithMtls[UserId, api.Embedding](
        ManhattanROConfig(
          HDFSPath(""), // not needed
          ApplicationID("cr_mixer_apollo"),
          DatasetName("two_tower_fav_user_embedding_apollo"),
          Apollo
        ),
        ManhattanKVClientMtlsParams(serviceIdentifier)
      )(longCodec, binaryEmbeddingInjection).composeKeyMapping[InternalId] {
        case InternalId.UserId(userId) =>
          userId
        case _ =>
          throw new UnsupportedOperationException("Invalid Internal Id")
      }
  }

  final val DebuggerDemoUserEmbeddingMhStoreName = "DebuggerDemoUserEmbeddingMhStoreName"
  @Provides
  @Singleton
  @Named(DebuggerDemoUserEmbeddingMhStoreName)
  def debuggerDemoUserEmbeddingStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[InternalId, api.Embedding] = {
    // This dataset is from src/scala/com/twitter/wtf/beam/bq_embedding_export/sql/MlfExperimentalUserEmbeddingScalaDataset.sql
    // Change the above sql if you want to use a diff embedding
    val manhattanROConfig = ManhattanROConfig(
      HDFSPath(""), // not needed
      ApplicationID("cr_mixer_apollo"),
      DatasetName("experimental_user_embedding"),
      Apollo
    )
    buildUserEmbeddingStore(serviceIdentifier, manhattanROConfig)
  }

  final val DebuggerDemoTweetEmbeddingMhStoreName = "DebuggerDemoTweetEmbeddingMhStore"
  @Provides
  @Singleton
  @Named(DebuggerDemoTweetEmbeddingMhStoreName)
  def debuggerDemoTweetEmbeddingStore(
    serviceIdentifier: ServiceIdentifier
  ): ReadableStore[InternalId, api.Embedding] = {
    // This dataset is from src/scala/com/twitter/wtf/beam/bq_embedding_export/sql/MlfExperimentalTweetEmbeddingScalaDataset.sql
    // Change the above sql if you want to use a diff embedding
    val manhattanROConfig = ManhattanROConfig(
      HDFSPath(""), // not needed
      ApplicationID("cr_mixer_apollo"),
      DatasetName("experimental_tweet_embedding"),
      Apollo
    )
    buildTweetEmbeddingStore(serviceIdentifier, manhattanROConfig)
  }

  private def buildUserEmbeddingStore(
    serviceIdentifier: ServiceIdentifier,
    manhattanROConfig: ManhattanROConfig
  ): ReadableStore[InternalId, api.Embedding] = {
    val binaryEmbeddingInjection: Injection[api.Embedding, Array[Byte]] =
      BinaryScalaCodec(api.Embedding)

    val longCodec = implicitly[Injection[Long, Array[Byte]]]
    ManhattanRO
      .getReadableStoreWithMtls[UserId, api.Embedding](
        manhattanROConfig,
        ManhattanKVClientMtlsParams(serviceIdentifier)
      )(longCodec, binaryEmbeddingInjection).composeKeyMapping[InternalId] {
        case InternalId.UserId(userId) =>
          userId
        case _ =>
          throw new UnsupportedOperationException("Invalid Internal Id")
      }
  }

  private def buildTweetEmbeddingStore(
    serviceIdentifier: ServiceIdentifier,
    manhattanROConfig: ManhattanROConfig
  ): ReadableStore[InternalId, api.Embedding] = {
    val binaryEmbeddingInjection: Injection[api.Embedding, Array[Byte]] =
      BinaryScalaCodec(api.Embedding)

    val longCodec = implicitly[Injection[Long, Array[Byte]]]

    ManhattanRO
      .getReadableStoreWithMtls[TweetId, api.Embedding](
        manhattanROConfig,
        ManhattanKVClientMtlsParams(serviceIdentifier)
      )(longCodec, binaryEmbeddingInjection).composeKeyMapping[InternalId] {
        case InternalId.TweetId(tweetId) =>
          tweetId
        case _ =>
          throw new UnsupportedOperationException("Invalid Internal Id")
      }
  }
}
package com.twitter.cr_mixer.module.thrift_client

import com.twitter.app.Flag
import com.twitter.cr_mixer.module.core.TimeoutConfigModule.UserAdGraphClientTimeoutFlagName
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.finagle.mtls.client.MtlsStackClient.MtlsThriftMuxClientSyntax
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finatra.mtls.thriftmux.modules.MtlsClient
import com.twitter.inject.Injector
import com.twitter.inject.thrift.modules.ThriftMethodBuilderClientModule
import com.twitter.recos.user_ad_graph.thriftscala.UserAdGraph
import com.twitter.util.Duration
import com.twitter.util.Throw

object UserAdGraphClientModule
    extends ThriftMethodBuilderClientModule[
      UserAdGraph.ServicePerEndpoint,
      UserAdGraph.MethodPerEndpoint
    ]
    with MtlsClient {

  override val label = "user-ad-graph"
  override val dest = "/s/user-tweet-graph/user-ad-graph"
  private val userAdGraphClientTimeout: Flag[Duration] =
    flag[Duration](UserAdGraphClientTimeoutFlagName, "userAdGraph client timeout")
  override def requestTimeout: Duration = userAdGraphClientTimeout()

  override def retryBudget: RetryBudget = RetryBudget.Empty

  override def configureThriftMuxClient(
    injector: Injector,
    client: ThriftMux.Client
  ): ThriftMux.Client =
    super
      .configureThriftMuxClient(injector, client)
      .withMutualTls(injector.instance[ServiceIdentifier])
      .withStatsReceiver(injector.instance[StatsReceiver].scope("clnt"))
      .withResponseClassifier {
        case ReqRep(_, Throw(_: ClientDiscardedRequestException)) => ResponseClass.Ignorable
      }

}
package com.twitter.cr_mixer.module.thrift_client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.thriftmux.MethodBuilder
import com.twitter.finatra.mtls.thriftmux.modules.MtlsClient
import com.twitter.hydra.root.{thriftscala => ht}
import com.twitter.inject.Injector
import com.twitter.inject.thrift.modules.ThriftMethodBuilderClientModule

object HydraRootClientModule
    extends ThriftMethodBuilderClientModule[
      ht.HydraRoot.ServicePerEndpoint,
      ht.HydraRoot.MethodPerEndpoint
    ]
    with MtlsClient {
  override def label: String = "hydra-root"

  override def dest: String = "/s/hydra/hydra-root"

  override protected def configureMethodBuilder(
    injector: Injector,
    methodBuilder: MethodBuilder
  ): MethodBuilder = methodBuilder.withTimeoutTotal(500.milliseconds)

}
package com.twitter.cr_mixer.source_signal

import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.param.FrsParams
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.source_signal.FrsStore.FrsQueryResult
import com.twitter.cr_mixer.source_signal.SourceFetcher.FetcherQuery
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import javax.inject.Singleton
import javax.inject.Inject
import javax.inject.Named

@Singleton
case class FrsSourceSignalFetcher @Inject() (
  @Named(ModuleNames.FrsStore) frsStore: ReadableStore[FrsStore.Query, Seq[FrsQueryResult]],
  override val timeoutConfig: TimeoutConfig,
  globalStats: StatsReceiver)
    extends SourceSignalFetcher {

  override protected val stats: StatsReceiver = globalStats.scope(identifier)
  override type SignalConvertType = UserId

  override def isEnabled(query: FetcherQuery): Boolean = {
    query.params(FrsParams.EnableSourceParam)
  }

  override def fetchAndProcess(query: FetcherQuery): Future[Option[Seq[SourceInfo]]] = {
    // Fetch raw signals
    val rawSignals = frsStore
      .get(FrsStore.Query(query.userId, query.params(GlobalParams.UnifiedMaxSourceKeyNum)))
      .map {
        _.map {
          _.map {
            _.userId
          }
        }
      }
    // Process signals
    rawSignals.map {
      _.map { frsUsers =>
        convertSourceInfo(SourceType.FollowRecommendation, frsUsers)
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
        internalId = InternalId.UserId(signal),
        sourceEventTime = None
      )
    }
  }
}
package com.twitter.cr_mixer.module.thrift_client

import com.twitter.app.Flag
import com.twitter.cr_mixer.module.core.TimeoutConfigModule.QigRankerClientTimeoutFlagName
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finatra.mtls.thriftmux.modules.MtlsClient
import com.twitter.inject.Injector
import com.twitter.inject.thrift.modules.ThriftMethodBuilderClientModule
import com.twitter.qig_ranker.thriftscala.QigRanker
import com.twitter.util.Duration
import com.twitter.util.Throw

object QigServiceClientModule
    extends ThriftMethodBuilderClientModule[
      QigRanker.ServicePerEndpoint,
      QigRanker.MethodPerEndpoint
    ]
    with MtlsClient {
  override val label: String = "qig-ranker"
  override val dest: String = "/s/qig-shared/qig-ranker"
  private val qigRankerClientTimeout: Flag[Duration] =
    flag[Duration](QigRankerClientTimeoutFlagName, "ranking timeout")

  override def requestTimeout: Duration = qigRankerClientTimeout()

  override def configureThriftMuxClient(
    injector: Injector,
    client: ThriftMux.Client
  ): ThriftMux.Client =
    super
      .configureThriftMuxClient(injector, client)
      .withStatsReceiver(injector.instance[StatsReceiver].scope("clnt"))
      .withResponseClassifier {
        case ReqRep(_, Throw(_: ClientDiscardedRequestException)) => ResponseClass.Ignorable
      }
}
package com.twitter.cr_mixer.module.thrift_client

import com.twitter.app.Flag
import com.twitter.cr_mixer.module.core.TimeoutConfigModule.UserVideoGraphClientTimeoutFlagName
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finatra.mtls.thriftmux.modules.MtlsClient
import com.twitter.inject.Injector
import com.twitter.inject.thrift.modules.ThriftMethodBuilderClientModule
import com.twitter.recos.user_video_graph.thriftscala.UserVideoGraph
import com.twitter.util.Duration
import com.twitter.util.Throw

object UserVideoGraphClientModule
    extends ThriftMethodBuilderClientModule[
      UserVideoGraph.ServicePerEndpoint,
      UserVideoGraph.MethodPerEndpoint
    ]
    with MtlsClient {

  override val label = "user-video-graph"
  override val dest = "/s/user-tweet-graph/user-video-graph"
  private val userVideoGraphClientTimeout: Flag[Duration] =
    flag[Duration](
      UserVideoGraphClientTimeoutFlagName,
      "userVideoGraph client timeout"
    )
  override def requestTimeout: Duration = userVideoGraphClientTimeout()

  override def retryBudget: RetryBudget = RetryBudget.Empty

  override def configureThriftMuxClient(
    injector: Injector,
    client: ThriftMux.Client
  ): ThriftMux.Client =
    super
      .configureThriftMuxClient(injector, client)
      .withStatsReceiver(injector.instance[StatsReceiver].scope("clnt"))
      .withResponseClassifier {
        case ReqRep(_, Throw(_: ClientDiscardedRequestException)) => ResponseClass.Ignorable
      }
}
