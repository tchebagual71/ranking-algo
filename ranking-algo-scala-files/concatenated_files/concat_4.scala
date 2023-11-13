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
