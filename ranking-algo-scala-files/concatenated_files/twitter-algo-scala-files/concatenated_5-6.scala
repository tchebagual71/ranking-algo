package com.twitter.cr_mixer.module.thrift_client

import com.twitter.app.Flag
import com.twitter.cr_mixer.module.core.TimeoutConfigModule.UtegClientTimeoutFlagName
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finatra.mtls.thriftmux.modules.MtlsClient
import com.twitter.inject.Injector
import com.twitter.inject.thrift.modules.ThriftMethodBuilderClientModule
import com.twitter.recos.user_tweet_entity_graph.thriftscala.UserTweetEntityGraph
import com.twitter.util.Duration
import com.twitter.util.Throw

object UserTweetEntityGraphClientModule
    extends ThriftMethodBuilderClientModule[
      UserTweetEntityGraph.ServicePerEndpoint,
      UserTweetEntityGraph.MethodPerEndpoint
    ]
    with MtlsClient {

  override val label = "user-tweet-entity-graph"
  override val dest = "/s/cassowary/user_tweet_entity_graph"
  private val userTweetEntityGraphClientTimeout: Flag[Duration] =
    flag[Duration](UtegClientTimeoutFlagName, "user tweet entity graph client timeout")
  override def requestTimeout: Duration = userTweetEntityGraphClientTimeout()

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
package com.twitter.cr_mixer.module.thrift_client

import com.google.inject.Provides
import com.twitter.conversions.PercentOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.finagle.mtls.client.MtlsStackClient._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.ClientId
import com.twitter.inject.TwitterModule
import com.twitter.simclustersann.{thriftscala => t}
import javax.inject.Named
import javax.inject.Singleton

object SimClustersAnnServiceClientModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.ProdSimClustersANNServiceClientName)
  def providesProdSimClustersANNServiceClient(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
  ): t.SimClustersANNService.MethodPerEndpoint = {
    val label = "simclusters-ann-server"
    val dest = "/s/simclusters-ann/simclusters-ann"

    buildClient(serviceIdentifier, clientId, timeoutConfig, statsReceiver, dest, label)
  }

  @Provides
  @Singleton
  @Named(ModuleNames.ExperimentalSimClustersANNServiceClientName)
  def providesExperimentalSimClustersANNServiceClient(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
  ): t.SimClustersANNService.MethodPerEndpoint = {
    val label = "simclusters-ann-experimental-server"
    val dest = "/s/simclusters-ann/simclusters-ann-experimental"

    buildClient(serviceIdentifier, clientId, timeoutConfig, statsReceiver, dest, label)
  }

  @Provides
  @Singleton
  @Named(ModuleNames.SimClustersANNServiceClientName1)
  def providesSimClustersANNServiceClient1(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
  ): t.SimClustersANNService.MethodPerEndpoint = {
    val label = "simclusters-ann-server-1"
    val dest = "/s/simclusters-ann/simclusters-ann-1"

    buildClient(serviceIdentifier, clientId, timeoutConfig, statsReceiver, dest, label)
  }

  @Provides
  @Singleton
  @Named(ModuleNames.SimClustersANNServiceClientName2)
  def providesSimClustersANNServiceClient2(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
  ): t.SimClustersANNService.MethodPerEndpoint = {
    val label = "simclusters-ann-server-2"
    val dest = "/s/simclusters-ann/simclusters-ann-2"

    buildClient(serviceIdentifier, clientId, timeoutConfig, statsReceiver, dest, label)
  }

  @Provides
  @Singleton
  @Named(ModuleNames.SimClustersANNServiceClientName3)
  def providesSimClustersANNServiceClient3(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
  ): t.SimClustersANNService.MethodPerEndpoint = {
    val label = "simclusters-ann-server-3"
    val dest = "/s/simclusters-ann/simclusters-ann-3"

    buildClient(serviceIdentifier, clientId, timeoutConfig, statsReceiver, dest, label)
  }

  @Provides
  @Singleton
  @Named(ModuleNames.SimClustersANNServiceClientName5)
  def providesSimClustersANNServiceClient5(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
  ): t.SimClustersANNService.MethodPerEndpoint = {
    val label = "simclusters-ann-server-5"
    val dest = "/s/simclusters-ann/simclusters-ann-5"

    buildClient(serviceIdentifier, clientId, timeoutConfig, statsReceiver, dest, label)
  }

  @Provides
  @Singleton
  @Named(ModuleNames.SimClustersANNServiceClientName4)
  def providesSimClustersANNServiceClient4(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
  ): t.SimClustersANNService.MethodPerEndpoint = {
    val label = "simclusters-ann-server-4"
    val dest = "/s/simclusters-ann/simclusters-ann-4"

    buildClient(serviceIdentifier, clientId, timeoutConfig, statsReceiver, dest, label)
  }
  private def buildClient(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
    dest: String,
    label: String
  ): t.SimClustersANNService.MethodPerEndpoint = {
    val stats = statsReceiver.scope("clnt")

    val thriftClient = ThriftMux.client
      .withMutualTls(serviceIdentifier)
      .withClientId(clientId)
      .withLabel(label)
      .withStatsReceiver(stats)
      .methodBuilder(dest)
      .idempotent(5.percent)
      .withTimeoutPerRequest(timeoutConfig.annServiceClientTimeout)
      .withRetryDisabled
      .servicePerEndpoint[t.SimClustersANNService.ServicePerEndpoint]

    ThriftMux.Client.methodPerEndpoint(thriftClient)
  }

}
package com.twitter.cr_mixer.module.thrift_client

import com.twitter.app.Flag
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finatra.mtls.thriftmux.modules.MtlsClient
import com.twitter.inject.Injector
import com.twitter.inject.thrift.modules.ThriftMethodBuilderClientModule
import com.twitter.recos.user_tweet_graph.thriftscala.UserTweetGraph
import com.twitter.util.Duration
import com.twitter.util.Throw
import com.twitter.cr_mixer.module.core.TimeoutConfigModule.UserTweetGraphClientTimeoutFlagName
import com.twitter.finagle.service.RetryBudget

object UserTweetGraphClientModule
    extends ThriftMethodBuilderClientModule[
      UserTweetGraph.ServicePerEndpoint,
      UserTweetGraph.MethodPerEndpoint
    ]
    with MtlsClient {

  override val label = "user-tweet-graph"
  override val dest = "/s/user-tweet-graph/user-tweet-graph"
  private val userTweetGraphClientTimeout: Flag[Duration] =
    flag[Duration](UserTweetGraphClientTimeoutFlagName, "userTweetGraph client timeout")
  override def requestTimeout: Duration = userTweetGraphClientTimeout()

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
package com.twitter.cr_mixer.module.thrift_client

import com.google.inject.Provides
import com.twitter.app.Flag
import com.twitter.conversions.DurationOps.richDurationFromInt
import com.twitter.cr_mixer.module.core.TimeoutConfigModule.TweetypieClientTimeoutFlagName
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finatra.mtls.thriftmux.modules.MtlsClient
import com.twitter.inject.Injector
import com.twitter.inject.thrift.modules.ThriftMethodBuilderClientModule
import com.twitter.stitch.tweetypie.{TweetyPie => STweetyPie}
import com.twitter.tweetypie.thriftscala.TweetService
import com.twitter.util.Duration
import com.twitter.util.Throw
import javax.inject.Singleton

object TweetyPieClientModule
    extends ThriftMethodBuilderClientModule[
      TweetService.ServicePerEndpoint,
      TweetService.MethodPerEndpoint
    ]
    with MtlsClient {

  override val label = "tweetypie"
  override val dest = "/s/tweetypie/tweetypie"

  private val tweetypieClientTimeout: Flag[Duration] =
    flag[Duration](TweetypieClientTimeoutFlagName, "tweetypie client timeout")
  override def requestTimeout: Duration = tweetypieClientTimeout()

  override def retryBudget: RetryBudget = RetryBudget.Empty

  // We bump the success rate from the default of 0.8 to 0.9 since we're dropping the
  // consecutive failures part of the default policy.
  override def configureThriftMuxClient(
    injector: Injector,
    client: ThriftMux.Client
  ): ThriftMux.Client =
    super
      .configureThriftMuxClient(injector, client)
      .withStatsReceiver(injector.instance[StatsReceiver].scope("clnt"))
      .withSessionQualifier
      .successRateFailureAccrual(successRate = 0.9, window = 30.seconds)
      .withResponseClassifier {
        case ReqRep(_, Throw(_: ClientDiscardedRequestException)) => ResponseClass.Ignorable
      }

  @Provides
  @Singleton
  def providesTweetyPie(
    tweetyPieService: TweetService.MethodPerEndpoint
  ): STweetyPie = {
    STweetyPie(tweetyPieService)
  }
}
package com.twitter.cr_mixer.module.thrift_client

import com.twitter.app.Flag
import com.twitter.finagle.ThriftMux
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.module.core.TimeoutConfigModule.FrsClientTimeoutFlagName
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finatra.mtls.thriftmux.modules.MtlsClient
import com.twitter.follow_recommendations.thriftscala.FollowRecommendationsThriftService
import com.twitter.inject.Injector
import com.twitter.inject.thrift.modules.ThriftMethodBuilderClientModule
import com.twitter.util.Duration

object FrsClientModule
    extends ThriftMethodBuilderClientModule[
      FollowRecommendationsThriftService.ServicePerEndpoint,
      FollowRecommendationsThriftService.MethodPerEndpoint
    ]
    with MtlsClient {

  override def label: String = "follow-recommendations-service"
  override def dest: String = "/s/follow-recommendations/follow-recos-service"

  private val frsSignalFetchTimeout: Flag[Duration] =
    flag[Duration](FrsClientTimeoutFlagName, "FRS signal fetch client timeout")
  override def requestTimeout: Duration = frsSignalFetchTimeout()

  override def retryBudget: RetryBudget = RetryBudget.Empty

  override def configureThriftMuxClient(
    injector: Injector,
    client: ThriftMux.Client
  ): ThriftMux.Client = {
    super
      .configureThriftMuxClient(injector, client)
      .withStatsReceiver(injector.instance[StatsReceiver].scope("clnt"))
      .withSessionQualifier
      .successRateFailureAccrual(successRate = 0.9, window = 30.seconds)
  }
}
package com.twitter.cr_mixer.module.thrift_client

import com.google.inject.Provides
import com.twitter.ann.common.thriftscala.AnnQueryService
import com.twitter.conversions.DurationOps._
import com.twitter.conversions.PercentOps._
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.finagle.mtls.client.MtlsStackClient._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.ClientId
import com.twitter.inject.TwitterModule
import javax.inject.Named
import javax.inject.Singleton

object AnnQueryServiceClientModule extends TwitterModule {
  final val DebuggerDemoAnnServiceClientName = "DebuggerDemoAnnServiceClient"

  @Provides
  @Singleton
  @Named(DebuggerDemoAnnServiceClientName)
  def debuggerDemoAnnServiceClient(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    statsReceiver: StatsReceiver,
    timeoutConfig: TimeoutConfig,
  ): AnnQueryService.MethodPerEndpoint = {
    // This ANN is built from the embeddings in src/scala/com/twitter/wtf/beam/bq_embedding_export/sql/MlfExperimentalTweetEmbeddingScalaDataset.sql
    // Change the above sql if you want to build the index from a diff embedding
    val dest = "/s/cassowary/mlf-experimental-ann-service"
    val label = "experimental-ann"
    buildClient(serviceIdentifier, clientId, timeoutConfig, statsReceiver, dest, label)
  }

  final val TwHINUuaAnnServiceClientName = "TwHINUuaAnnServiceClient"
  @Provides
  @Singleton
  @Named(TwHINUuaAnnServiceClientName)
  def twhinUuaAnnServiceClient(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    statsReceiver: StatsReceiver,
    timeoutConfig: TimeoutConfig,
  ): AnnQueryService.MethodPerEndpoint = {
    val dest = "/s/cassowary/twhin-uua-ann-service"
    val label = "twhin_uua_ann"

    buildClient(serviceIdentifier, clientId, timeoutConfig, statsReceiver, dest, label)
  }

  final val TwHINRegularUpdateAnnServiceClientName = "TwHINRegularUpdateAnnServiceClient"
  @Provides
  @Singleton
  @Named(TwHINRegularUpdateAnnServiceClientName)
  def twHINRegularUpdateAnnServiceClient(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    statsReceiver: StatsReceiver,
    timeoutConfig: TimeoutConfig,
  ): AnnQueryService.MethodPerEndpoint = {
    val dest = "/s/cassowary/twhin-regular-update-ann-service"
    val label = "twhin_regular_update"

    buildClient(serviceIdentifier, clientId, timeoutConfig, statsReceiver, dest, label)
  }

  final val TwoTowerFavAnnServiceClientName = "TwoTowerFavAnnServiceClient"
  @Provides
  @Singleton
  @Named(TwoTowerFavAnnServiceClientName)
  def twoTowerFavAnnServiceClient(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    statsReceiver: StatsReceiver,
    timeoutConfig: TimeoutConfig,
  ): AnnQueryService.MethodPerEndpoint = {
    val dest = "/s/cassowary/tweet-rec-two-tower-fav-ann"
    val label = "tweet_rec_two_tower_fav_ann"

    buildClient(serviceIdentifier, clientId, timeoutConfig, statsReceiver, dest, label)
  }

  private def buildClient(
    serviceIdentifier: ServiceIdentifier,
    clientId: ClientId,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
    dest: String,
    label: String
  ): AnnQueryService.MethodPerEndpoint = {
    val thriftClient = ThriftMux.client
      .withMutualTls(serviceIdentifier)
      .withClientId(clientId)
      .withLabel(label)
      .withStatsReceiver(statsReceiver)
      .withTransport.connectTimeout(500.milliseconds)
      .withSession.acquisitionTimeout(500.milliseconds)
      .methodBuilder(dest)
      .withTimeoutPerRequest(timeoutConfig.annServiceClientTimeout)
      .withRetryDisabled
      .idempotent(5.percent)
      .servicePerEndpoint[AnnQueryService.ServicePerEndpoint]

    ThriftMux.Client.methodPerEndpoint(thriftClient)
  }
}
package com.twitter.cr_mixer.module.thrift_client
import com.twitter.app.Flag
import com.twitter.finagle.ThriftMux
import com.twitter.finatra.mtls.thriftmux.modules.MtlsClient
import com.twitter.inject.thrift.modules.ThriftMethodBuilderClientModule
import com.twitter.search.earlybird.thriftscala.EarlybirdService
import com.twitter.inject.Injector
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.module.core.TimeoutConfigModule.EarlybirdClientTimeoutFlagName
import com.twitter.finagle.service.RetryBudget
import com.twitter.util.Duration
import org.apache.thrift.protocol.TCompactProtocol

object EarlybirdSearchClientModule
    extends ThriftMethodBuilderClientModule[
      EarlybirdService.ServicePerEndpoint,
      EarlybirdService.MethodPerEndpoint
    ]
    with MtlsClient {

  override def label: String = "earlybird"
  override def dest: String = "/s/earlybird-root-superroot/root-superroot"
  private val requestTimeoutFlag: Flag[Duration] =
    flag[Duration](EarlybirdClientTimeoutFlagName, "Earlybird client timeout")
  override protected def requestTimeout: Duration = requestTimeoutFlag()

  override def retryBudget: RetryBudget = RetryBudget.Empty

  override def configureThriftMuxClient(
    injector: Injector,
    client: ThriftMux.Client
  ): ThriftMux.Client = {
    super
      .configureThriftMuxClient(injector, client)
      .withProtocolFactory(new TCompactProtocol.Factory())
      .withSessionQualifier
      .successRateFailureAccrual(successRate = 0.9, window = 30.seconds)
  }
}
package com.twitter.cr_mixer.module.thrift_client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.thriftmux.MethodBuilder
import com.twitter.finatra.mtls.thriftmux.modules.MtlsClient
import com.twitter.inject.Injector
import com.twitter.inject.thrift.modules.ThriftMethodBuilderClientModule
import com.twitter.hydra.partition.{thriftscala => ht}

object HydraPartitionClientModule
    extends ThriftMethodBuilderClientModule[
      ht.HydraPartition.ServicePerEndpoint,
      ht.HydraPartition.MethodPerEndpoint
    ]
    with MtlsClient {
  override def label: String = "hydra-partition"

  override def dest: String = "/s/hydra/hydra-partition"

  override protected def configureMethodBuilder(
    injector: Injector,
    methodBuilder: MethodBuilder
  ): MethodBuilder = methodBuilder.withTimeoutTotal(500.milliseconds)

}
package com.twitter.cr_mixer.source_signal

import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.GraphSourceInfo
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.param.FrsParams
import com.twitter.cr_mixer.source_signal.FrsStore.FrsQueryResult
import com.twitter.cr_mixer.source_signal.SourceFetcher.FetcherQuery
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

/***
 * This store fetches user recommendations from FRS (go/frs) for a given userId
 */
@Singleton
case class FrsSourceGraphFetcher @Inject() (
  @Named(ModuleNames.FrsStore) frsStore: ReadableStore[FrsStore.Query, Seq[FrsQueryResult]],
  override val timeoutConfig: TimeoutConfig,
  globalStats: StatsReceiver)
    extends SourceGraphFetcher {

  override protected val stats: StatsReceiver = globalStats.scope(identifier)
  override protected val graphSourceType: SourceType = SourceType.FollowRecommendation

  override def isEnabled(query: FetcherQuery): Boolean = {
    query.params(FrsParams.EnableSourceGraphParam)
  }

  override def fetchAndProcess(
    query: FetcherQuery,
  ): Future[Option[GraphSourceInfo]] = {

    val rawSignals = trackPerItemStats(query)(
      frsStore
        .get(
          FrsStore
            .Query(query.userId, query.params(FrsParams.MaxConsumerSeedsNumParam))).map {
          _.map {
            _.map { v => (v.userId, v.score) }
          }
        }
    )
    rawSignals.map {
      _.map { userWithScores =>
        convertGraphSourceInfo(userWithScores)
      }
    }
  }
}
package com.twitter.cr_mixer.module.thrift_client

import com.twitter.app.Flag
import com.twitter.cr_mixer.module.core.TimeoutConfigModule.UserTweetGraphPlusClientTimeoutFlagName
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finatra.mtls.thriftmux.modules.MtlsClient
import com.twitter.inject.Injector
import com.twitter.inject.thrift.modules.ThriftMethodBuilderClientModule
import com.twitter.recos.user_tweet_graph_plus.thriftscala.UserTweetGraphPlus
import com.twitter.util.Duration
import com.twitter.util.Throw

object UserTweetGraphPlusClientModule
    extends ThriftMethodBuilderClientModule[
      UserTweetGraphPlus.ServicePerEndpoint,
      UserTweetGraphPlus.MethodPerEndpoint
    ]
    with MtlsClient {

  override val label = "user-tweet-graph-plus"
  override val dest = "/s/user-tweet-graph/user-tweet-graph-plus"
  private val userTweetGraphPlusClientTimeout: Flag[Duration] =
    flag[Duration](
      UserTweetGraphPlusClientTimeoutFlagName,
      "userTweetGraphPlus client timeout"
    )
  override def requestTimeout: Duration = userTweetGraphPlusClientTimeout()

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
package com.twitter.cr_mixer.module.similarity_engine
import com.google.inject.Provides
import com.twitter.ann.common.thriftscala.AnnQueryService
import com.twitter.cr_mixer.model.ModelConfig
import com.twitter.cr_mixer.module.EmbeddingStoreModule
import com.twitter.cr_mixer.module.thrift_client.AnnQueryServiceClientModule
import com.twitter.cr_mixer.similarity_engine.HnswANNSimilarityEngine
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import javax.inject.Named
import com.twitter.ml.api.{thriftscala => api}
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.similarity_engine.HnswANNEngineQuery
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.memcached.{Client => MemcachedClient}

object TweetBasedTwHINSimlarityEngineModule extends TwitterModule {
  @Provides
  @Named(ModuleNames.TweetBasedTwHINANNSimilarityEngine)
  def providesTweetBasedTwHINANNSimilarityEngine(
    // MH stores
    @Named(EmbeddingStoreModule.TwHINEmbeddingRegularUpdateMhStoreName)
    twHINEmbeddingRegularUpdateMhStore: ReadableStore[InternalId, api.Embedding],
    @Named(EmbeddingStoreModule.DebuggerDemoTweetEmbeddingMhStoreName)
    debuggerDemoTweetEmbeddingMhStore: ReadableStore[InternalId, api.Embedding],
    // ANN clients
    @Named(AnnQueryServiceClientModule.TwHINRegularUpdateAnnServiceClientName)
    twHINRegularUpdateAnnService: AnnQueryService.MethodPerEndpoint,
    @Named(AnnQueryServiceClientModule.DebuggerDemoAnnServiceClientName)
    debuggerDemoAnnService: AnnQueryService.MethodPerEndpoint,
    // Other configs
    @Named(ModuleNames.UnifiedCache) crMixerUnifiedCacheClient: MemcachedClient,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver
  ): HnswANNSimilarityEngine = {
    new HnswANNSimilarityEngine(
      embeddingStoreLookUpMap = Map(
        ModelConfig.TweetBasedTwHINRegularUpdateAll20221024 -> twHINEmbeddingRegularUpdateMhStore,
        ModelConfig.DebuggerDemo -> debuggerDemoTweetEmbeddingMhStore,
      ),
      annServiceLookUpMap = Map(
        ModelConfig.TweetBasedTwHINRegularUpdateAll20221024 -> twHINRegularUpdateAnnService,
        ModelConfig.DebuggerDemo -> debuggerDemoAnnService,
      ),
      globalStats = statsReceiver,
      identifier = SimilarityEngineType.TweetBasedTwHINANN,
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.similarityEngineTimeout,
        gatingConfig = GatingConfig(
          deciderConfig = None,
          enableFeatureSwitch = None
        )
      ),
      memCacheConfigOpt = Some(
        SimilarityEngine.MemCacheConfig[HnswANNEngineQuery](
          cacheClient = crMixerUnifiedCacheClient,
          ttl = 30.minutes,
          keyToString = (query: HnswANNEngineQuery) =>
            SimilarityEngine.keyHasher.hashKey(query.cacheKey.getBytes).toString
        ))
    )
  }
}
package com.twitter.cr_mixer.module.similarity_engine

import com.google.inject.Provides
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.similarity_engine.ProducerBasedUserTweetGraphSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.ProducerBasedUnifiedSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.cr_mixer.similarity_engine.SimClustersANNSimilarityEngine
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.storehaus.ReadableStore
import javax.inject.Named
import javax.inject.Singleton

object ProducerBasedUnifiedSimilarityEngineModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.ProducerBasedUnifiedSimilarityEngine)
  def providesProducerBasedUnifiedSimilarityEngine(
    @Named(ModuleNames.ProducerBasedUserTweetGraphSimilarityEngine)
    producerBasedUserTweetGraphSimilarityEngine: StandardSimilarityEngine[
      ProducerBasedUserTweetGraphSimilarityEngine.Query,
      TweetWithScore
    ],
    @Named(ModuleNames.SimClustersANNSimilarityEngine)
    simClustersANNSimilarityEngine: StandardSimilarityEngine[
      SimClustersANNSimilarityEngine.Query,
      TweetWithScore
    ],
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
  ): StandardSimilarityEngine[
    ProducerBasedUnifiedSimilarityEngine.Query,
    TweetWithCandidateGenerationInfo
  ] = {

    val underlyingStore: ReadableStore[ProducerBasedUnifiedSimilarityEngine.Query, Seq[
      TweetWithCandidateGenerationInfo
    ]] = ProducerBasedUnifiedSimilarityEngine(
      producerBasedUserTweetGraphSimilarityEngine,
      simClustersANNSimilarityEngine,
      statsReceiver
    )

    new StandardSimilarityEngine[
      ProducerBasedUnifiedSimilarityEngine.Query,
      TweetWithCandidateGenerationInfo
    ](
      implementingStore = underlyingStore,
      identifier = SimilarityEngineType.ProducerBasedUnifiedSimilarityEngine,
      globalStats = statsReceiver,
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.similarityEngineTimeout,
        gatingConfig = GatingConfig(
          deciderConfig = None,
          enableFeatureSwitch = None
        )
      )
    )
  }
}
package com.twitter.cr_mixer.module.similarity_engine

import com.google.inject.Provides
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.similarity_engine.ConsumersBasedUserAdGraphSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.DeciderConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.recos.user_ad_graph.thriftscala.ConsumersBasedRelatedAdRequest
import com.twitter.recos.user_ad_graph.thriftscala.RelatedAdResponse
import com.twitter.storehaus.ReadableStore
import javax.inject.Named
import javax.inject.Singleton

object ConsumersBasedUserAdGraphSimilarityEngineModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.ConsumersBasedUserAdGraphSimilarityEngine)
  def providesConsumersBasedUserAdGraphSimilarityEngine(
    @Named(ModuleNames.ConsumerBasedUserAdGraphStore)
    consumersBasedUserAdGraphStore: ReadableStore[
      ConsumersBasedRelatedAdRequest,
      RelatedAdResponse
    ],
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
    decider: CrMixerDecider
  ): StandardSimilarityEngine[
    ConsumersBasedUserAdGraphSimilarityEngine.Query,
    TweetWithScore
  ] = {

    new StandardSimilarityEngine[
      ConsumersBasedUserAdGraphSimilarityEngine.Query,
      TweetWithScore
    ](
      implementingStore =
        ConsumersBasedUserAdGraphSimilarityEngine(consumersBasedUserAdGraphStore, statsReceiver),
      identifier = SimilarityEngineType.ConsumersBasedUserTweetGraph,
      globalStats = statsReceiver,
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.similarityEngineTimeout,
        gatingConfig = GatingConfig(
          deciderConfig =
            Some(DeciderConfig(decider, DeciderConstants.enableUserTweetGraphTrafficDeciderKey)),
          enableFeatureSwitch = None
        )
      ),
      memCacheConfig = None
    )
  }
}
package com.twitter.cr_mixer.module
package similarity_engine

import com.google.inject.Provides
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.ModelConfig
import com.twitter.simclusters_v2.thriftscala.TweetsWithScore
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.similarity_engine.DiffusionBasedSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.DiffusionBasedSimilarityEngine.Query
import com.twitter.cr_mixer.similarity_engine.LookupSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.storehaus.ReadableStore
import javax.inject.Named
import javax.inject.Singleton

object DiffusionBasedSimilarityEngineModule extends TwitterModule {
  @Provides
  @Singleton
  @Named(ModuleNames.DiffusionBasedSimilarityEngine)
  def providesDiffusionBasedSimilarityEngineModule(
    @Named(ModuleNames.RetweetBasedDiffusionRecsMhStore)
    retweetBasedDiffusionRecsMhStore: ReadableStore[Long, TweetsWithScore],
    timeoutConfig: TimeoutConfig,
    globalStats: StatsReceiver
  ): LookupSimilarityEngine[Query, TweetWithScore] = {

    val versionedStoreMap = Map(
      ModelConfig.RetweetBasedDiffusion -> DiffusionBasedSimilarityEngine(
        retweetBasedDiffusionRecsMhStore,
        globalStats),
    )

    new LookupSimilarityEngine[Query, TweetWithScore](
      versionedStoreMap = versionedStoreMap,
      identifier = SimilarityEngineType.DiffusionBasedTweet,
      globalStats = globalStats,
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.similarityEngineTimeout,
        gatingConfig = GatingConfig(
          deciderConfig = None,
          enableFeatureSwitch = None
        )
      )
    )
  }
}
package com.twitter.cr_mixer.module.similarity_engine

import com.google.inject.Provides
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine._
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.keyHasher
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.DeciderConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.TweetBasedQigSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.qig_ranker.thriftscala.QigRanker
import javax.inject.Named
import javax.inject.Singleton

object TweetBasedQigSimilarityEngineModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.TweetBasedQigSimilarityEngine)
  def providesTweetBasedQigSimilarTweetsCandidateSource(
    qigRanker: QigRanker.MethodPerEndpoint,
    @Named(ModuleNames.UnifiedCache) crMixerUnifiedCacheClient: MemcachedClient,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
    decider: CrMixerDecider
  ): StandardSimilarityEngine[
    TweetBasedQigSimilarityEngine.Query,
    TweetWithScore
  ] = {
    new StandardSimilarityEngine[
      TweetBasedQigSimilarityEngine.Query,
      TweetWithScore
    ](
      implementingStore = TweetBasedQigSimilarityEngine(qigRanker, statsReceiver),
      identifier = SimilarityEngineType.Qig,
      globalStats = statsReceiver,
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.similarityEngineTimeout,
        gatingConfig = GatingConfig(
          deciderConfig =
            Some(DeciderConfig(decider, DeciderConstants.enableQigSimilarTweetsTrafficDeciderKey)),
          enableFeatureSwitch = None
        )
      ),
      memCacheConfig = Some(
        MemCacheConfig(
          cacheClient = crMixerUnifiedCacheClient,
          ttl = 10.minutes,
          keyToString = { k =>
            f"TweetBasedQIGRanker:${keyHasher.hashKey(k.sourceId.toString.getBytes)}%X"
          }
        )
      )
    )
  }
}
package com.twitter.cr_mixer.module.similarity_engine

import com.google.inject.Provides
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TopicTweetWithScore
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.similarity_engine.EngineQuery
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.DeciderConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.similarity_engine.SkitHighPrecisionTopicTweetSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SkitTopicTweetSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SkitTopicTweetSimilarityEngine.Query
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.storehaus.ReadableStore
import com.twitter.topic_recos.thriftscala.TopicTweet
import com.twitter.topic_recos.thriftscala.TopicTweetPartitionFlatKey
import javax.inject.Named
import javax.inject.Singleton

object SkitTopicTweetSimilarityEngineModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.SkitHighPrecisionTopicTweetSimilarityEngine)
  def providesSkitHighPrecisionTopicTweetSimilarityEngine(
    @Named(ModuleNames.SkitStratoStoreName) skitStratoStore: ReadableStore[
      TopicTweetPartitionFlatKey,
      Seq[TopicTweet]
    ],
    timeoutConfig: TimeoutConfig,
    decider: CrMixerDecider,
    statsReceiver: StatsReceiver
  ): StandardSimilarityEngine[
    EngineQuery[Query],
    TopicTweetWithScore
  ] = {
    new StandardSimilarityEngine[EngineQuery[Query], TopicTweetWithScore](
      implementingStore =
        SkitHighPrecisionTopicTweetSimilarityEngine(skitStratoStore, statsReceiver),
      identifier = SimilarityEngineType.SkitHighPrecisionTopicTweet,
      globalStats = statsReceiver.scope(SimilarityEngineType.SkitHighPrecisionTopicTweet.name),
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.topicTweetEndpointTimeout,
        gatingConfig = GatingConfig(
          deciderConfig =
            Some(DeciderConfig(decider, DeciderConstants.enableTopicTweetTrafficDeciderKey)),
          enableFeatureSwitch = None
        )
      )
    )
  }
  @Provides
  @Singleton
  @Named(ModuleNames.SkitTopicTweetSimilarityEngine)
  def providesSkitTfgTopicTweetSimilarityEngine(
    @Named(ModuleNames.SkitStratoStoreName) skitStratoStore: ReadableStore[
      TopicTweetPartitionFlatKey,
      Seq[TopicTweet]
    ],
    timeoutConfig: TimeoutConfig,
    decider: CrMixerDecider,
    statsReceiver: StatsReceiver
  ): StandardSimilarityEngine[
    EngineQuery[Query],
    TopicTweetWithScore
  ] = {
    new StandardSimilarityEngine[EngineQuery[Query], TopicTweetWithScore](
      implementingStore = SkitTopicTweetSimilarityEngine(skitStratoStore, statsReceiver),
      identifier = SimilarityEngineType.SkitTfgTopicTweet,
      globalStats = statsReceiver.scope(SimilarityEngineType.SkitTfgTopicTweet.name),
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.topicTweetEndpointTimeout,
        gatingConfig = GatingConfig(
          deciderConfig =
            Some(DeciderConfig(decider, DeciderConstants.enableTopicTweetTrafficDeciderKey)),
          enableFeatureSwitch = None
        )
      )
    )
  }

}
package com.twitter.cr_mixer.module.similarity_engine

import com.google.inject.Provides
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.DeciderConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.TweetBasedUserAdGraphSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.hashing.KeyHasher
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.recos.user_ad_graph.thriftscala.UserAdGraph
import com.twitter.relevance_platform.common.injection.LZ4Injection
import com.twitter.relevance_platform.common.injection.SeqObjectInjection
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.storehaus.ReadableStore
import com.twitter.twistly.thriftscala.TweetRecentEngagedUsers
import javax.inject.Named
import javax.inject.Singleton

object TweetBasedUserAdGraphSimilarityEngineModule extends TwitterModule {

  private val keyHasher: KeyHasher = KeyHasher.FNV1A_64

  @Provides
  @Singleton
  @Named(ModuleNames.TweetBasedUserAdGraphSimilarityEngine)
  def providesTweetBasedUserAdGraphSimilarityEngine(
    userAdGraphService: UserAdGraph.MethodPerEndpoint,
    tweetRecentEngagedUserStore: ReadableStore[TweetId, TweetRecentEngagedUsers],
    @Named(ModuleNames.UnifiedCache) crMixerUnifiedCacheClient: MemcachedClient,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
    decider: CrMixerDecider
  ): StandardSimilarityEngine[
    TweetBasedUserAdGraphSimilarityEngine.Query,
    TweetWithScore
  ] = {

    val underlyingStore = TweetBasedUserAdGraphSimilarityEngine(
      userAdGraphService,
      tweetRecentEngagedUserStore,
      statsReceiver)

    val memCachedStore: ReadableStore[
      TweetBasedUserAdGraphSimilarityEngine.Query,
      Seq[
        TweetWithScore
      ]
    ] =
      ObservedMemcachedReadableStore
        .fromCacheClient(
          backingStore = underlyingStore,
          cacheClient = crMixerUnifiedCacheClient,
          ttl = 10.minutes
        )(
          valueInjection = LZ4Injection.compose(SeqObjectInjection[TweetWithScore]()),
          statsReceiver = statsReceiver.scope("tweet_based_user_ad_graph_store_memcache"),
          keyToString = { k =>
            //Example Query CRMixer:TweetBasedUTG:1234567890ABCDEF
            f"CRMixer:TweetBasedUAG:${keyHasher.hashKey(k.toString.getBytes)}%X"
          }
        )

    new StandardSimilarityEngine[
      TweetBasedUserAdGraphSimilarityEngine.Query,
      TweetWithScore
    ](
      implementingStore = memCachedStore,
      identifier = SimilarityEngineType.TweetBasedUserAdGraph,
      globalStats = statsReceiver,
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.similarityEngineTimeout,
        gatingConfig = GatingConfig(
          deciderConfig =
            Some(DeciderConfig(decider, DeciderConstants.enableUserAdGraphTrafficDeciderKey)),
          enableFeatureSwitch = None
        )
      )
    )
  }
}
package com.twitter.cr_mixer.module.similarity_engine

import com.google.inject.Provides
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TweetWithScoreAndSocialProof
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.similarity_engine.UserTweetEntityGraphSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.DeciderConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.recos.user_tweet_entity_graph.thriftscala.UserTweetEntityGraph
import javax.inject.Named
import javax.inject.Singleton

object UserTweetEntityGraphSimilarityEngineModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.UserTweetEntityGraphSimilarityEngine)
  def providesUserTweetEntityGraphSimilarityEngine(
    userTweetEntityGraphService: UserTweetEntityGraph.MethodPerEndpoint,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
    decider: CrMixerDecider
  ): StandardSimilarityEngine[
    UserTweetEntityGraphSimilarityEngine.Query,
    TweetWithScoreAndSocialProof
  ] = {
    new StandardSimilarityEngine[
      UserTweetEntityGraphSimilarityEngine.Query,
      TweetWithScoreAndSocialProof
    ](
      implementingStore =
        UserTweetEntityGraphSimilarityEngine(userTweetEntityGraphService, statsReceiver),
      identifier = SimilarityEngineType.Uteg,
      globalStats = statsReceiver,
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.utegSimilarityEngineTimeout,
        gatingConfig = GatingConfig(
          deciderConfig = Some(
            DeciderConfig(decider, DeciderConstants.enableUserTweetEntityGraphTrafficDeciderKey)),
          enableFeatureSwitch = None
        )
      ),
      // We cannot use the key to cache anything in UTEG because the key contains a long list of userIds
      memCacheConfig = None
    )
  }
}
package com.twitter.cr_mixer.source_signal

import com.twitter.core_workflows.user_model.thriftscala.UserState
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.source_signal.SourceFetcher.FetcherQuery
import com.twitter.simclusters_v2.common.UserId
import com.twitter.timelines.configapi.Params
import com.twitter.cr_mixer.thriftscala.{Product => TProduct}
import com.twitter.finagle.GlobalRequestTimeoutException
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.mux.ServerApplicationError
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import com.twitter.util.TimeoutException
import org.apache.thrift.TApplicationException
import com.twitter.util.logging.Logging

/**
 * A SourceFetcher is a trait which, given a [[FetcherQuery]], returns [[ResultType]]
 * The main purposes of a SourceFetcher is to provide a consistent interface for source fetch
 * logic, and provides default functions, including:
 * - Identification
 * - Observability
 * - Timeout settings
 * - Exception Handling
 */
trait SourceFetcher[ResultType] extends ReadableStore[FetcherQuery, ResultType] with Logging {

  protected final val timer = com.twitter.finagle.util.DefaultTimer
  protected final def identifier: String = this.getClass.getSimpleName
  protected def stats: StatsReceiver
  protected def timeoutConfig: TimeoutConfig

  /***
   * Use FeatureSwitch to decide if a specific source is enabled.
   */
  def isEnabled(query: FetcherQuery): Boolean

  /***
   * This function fetches the raw sources and process them.
   * Custom stats tracking can be added depending on the type of ResultType
   */
  def fetchAndProcess(
    query: FetcherQuery,
  ): Future[Option[ResultType]]

  /***
   * Side-effect function to track stats for signal fetching and processing.
   */
  def trackStats(
    query: FetcherQuery
  )(
    func: => Future[Option[ResultType]]
  ): Future[Option[ResultType]]

  /***
   * This function is called by the top level class to fetch sources. It executes the pipeline to
   * fetch raw data, process and transform the sources. Exceptions, Stats, and timeout control are
   * handled here.
   */
  override def get(
    query: FetcherQuery
  ): Future[Option[ResultType]] = {
    val scopedStats = stats.scope(query.product.originalName)
    if (isEnabled(query)) {
      scopedStats.counter("gate_enabled").incr()
      trackStats(query)(fetchAndProcess(query))
        .raiseWithin(timeoutConfig.signalFetchTimeout)(timer)
        .onFailure { e =>
          scopedStats.scope("exceptions").counter(e.getClass.getSimpleName).incr()
        }
        .rescue {
          case _: TimeoutException | _: GlobalRequestTimeoutException | _: TApplicationException |
              _: ClientDiscardedRequestException |
              _: ServerApplicationError // TApplicationException inside
              =>
            Future.None
          case e =>
            logger.info(e)
            Future.None
        }
    } else {
      scopedStats.counter("gate_disabled").incr()
      Future.None
    }
  }
}

object SourceFetcher {

  /***
   * Every SourceFetcher all share the same input: FetcherQuery
   */
  case class FetcherQuery(
    userId: UserId,
    product: TProduct,
    userState: UserState,
    params: Params)

}
