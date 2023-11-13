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
