package com.twitter.cr_mixer

import com.twitter.finatra.http.routing.HttpWarmup
import com.twitter.finatra.httpclient.RequestBuilder._
import com.twitter.inject.Logging
import com.twitter.inject.utils.Handler
import com.twitter.util.Try
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class CrMixerHttpServerWarmupHandler @Inject() (warmup: HttpWarmup) extends Handler with Logging {

  override def handle(): Unit = {
    Try(warmup.send(get("/admin/cr-mixer/product-pipelines"), admin = true)())
      .onFailure(e => error(e.getMessage, e))
  }
}
package com.twitter.cr_mixer

import com.google.inject.Module
import com.twitter.cr_mixer.controller.CrMixerThriftController
import com.twitter.cr_mixer.featureswitch.SetImpressedBucketsLocalContextFilter
import com.twitter.cr_mixer.module.ActivePromotedTweetStoreModule
import com.twitter.cr_mixer.module.CertoStratoStoreModule
import com.twitter.cr_mixer.module.CrMixerParamConfigModule
import com.twitter.cr_mixer.module.EmbeddingStoreModule
import com.twitter.cr_mixer.module.FrsStoreModule
import com.twitter.cr_mixer.module.MHMtlsParamsModule
import com.twitter.cr_mixer.module.OfflineCandidateStoreModule
import com.twitter.cr_mixer.module.RealGraphStoreMhModule
import com.twitter.cr_mixer.module.RealGraphOonStoreModule
import com.twitter.cr_mixer.module.RepresentationManagerModule
import com.twitter.cr_mixer.module.RepresentationScorerModule
import com.twitter.cr_mixer.module.TweetInfoStoreModule
import com.twitter.cr_mixer.module.TweetRecentEngagedUserStoreModule
import com.twitter.cr_mixer.module.TweetRecommendationResultsStoreModule
import com.twitter.cr_mixer.module.TripCandidateStoreModule
import com.twitter.cr_mixer.module.TwhinCollabFilterStratoStoreModule
import com.twitter.cr_mixer.module.UserSignalServiceColumnModule
import com.twitter.cr_mixer.module.UserSignalServiceStoreModule
import com.twitter.cr_mixer.module.UserStateStoreModule
import com.twitter.cr_mixer.module.core.ABDeciderModule
import com.twitter.cr_mixer.module.core.CrMixerFlagModule
import com.twitter.cr_mixer.module.core.CrMixerLoggingABDeciderModule
import com.twitter.cr_mixer.module.core.FeatureContextBuilderModule
import com.twitter.cr_mixer.module.core.FeatureSwitchesModule
import com.twitter.cr_mixer.module.core.KafkaProducerModule
import com.twitter.cr_mixer.module.core.LoggerFactoryModule
import com.twitter.cr_mixer.module.similarity_engine.ConsumerEmbeddingBasedTripSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.ConsumerEmbeddingBasedTwHINSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.ConsumerEmbeddingBasedTwoTowerSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.ConsumersBasedUserAdGraphSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.ConsumersBasedUserVideoGraphSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.ProducerBasedUserAdGraphSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.ProducerBasedUserTweetGraphSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.ProducerBasedUnifiedSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.SimClustersANNSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.TweetBasedUnifiedSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.TweetBasedQigSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.TweetBasedTwHINSimlarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.TweetBasedUserAdGraphSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.TweetBasedUserTweetGraphSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.TweetBasedUserVideoGraphSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.TwhinCollabFilterLookupSimilarityEngineModule
import com.twitter.cr_mixer.module.ConsumersBasedUserAdGraphStoreModule
import com.twitter.cr_mixer.module.ConsumersBasedUserTweetGraphStoreModule
import com.twitter.cr_mixer.module.ConsumersBasedUserVideoGraphStoreModule
import com.twitter.cr_mixer.module.DiffusionStoreModule
import com.twitter.cr_mixer.module.EarlybirdRecencyBasedCandidateStoreModule
import com.twitter.cr_mixer.module.TwiceClustersMembersStoreModule
import com.twitter.cr_mixer.module.StrongTiePredictionStoreModule
import com.twitter.cr_mixer.module.thrift_client.AnnQueryServiceClientModule
import com.twitter.cr_mixer.module.thrift_client.EarlybirdSearchClientModule
import com.twitter.cr_mixer.module.thrift_client.FrsClientModule
import com.twitter.cr_mixer.module.thrift_client.QigServiceClientModule
import com.twitter.cr_mixer.module.thrift_client.SimClustersAnnServiceClientModule
import com.twitter.cr_mixer.module.thrift_client.TweetyPieClientModule
import com.twitter.cr_mixer.module.thrift_client.UserTweetGraphClientModule
import com.twitter.cr_mixer.module.thrift_client.UserTweetGraphPlusClientModule
import com.twitter.cr_mixer.module.thrift_client.UserVideoGraphClientModule
import com.twitter.cr_mixer.{thriftscala => st}
import com.twitter.finagle.Filter
import com.twitter.finatra.annotations.DarkTrafficFilterType
import com.twitter.finatra.decider.modules.DeciderModule
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.jackson.modules.ScalaObjectMapperModule
import com.twitter.finatra.mtls.http.{Mtls => HttpMtls}
import com.twitter.finatra.mtls.thriftmux.Mtls
import com.twitter.finatra.mtls.thriftmux.modules.MtlsThriftWebFormsModule
import com.twitter.finatra.thrift.ThriftServer
import com.twitter.finatra.thrift.filters._
import com.twitter.finatra.thrift.routing.ThriftRouter
import com.twitter.hydra.common.model_config.{ConfigModule => HydraConfigModule}
import com.twitter.inject.thrift.modules.ThriftClientIdModule
import com.twitter.product_mixer.core.module.LoggingThrowableExceptionMapper
import com.twitter.product_mixer.core.module.StratoClientModule
import com.twitter.product_mixer.core.module.product_mixer_flags.ProductMixerFlagModule
import com.twitter.relevance_platform.common.filters.ClientStatsFilter
import com.twitter.relevance_platform.common.filters.DarkTrafficFilterModule
import com.twitter.cr_mixer.module.SimClustersANNServiceNameToClientMapper
import com.twitter.cr_mixer.module.SkitStratoStoreModule
import com.twitter.cr_mixer.module.BlueVerifiedAnnotationStoreModule
import com.twitter.cr_mixer.module.core.TimeoutConfigModule
import com.twitter.cr_mixer.module.grpc_client.NaviGRPCClientModule
import com.twitter.cr_mixer.module.similarity_engine.CertoTopicTweetSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.ConsumerBasedWalsSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.DiffusionBasedSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.EarlybirdSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.SkitTopicTweetSimilarityEngineModule
import com.twitter.cr_mixer.module.similarity_engine.UserTweetEntityGraphSimilarityEngineModule
import com.twitter.cr_mixer.module.thrift_client.HydraPartitionClientModule
import com.twitter.cr_mixer.module.thrift_client.HydraRootClientModule
import com.twitter.cr_mixer.module.thrift_client.UserAdGraphClientModule
import com.twitter.cr_mixer.module.thrift_client.UserTweetEntityGraphClientModule
import com.twitter.thriftwebforms.MethodOptions

object CrMixerServerMain extends CrMixerServer

class CrMixerServer extends ThriftServer with Mtls with HttpServer with HttpMtls {
  override val name = "cr-mixer-server"

  private val coreModules = Seq(
    ABDeciderModule,
    CrMixerFlagModule,
    CrMixerLoggingABDeciderModule,
    CrMixerParamConfigModule,
    new DarkTrafficFilterModule[st.CrMixer.ReqRepServicePerEndpoint](),
    DeciderModule,
    FeatureContextBuilderModule,
    FeatureSwitchesModule,
    KafkaProducerModule,
    LoggerFactoryModule,
    MHMtlsParamsModule,
    ProductMixerFlagModule,
    ScalaObjectMapperModule,
    ThriftClientIdModule
  )

  private val thriftClientModules = Seq(
    AnnQueryServiceClientModule,
    EarlybirdSearchClientModule,
    FrsClientModule,
    HydraPartitionClientModule,
    HydraRootClientModule,
    QigServiceClientModule,
    SimClustersAnnServiceClientModule,
    TweetyPieClientModule,
    UserAdGraphClientModule,
    UserTweetEntityGraphClientModule,
    UserTweetGraphClientModule,
    UserTweetGraphPlusClientModule,
    UserVideoGraphClientModule,
  )

  private val grpcClientModules = Seq(
    NaviGRPCClientModule
  )

  // Modules sorted alphabetically, please keep the order when adding a new module
  override val modules: Seq[Module] =
    coreModules ++ thriftClientModules ++ grpcClientModules ++
      Seq(
        ActivePromotedTweetStoreModule,
        CertoStratoStoreModule,
        CertoTopicTweetSimilarityEngineModule,
        ConsumersBasedUserAdGraphSimilarityEngineModule,
        ConsumersBasedUserTweetGraphStoreModule,
        ConsumersBasedUserVideoGraphSimilarityEngineModule,
        ConsumersBasedUserVideoGraphStoreModule,
        ConsumerEmbeddingBasedTripSimilarityEngineModule,
        ConsumerEmbeddingBasedTwHINSimilarityEngineModule,
        ConsumerEmbeddingBasedTwoTowerSimilarityEngineModule,
        ConsumersBasedUserAdGraphStoreModule,
        ConsumerBasedWalsSimilarityEngineModule,
        DiffusionStoreModule,
        EmbeddingStoreModule,
        EarlybirdSimilarityEngineModule,
        EarlybirdRecencyBasedCandidateStoreModule,
        FrsStoreModule,
        HydraConfigModule,
        OfflineCandidateStoreModule,
        ProducerBasedUnifiedSimilarityEngineModule,
        ProducerBasedUserAdGraphSimilarityEngineModule,
        ProducerBasedUserTweetGraphSimilarityEngineModule,
        RealGraphOonStoreModule,
        RealGraphStoreMhModule,
        RepresentationManagerModule,
        RepresentationScorerModule,
        SimClustersANNServiceNameToClientMapper,
        SimClustersANNSimilarityEngineModule,
        SkitStratoStoreModule,
        SkitTopicTweetSimilarityEngineModule,
        StratoClientModule,
        StrongTiePredictionStoreModule,
        TimeoutConfigModule,
        TripCandidateStoreModule,
        TwiceClustersMembersStoreModule,
        TweetBasedQigSimilarityEngineModule,
        TweetBasedTwHINSimlarityEngineModule,
        TweetBasedUnifiedSimilarityEngineModule,
        TweetBasedUserAdGraphSimilarityEngineModule,
        TweetBasedUserTweetGraphSimilarityEngineModule,
        TweetBasedUserVideoGraphSimilarityEngineModule,
        TweetInfoStoreModule,
        TweetRecentEngagedUserStoreModule,
        TweetRecommendationResultsStoreModule,
        TwhinCollabFilterStratoStoreModule,
        TwhinCollabFilterLookupSimilarityEngineModule,
        UserSignalServiceColumnModule,
        UserSignalServiceStoreModule,
        UserStateStoreModule,
        UserTweetEntityGraphSimilarityEngineModule,
        DiffusionBasedSimilarityEngineModule,
        BlueVerifiedAnnotationStoreModule,
        new MtlsThriftWebFormsModule[st.CrMixer.MethodPerEndpoint](this) {
          override protected def defaultMethodAccess: MethodOptions.Access = {
            MethodOptions.Access.ByLdapGroup(
              Seq(
                "cr-mixer-admins",
                "recosplat-sensitive-data-medium",
                "recos-platform-admins",
              ))
          }
        }
      )

  def configureThrift(router: ThriftRouter): Unit = {
    router
      .filter[LoggingMDCFilter]
      .filter[TraceIdMDCFilter]
      .filter[ThriftMDCFilter]
      .filter[ClientStatsFilter]
      .filter[AccessLoggingFilter]
      .filter[SetImpressedBucketsLocalContextFilter]
      .filter[ExceptionMappingFilter]
      .filter[Filter.TypeAgnostic, DarkTrafficFilterType]
      .exceptionMapper[LoggingThrowableExceptionMapper]
      .add[CrMixerThriftController]
  }

  override protected def warmup(): Unit = {
    handle[CrMixerThriftServerWarmupHandler]()
    handle[CrMixerHttpServerWarmupHandler]()
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
package com.twitter.cr_mixer.model

/**
 * A Configuration class for all Model Based Candidate Sources.
 *
 * The Model Name Guideline. Please your modelId as "Algorithm_Product_Date"
 * If your model is used for multiple product surfaces, name it as all
 * Don't name your algorithm as MBCG. All the algorithms here are MBCG =.=
 *
 * Don't forgot to add your new models into allHnswANNSimilarityEngineModelIds list.
 */
object ModelConfig {
  // Offline SimClusters CG Experiment related Model Ids
  val OfflineInterestedInFromKnownFor2020: String = "OfflineIIKF_ALL_20220414"
  val OfflineInterestedInFromKnownFor2020Hl0El15: String = "OfflineIIKF_ALL_20220414_Hl0_El15"
  val OfflineInterestedInFromKnownFor2020Hl2El15: String = "OfflineIIKF_ALL_20220414_Hl2_El15"
  val OfflineInterestedInFromKnownFor2020Hl2El50: String = "OfflineIIKF_ALL_20220414_Hl2_El50"
  val OfflineInterestedInFromKnownFor2020Hl8El50: String = "OfflineIIKF_ALL_20220414_Hl8_El50"
  val OfflineMTSConsumerEmbeddingsFav90P20M: String =
    "OfflineMTSConsumerEmbeddingsFav90P20M_ALL_20220414"

  // Twhin Model Ids
  val ConsumerBasedTwHINRegularUpdateAll20221024: String =
    "ConsumerBasedTwHINRegularUpdate_All_20221024"

  // Averaged Twhin Model Ids
  val TweetBasedTwHINRegularUpdateAll20221024: String =
    "TweetBasedTwHINRegularUpdate_All_20221024"

  // Collaborative Filtering Twhin Model Ids
  val TwhinCollabFilterForFollow: String =
    "TwhinCollabFilterForFollow"
  val TwhinCollabFilterForEngagement: String =
    "TwhinCollabFilterForEngagement"
  val TwhinMultiClusterForFollow: String =
    "TwhinMultiClusterForFollow"
  val TwhinMultiClusterForEngagement: String =
    "TwhinMultiClusterForEngagement"

  // Two Tower model Ids
  val TwoTowerFavALL20220808: String =
    "TwoTowerFav_ALL_20220808"

  // Debugger Demo-Only Model Ids
  val DebuggerDemo: String = "DebuggerDemo"

  // ColdStartLookalike - this is not really a model name, it is as a placeholder to
  // indicate ColdStartLookalike candidate source, which is currently being pluged into
  // CustomizedRetrievalCandidateGeneration temporarily.
  val ColdStartLookalikeModelName: String = "ConsumersBasedUtgColdStartLookalike20220707"

  // consumersBasedUTG-RealGraphOon Model Id
  val ConsumersBasedUtgRealGraphOon20220705: String = "ConsumersBasedUtgRealGraphOon_All_20220705"
  // consumersBasedUAG-RealGraphOon Model Id
  val ConsumersBasedUagRealGraphOon20221205: String = "ConsumersBasedUagRealGraphOon_All_20221205"

  // FTR
  val OfflineFavDecayedSum: String = "OfflineFavDecayedSum"
  val OfflineFtrAt5Pop1000RnkDcy11: String = "OfflineFtrAt5Pop1000RnkDcy11"
  val OfflineFtrAt5Pop10000RnkDcy11: String = "OfflineFtrAt5Pop10000RnkDcy11"

  // All Model Ids of HnswANNSimilarityEngines
  val allHnswANNSimilarityEngineModelIds = Seq(
    ConsumerBasedTwHINRegularUpdateAll20221024,
    TwoTowerFavALL20220808,
    DebuggerDemo
  )

  val ConsumerLogFavBasedInterestedInEmbedding: String =
    "ConsumerLogFavBasedInterestedIn_ALL_20221228"
  val ConsumerFollowBasedInterestedInEmbedding: String =
    "ConsumerFollowBasedInterestedIn_ALL_20221228"

  val RetweetBasedDiffusion: String =
    "RetweetBasedDiffusion"

}
package com.twitter.cr_mixer.model

sealed trait EarlybirdSimilarityEngineType
object EarlybirdSimilarityEngineType_RecencyBased extends EarlybirdSimilarityEngineType
object EarlybirdSimilarityEngineType_ModelBased extends EarlybirdSimilarityEngineType
object EarlybirdSimilarityEngineType_TensorflowBased extends EarlybirdSimilarityEngineType
package com.twitter.cr_mixer.module.core

import com.google.inject.Provides
import com.google.inject.name.Named
import com.twitter.abdecider.ABDeciderFactory
import com.twitter.abdecider.LoggingABDecider
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.inject.TwitterModule
import com.twitter.inject.annotations.Flag
import com.twitter.logging.Logger
import javax.inject.Singleton

object ABDeciderModule extends TwitterModule {

  flag(
    name = "abdecider.path",
    default = "/usr/local/config/abdecider/abdecider.yml",
    help = "path to the abdecider Yml file location"
  )

  @Provides
  @Singleton
  def provideABDecider(
    @Flag("abdecider.path") abDeciderYmlPath: String,
    @Named(ModuleNames.AbDeciderLogger) scribeLogger: Logger
  ): LoggingABDecider = {
    ABDeciderFactory(
      abDeciderYmlPath = abDeciderYmlPath,
      scribeLogger = Some(scribeLogger),
      environment = Some("production")
    ).buildWithLogging()
  }
}
package com.twitter.cr_mixer.module.core

import com.google.inject.Provides
import com.twitter.cr_mixer.thriftscala.GetTweetsRecommendationsScribe
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.finatra.kafka.producers.FinagleKafkaProducerBuilder
import com.twitter.finatra.kafka.producers.KafkaProducerBase
import com.twitter.finatra.kafka.producers.NullKafkaProducer
import com.twitter.finatra.kafka.serde.ScalaSerdes
import com.twitter.inject.TwitterModule
import javax.inject.Singleton
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.Serdes

object KafkaProducerModule extends TwitterModule {

  @Provides
  @Singleton
  def provideTweetRecsLoggerFactory(
    serviceIdentifier: ServiceIdentifier,
  ): KafkaProducerBase[String, GetTweetsRecommendationsScribe] = {
    KafkaProducerFactory.getKafkaProducer(serviceIdentifier.environment)
  }
}

object KafkaProducerFactory {
  private val jaasConfig =
    """com.sun.security.auth.module.Krb5LoginModule
      |required 
      |principal="cr-mixer@TWITTER.BIZ" 
      |debug=true 
      |useKeyTab=true 
      |storeKey=true 
      |keyTab="/var/lib/tss/keys/fluffy/keytabs/client/cr-mixer.keytab" 
      |doNotPrompt=true;
    """.stripMargin.replaceAll("\n", " ")

  private val trustStoreLocation = "/etc/tw_truststore/messaging/kafka/client.truststore.jks"

  def getKafkaProducer(
    environment: String
  ): KafkaProducerBase[String, GetTweetsRecommendationsScribe] = {
    if (environment == "prod") {
      FinagleKafkaProducerBuilder()
        .dest("/s/kafka/recommendations:kafka-tls")
        // kerberos params
        .withConfig(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig)
        .withConfig(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
          SecurityProtocol.SASL_SSL.toString)
        .withConfig(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreLocation)
        .withConfig(SaslConfigs.SASL_MECHANISM, SaslConfigs.GSSAPI_MECHANISM)
        .withConfig(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka")
        .withConfig(SaslConfigs.SASL_KERBEROS_SERVER_NAME, "kafka")
        // Kafka params
        .keySerializer(Serdes.String.serializer)
        .valueSerializer(ScalaSerdes.CompactThrift[GetTweetsRecommendationsScribe].serializer())
        .clientId("cr-mixer")
        .enableIdempotence(true)
        .compressionType(CompressionType.LZ4)
        .build()
    } else {
      new NullKafkaProducer[String, GetTweetsRecommendationsScribe]
    }
  }
}
package com.twitter.cr_mixer.module.core

import com.twitter.finagle.stats.LoadedStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.servo.util.MemoizingStatsReceiver

object MemoizingStatsReceiverModule extends TwitterModule {
  override def configure(): Unit = {
    bind[StatsReceiver].toInstance(new MemoizingStatsReceiver(LoadedStatsReceiver))
  }
}
package com.twitter.cr_mixer.module.core

import com.google.inject.Provides
import com.twitter.abdecider.LoggingABDecider
import com.twitter.cr_mixer.featureswitch.CrMixerLoggingABDecider
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import javax.inject.Singleton

object CrMixerLoggingABDeciderModule extends TwitterModule {

  @Provides
  @Singleton
  def provideABDecider(
    loggingABDecider: LoggingABDecider,
    statsReceiver: StatsReceiver
  ): CrMixerLoggingABDecider = {
    CrMixerLoggingABDecider(loggingABDecider, statsReceiver)
  }
}
package com.twitter.cr_mixer.module.core

import com.google.inject.Provides
import com.twitter.discovery.common.configapi.FeatureContextBuilder
import com.twitter.featureswitches.v2.FeatureSwitches
import com.twitter.inject.TwitterModule
import javax.inject.Singleton

object FeatureContextBuilderModule extends TwitterModule {

  @Provides
  @Singleton
  def providesFeatureContextBuilder(featureSwitches: FeatureSwitches): FeatureContextBuilder = {
    FeatureContextBuilder(featureSwitches)
  }
}

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
