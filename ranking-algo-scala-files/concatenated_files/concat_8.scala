package com.twitter.cr_mixer.source_signal

import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.source_signal.FrsStore.Query
import com.twitter.cr_mixer.source_signal.FrsStore.FrsQueryResult
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.follow_recommendations.thriftscala.ClientContext
import com.twitter.follow_recommendations.thriftscala.DisplayLocation
import com.twitter.follow_recommendations.thriftscala.FollowRecommendationsThriftService
import com.twitter.follow_recommendations.thriftscala.Recommendation
import com.twitter.follow_recommendations.thriftscala.RecommendationRequest
import com.twitter.storehaus.ReadableStore
import javax.inject.Singleton
import com.twitter.simclusters_v2.common.UserId
import com.twitter.util.Future

@Singleton
case class FrsStore(
  frsClient: FollowRecommendationsThriftService.MethodPerEndpoint,
  statsReceiver: StatsReceiver,
  decider: CrMixerDecider)
    extends ReadableStore[Query, Seq[FrsQueryResult]] {

  override def get(
    query: Query
  ): Future[Option[Seq[FrsQueryResult]]] = {
    if (decider.isAvailable(DeciderConstants.enableFRSTrafficDeciderKey)) {
      val recommendationRequest =
        buildFollowRecommendationRequest(query)

      frsClient
        .getRecommendations(recommendationRequest).map { recommendationResponse =>
          Some(recommendationResponse.recommendations.collect {
            case recommendation: Recommendation.User =>
              FrsQueryResult(
                recommendation.user.userId,
                recommendation.user.scoringDetails
                  .flatMap(_.score).getOrElse(0.0),
                recommendation.user.scoringDetails
                  .flatMap(_.candidateSourceDetails.flatMap(_.primarySource)),
                recommendation.user.scoringDetails
                  .flatMap(_.candidateSourceDetails.flatMap(_.candidateSourceScores)).map(_.toMap)
              )
          })
        }
    } else {
      Future.None
    }
  }

  private def buildFollowRecommendationRequest(
    query: Query
  ): RecommendationRequest = {
    RecommendationRequest(
      clientContext = ClientContext(
        userId = Some(query.userId),
        countryCode = query.countryCodeOpt,
        languageCode = query.languageCodeOpt),
      displayLocation = query.displayLocation,
      maxResults = Some(query.maxConsumerSeedsNum),
      excludedIds = Some(query.excludedUserIds)
    )
  }
}

object FrsStore {
  case class Query(
    userId: UserId,
    maxConsumerSeedsNum: Int,
    displayLocation: DisplayLocation = DisplayLocation.ContentRecommender,
    excludedUserIds: Seq[UserId] = Seq.empty,
    languageCodeOpt: Option[String] = None,
    countryCodeOpt: Option[String] = None)

  case class FrsQueryResult(
    userId: UserId,
    score: Double,
    primarySource: Option[Int],
    sourceWithScores: Option[Map[String, Double]])
}
package com.twitter.cr_mixer.module
package similarity_engine

import com.google.inject.Provides
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.model.ModelConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.similarity_engine.LookupSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.TwhinCollabFilterSimilarityEngine.Query
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.similarity_engine.TwhinCollabFilterSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.storehaus.ReadableStore
import javax.inject.Named
import javax.inject.Singleton

/**
 * TwhinCandidatesLookupSimilarityEngineModule routes the request to the corresponding
 * twhin based candidate store which follow the same pattern as TwHIN Collaborative Filtering.
 */

object TwhinCollabFilterLookupSimilarityEngineModule extends TwitterModule {
  @Provides
  @Singleton
  @Named(ModuleNames.TwhinCollabFilterSimilarityEngine)
  def providesTwhinCollabFilterLookupSimilarityEngineModule(
    @Named(ModuleNames.TwhinCollabFilterStratoStoreForFollow)
    twhinCollabFilterStratoStoreForFollow: ReadableStore[Long, Seq[TweetId]],
    @Named(ModuleNames.TwhinCollabFilterStratoStoreForEngagement)
    twhinCollabFilterStratoStoreForEngagement: ReadableStore[Long, Seq[TweetId]],
    @Named(ModuleNames.TwhinMultiClusterStratoStoreForFollow)
    twhinMultiClusterStratoStoreForFollow: ReadableStore[Long, Seq[TweetId]],
    @Named(ModuleNames.TwhinMultiClusterStratoStoreForEngagement)
    twhinMultiClusterStratoStoreForEngagement: ReadableStore[Long, Seq[TweetId]],
    timeoutConfig: TimeoutConfig,
    globalStats: StatsReceiver
  ): LookupSimilarityEngine[Query, TweetWithScore] = {
    val versionedStoreMap = Map(
      ModelConfig.TwhinCollabFilterForFollow -> TwhinCollabFilterSimilarityEngine(
        twhinCollabFilterStratoStoreForFollow,
        globalStats),
      ModelConfig.TwhinCollabFilterForEngagement -> TwhinCollabFilterSimilarityEngine(
        twhinCollabFilterStratoStoreForEngagement,
        globalStats),
      ModelConfig.TwhinMultiClusterForFollow -> TwhinCollabFilterSimilarityEngine(
        twhinMultiClusterStratoStoreForFollow,
        globalStats),
      ModelConfig.TwhinMultiClusterForEngagement -> TwhinCollabFilterSimilarityEngine(
        twhinMultiClusterStratoStoreForEngagement,
        globalStats),
    )

    new LookupSimilarityEngine[Query, TweetWithScore](
      versionedStoreMap = versionedStoreMap,
      identifier = SimilarityEngineType.TwhinCollabFilter,
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
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.similarity_engine.ProducerBasedUserAdGraphSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.DeciderConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine._
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.keyHasher
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.recos.user_ad_graph.thriftscala.UserAdGraph
import javax.inject.Named
import javax.inject.Singleton

object ProducerBasedUserAdGraphSimilarityEngineModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.ProducerBasedUserAdGraphSimilarityEngine)
  def providesProducerBasedUserAdGraphSimilarityEngine(
    userAdGraphService: UserAdGraph.MethodPerEndpoint,
    @Named(ModuleNames.UnifiedCache) crMixerUnifiedCacheClient: MemcachedClient,
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver,
    decider: CrMixerDecider
  ): StandardSimilarityEngine[
    ProducerBasedUserAdGraphSimilarityEngine.Query,
    TweetWithScore
  ] = {
    new StandardSimilarityEngine[
      ProducerBasedUserAdGraphSimilarityEngine.Query,
      TweetWithScore
    ](
      implementingStore =
        ProducerBasedUserAdGraphSimilarityEngine(userAdGraphService, statsReceiver),
      identifier = SimilarityEngineType.ProducerBasedUserAdGraph,
      globalStats = statsReceiver,
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.similarityEngineTimeout,
        gatingConfig = GatingConfig(
          deciderConfig =
            Some(DeciderConfig(decider, DeciderConstants.enableUserAdGraphTrafficDeciderKey)),
          enableFeatureSwitch = None
        )
      ),
      memCacheConfig = Some(
        MemCacheConfig(
          cacheClient = crMixerUnifiedCacheClient,
          ttl = 10.minutes,
          keyToString = { k =>
            //Example Query CRMixer:ProducerBasedUTG:1234567890ABCDEF
            f"ProducerBasedUTG:${keyHasher.hashKey(k.toString.getBytes)}%X"
          }
        ))
    )
  }
}
package com.twitter.cr_mixer.module.similarity_engine

import com.google.inject.Provides
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.similarity_engine.SimClustersANNSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimClustersANNSimilarityEngine.Query
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.hashing.KeyHasher
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.relevance_platform.common.injection.LZ4Injection
import com.twitter.relevance_platform.common.injection.SeqObjectInjection
import com.twitter.simclusters_v2.candidate_source.SimClustersANNCandidateSource.CacheableShortTTLEmbeddingTypes
import com.twitter.simclustersann.thriftscala.SimClustersANNService
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import javax.inject.Named
import javax.inject.Singleton

object SimClustersANNSimilarityEngineModule extends TwitterModule {

  private val keyHasher: KeyHasher = KeyHasher.FNV1A_64

  @Provides
  @Singleton
  @Named(ModuleNames.SimClustersANNSimilarityEngine)
  def providesProdSimClustersANNSimilarityEngine(
    @Named(ModuleNames.UnifiedCache) crMixerUnifiedCacheClient: MemcachedClient,
    simClustersANNServiceNameToClientMapper: Map[String, SimClustersANNService.MethodPerEndpoint],
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver
  ): StandardSimilarityEngine[Query, TweetWithScore] = {

    val underlyingStore =
      SimClustersANNSimilarityEngine(simClustersANNServiceNameToClientMapper, statsReceiver)

    val observedReadableStore =
      ObservedReadableStore(underlyingStore)(statsReceiver.scope("SimClustersANNServiceStore"))

    val memCachedStore: ReadableStore[Query, Seq[TweetWithScore]] =
      ObservedMemcachedReadableStore
        .fromCacheClient(
          backingStore = observedReadableStore,
          cacheClient = crMixerUnifiedCacheClient,
          ttl = 10.minutes
        )(
          valueInjection = LZ4Injection.compose(SeqObjectInjection[TweetWithScore]()),
          statsReceiver = statsReceiver.scope("simclusters_ann_store_memcache"),
          keyToString = { k =>
            //Example Query CRMixer:SCANN:1:2:1234567890ABCDEF:1234567890ABCDEF
            f"CRMixer:SCANN:${k.simClustersANNQuery.sourceEmbeddingId.embeddingType.getValue()}%X" +
              f":${k.simClustersANNQuery.sourceEmbeddingId.modelVersion.getValue()}%X" +
              f":${keyHasher.hashKey(k.simClustersANNQuery.sourceEmbeddingId.internalId.toString.getBytes)}%X" +
              f":${keyHasher.hashKey(k.simClustersANNQuery.config.toString.getBytes)}%X"
          }
        )

    // Only cache the candidates if it's not Consumer-source. For example, TweetSource,
    // ProducerSource, TopicSource
    val wrapperStats = statsReceiver.scope("SimClustersANNWrapperStore")

    val wrapperStore: ReadableStore[Query, Seq[TweetWithScore]] =
      buildWrapperStore(memCachedStore, observedReadableStore, wrapperStats)

    new StandardSimilarityEngine[
      Query,
      TweetWithScore
    ](
      implementingStore = wrapperStore,
      identifier = SimilarityEngineType.SimClustersANN,
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

  def buildWrapperStore(
    memCachedStore: ReadableStore[Query, Seq[TweetWithScore]],
    underlyingStore: ReadableStore[Query, Seq[TweetWithScore]],
    wrapperStats: StatsReceiver
  ): ReadableStore[Query, Seq[TweetWithScore]] = {

    // Only cache the candidates if it's not Consumer-source. For example, TweetSource,
    // ProducerSource, TopicSource
    val wrapperStore: ReadableStore[Query, Seq[TweetWithScore]] =
      new ReadableStore[Query, Seq[TweetWithScore]] {

        override def multiGet[K1 <: Query](
          queries: Set[K1]
        ): Map[K1, Future[Option[Seq[TweetWithScore]]]] = {
          val (cacheableQueries, nonCacheableQueries) =
            queries.partition { query =>
              CacheableShortTTLEmbeddingTypes.contains(
                query.simClustersANNQuery.sourceEmbeddingId.embeddingType)
            }
          memCachedStore.multiGet(cacheableQueries) ++
            underlyingStore.multiGet(nonCacheableQueries)
        }
      }
    wrapperStore
  }

}
package com.twitter.cr_mixer.module.similarity_engine

import com.google.inject.Provides
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.similarity_engine.EarlybirdModelBasedSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.EarlybirdRecencyBasedSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.EarlybirdSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.EarlybirdTensorflowBasedSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.DeciderConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import javax.inject.Singleton

object EarlybirdSimilarityEngineModule extends TwitterModule {

  @Provides
  @Singleton
  def providesRecencyBasedEarlybirdSimilarityEngine(
    earlybirdRecencyBasedSimilarityEngine: EarlybirdRecencyBasedSimilarityEngine,
    timeoutConfig: TimeoutConfig,
    decider: CrMixerDecider,
    statsReceiver: StatsReceiver
  ): EarlybirdSimilarityEngine[
    EarlybirdRecencyBasedSimilarityEngine.EarlybirdRecencyBasedSearchQuery,
    EarlybirdRecencyBasedSimilarityEngine
  ] = {
    new EarlybirdSimilarityEngine[
      EarlybirdRecencyBasedSimilarityEngine.EarlybirdRecencyBasedSearchQuery,
      EarlybirdRecencyBasedSimilarityEngine
    ](
      implementingStore = earlybirdRecencyBasedSimilarityEngine,
      identifier = SimilarityEngineType.EarlybirdRecencyBasedSimilarityEngine,
      globalStats =
        statsReceiver.scope(SimilarityEngineType.EarlybirdRecencyBasedSimilarityEngine.name),
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.earlybirdSimilarityEngineTimeout,
        gatingConfig = GatingConfig(
          deciderConfig = Some(
            DeciderConfig(
              decider = decider,
              deciderString = DeciderConstants.enableEarlybirdTrafficDeciderKey
            )),
          enableFeatureSwitch = None
        )
      )
    )
  }

  @Provides
  @Singleton
  def providesModelBasedEarlybirdSimilarityEngine(
    earlybirdModelBasedSimilarityEngine: EarlybirdModelBasedSimilarityEngine,
    timeoutConfig: TimeoutConfig,
    decider: CrMixerDecider,
    statsReceiver: StatsReceiver
  ): EarlybirdSimilarityEngine[
    EarlybirdModelBasedSimilarityEngine.EarlybirdModelBasedSearchQuery,
    EarlybirdModelBasedSimilarityEngine
  ] = {
    new EarlybirdSimilarityEngine[
      EarlybirdModelBasedSimilarityEngine.EarlybirdModelBasedSearchQuery,
      EarlybirdModelBasedSimilarityEngine
    ](
      implementingStore = earlybirdModelBasedSimilarityEngine,
      identifier = SimilarityEngineType.EarlybirdModelBasedSimilarityEngine,
      globalStats =
        statsReceiver.scope(SimilarityEngineType.EarlybirdModelBasedSimilarityEngine.name),
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.earlybirdSimilarityEngineTimeout,
        gatingConfig = GatingConfig(
          deciderConfig = Some(
            DeciderConfig(
              decider = decider,
              deciderString = DeciderConstants.enableEarlybirdTrafficDeciderKey
            )),
          enableFeatureSwitch = None
        )
      )
    )
  }

  @Provides
  @Singleton
  def providesTensorflowBasedEarlybirdSimilarityEngine(
    earlybirdTensorflowBasedSimilarityEngine: EarlybirdTensorflowBasedSimilarityEngine,
    timeoutConfig: TimeoutConfig,
    decider: CrMixerDecider,
    statsReceiver: StatsReceiver
  ): EarlybirdSimilarityEngine[
    EarlybirdTensorflowBasedSimilarityEngine.EarlybirdTensorflowBasedSearchQuery,
    EarlybirdTensorflowBasedSimilarityEngine
  ] = {
    new EarlybirdSimilarityEngine[
      EarlybirdTensorflowBasedSimilarityEngine.EarlybirdTensorflowBasedSearchQuery,
      EarlybirdTensorflowBasedSimilarityEngine
    ](
      implementingStore = earlybirdTensorflowBasedSimilarityEngine,
      identifier = SimilarityEngineType.EarlybirdTensorflowBasedSimilarityEngine,
      globalStats =
        statsReceiver.scope(SimilarityEngineType.EarlybirdTensorflowBasedSimilarityEngine.name),
      engineConfig = SimilarityEngineConfig(
        timeout = timeoutConfig.earlybirdSimilarityEngineTimeout,
        gatingConfig = GatingConfig(
          deciderConfig = Some(
            DeciderConfig(
              decider = decider,
              deciderString = DeciderConstants.enableEarlybirdTrafficDeciderKey
            )),
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
import com.twitter.cr_mixer.model.TopicTweetWithScore
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.similarity_engine.CertoTopicTweetSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.CertoTopicTweetSimilarityEngine.Query
import com.twitter.cr_mixer.similarity_engine.EngineQuery
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.DeciderConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.thriftscala.TopicId
import com.twitter.storehaus.ReadableStore
import com.twitter.topic_recos.thriftscala.TweetWithScores
import javax.inject.Named
import javax.inject.Singleton

object CertoTopicTweetSimilarityEngineModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.CertoTopicTweetSimilarityEngine)
  def providesCertoTopicTweetSimilarityEngine(
    @Named(ModuleNames.CertoStratoStoreName) certoStratoStore: ReadableStore[
      TopicId,
      Seq[TweetWithScores]
    ],
    timeoutConfig: TimeoutConfig,
    decider: CrMixerDecider,
    statsReceiver: StatsReceiver
  ): StandardSimilarityEngine[
    EngineQuery[Query],
    TopicTweetWithScore
  ] = {
    new StandardSimilarityEngine[EngineQuery[Query], TopicTweetWithScore](
      implementingStore = CertoTopicTweetSimilarityEngine(certoStratoStore, statsReceiver),
      identifier = SimilarityEngineType.CertoTopicTweet,
      globalStats = statsReceiver,
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
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType

object ConsumerEmbeddingBasedTwHINSimilarityEngineModule extends TwitterModule {
  @Provides
  @Named(ModuleNames.ConsumerEmbeddingBasedTwHINANNSimilarityEngine)
  def providesConsumerEmbeddingBasedTwHINANNSimilarityEngine(
    // MH stores
    @Named(EmbeddingStoreModule.ConsumerBasedTwHINEmbeddingRegularUpdateMhStoreName)
    consumerBasedTwHINEmbeddingRegularUpdateMhStore: ReadableStore[InternalId, api.Embedding],
    @Named(EmbeddingStoreModule.DebuggerDemoUserEmbeddingMhStoreName)
    debuggerDemoUserEmbeddingMhStore: ReadableStore[InternalId, api.Embedding],
    @Named(AnnQueryServiceClientModule.TwHINRegularUpdateAnnServiceClientName)
    twHINRegularUpdateAnnService: AnnQueryService.MethodPerEndpoint,
    @Named(AnnQueryServiceClientModule.DebuggerDemoAnnServiceClientName)
    debuggerDemoAnnService: AnnQueryService.MethodPerEndpoint,
    // Other configs
    timeoutConfig: TimeoutConfig,
    statsReceiver: StatsReceiver
  ): HnswANNSimilarityEngine = {
    new HnswANNSimilarityEngine(
      embeddingStoreLookUpMap = Map(
        ModelConfig.ConsumerBasedTwHINRegularUpdateAll20221024 -> consumerBasedTwHINEmbeddingRegularUpdateMhStore,
        ModelConfig.DebuggerDemo -> debuggerDemoUserEmbeddingMhStore,
      ),
      annServiceLookUpMap = Map(
        ModelConfig.ConsumerBasedTwHINRegularUpdateAll20221024 -> twHINRegularUpdateAnnService,
        ModelConfig.DebuggerDemo -> debuggerDemoAnnService,
      ),
      globalStats = statsReceiver,
      identifier = SimilarityEngineType.ConsumerEmbeddingBasedTwHINANN,
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
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.inject.TwitterModule
import com.twitter.simclustersann.thriftscala.SimClustersANNService
import javax.inject.Named

object SimClustersANNServiceNameToClientMapper extends TwitterModule {

  @Provides
  @Singleton
  def providesSimClustersANNServiceNameToClientMapping(
    @Named(ModuleNames.ProdSimClustersANNServiceClientName) simClustersANNServiceProd: SimClustersANNService.MethodPerEndpoint,
    @Named(ModuleNames.ExperimentalSimClustersANNServiceClientName) simClustersANNServiceExperimental: SimClustersANNService.MethodPerEndpoint,
    @Named(ModuleNames.SimClustersANNServiceClientName1) simClustersANNService1: SimClustersANNService.MethodPerEndpoint,
    @Named(ModuleNames.SimClustersANNServiceClientName2) simClustersANNService2: SimClustersANNService.MethodPerEndpoint,
    @Named(ModuleNames.SimClustersANNServiceClientName3) simClustersANNService3: SimClustersANNService.MethodPerEndpoint,
    @Named(ModuleNames.SimClustersANNServiceClientName5) simClustersANNService5: SimClustersANNService.MethodPerEndpoint,
    @Named(ModuleNames.SimClustersANNServiceClientName4) simClustersANNService4: SimClustersANNService.MethodPerEndpoint
  ): Map[String, SimClustersANNService.MethodPerEndpoint] = {
    Map[String, SimClustersANNService.MethodPerEndpoint](
      "simclusters-ann" -> simClustersANNServiceProd,
      "simclusters-ann-experimental" -> simClustersANNServiceExperimental,
      "simclusters-ann-1" -> simClustersANNService1,
      "simclusters-ann-2" -> simClustersANNService2,
      "simclusters-ann-3" -> simClustersANNService3,
      "simclusters-ann-5" -> simClustersANNService5,
      "simclusters-ann-4" -> simClustersANNService4
    )
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Module
import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.bijection.scrooge.BinaryScalaCodec
import com.twitter.contentrecommender.thriftscala.TweetInfo
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.frigate.common.store.health.TweetHealthModelStore
import com.twitter.frigate.common.store.health.TweetHealthModelStore.TweetHealthModelStoreConfig
import com.twitter.frigate.common.store.health.UserHealthModelStore
import com.twitter.frigate.thriftscala.TweetHealthScores
import com.twitter.frigate.thriftscala.UserAgathaScores
import com.twitter.hermit.store.common.DeciderableReadableStore
import com.twitter.hermit.store.common.ObservedCachedReadableStore
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.strato.client.{Client => StratoClient}
import com.twitter.contentrecommender.store.TweetInfoStore
import com.twitter.contentrecommender.store.TweetyPieFieldsStore
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderKey
import com.twitter.frigate.data_pipeline.scalding.thriftscala.BlueVerifiedAnnotationsV2
import com.twitter.recos.user_tweet_graph_plus.thriftscala.UserTweetGraphPlus
import com.twitter.recos.user_tweet_graph_plus.thriftscala.TweetEngagementScores
import com.twitter.relevance_platform.common.health_store.UserMediaRepresentationHealthStore
import com.twitter.relevance_platform.common.health_store.MagicRecsRealTimeAggregatesStore
import com.twitter.relevance_platform.thriftscala.MagicRecsRealTimeAggregatesScores
import com.twitter.relevance_platform.thriftscala.UserMediaRepresentationScores
import com.twitter.storage.client.manhattan.kv.ManhattanKVClientMtlsParams
import com.twitter.tweetypie.thriftscala.TweetService
import com.twitter.util.Future
import com.twitter.util.JavaTimer
import com.twitter.util.Timer

import javax.inject.Named

object TweetInfoStoreModule extends TwitterModule {
  implicit val timer: Timer = new JavaTimer(true)
  override def modules: Seq[Module] = Seq(UnifiedCacheClient)

  @Provides
  @Singleton
  def providesTweetInfoStore(
    statsReceiver: StatsReceiver,
    serviceIdentifier: ServiceIdentifier,
    stratoClient: StratoClient,
    @Named(ModuleNames.UnifiedCache) crMixerUnifiedCacheClient: MemcachedClient,
    manhattanKVClientMtlsParams: ManhattanKVClientMtlsParams,
    tweetyPieService: TweetService.MethodPerEndpoint,
    userTweetGraphPlusService: UserTweetGraphPlus.MethodPerEndpoint,
    @Named(ModuleNames.BlueVerifiedAnnotationStore) blueVerifiedAnnotationStore: ReadableStore[
      String,
      BlueVerifiedAnnotationsV2
    ],
    decider: CrMixerDecider
  ): ReadableStore[TweetId, TweetInfo] = {

    val tweetEngagementScoreStore: ReadableStore[TweetId, TweetEngagementScores] = {
      val underlyingStore =
        ObservedReadableStore(new ReadableStore[TweetId, TweetEngagementScores] {
          override def get(
            k: TweetId
          ): Future[Option[TweetEngagementScores]] = {
            userTweetGraphPlusService.tweetEngagementScore(k).map {
              Some(_)
            }
          }
        })(statsReceiver.scope("UserTweetGraphTweetEngagementScoreStore"))

      DeciderableReadableStore(
        underlyingStore,
        decider.deciderGateBuilder.idGate(
          DeciderKey.enableUtgRealTimeTweetEngagementScoreDeciderKey),
        statsReceiver.scope("UserTweetGraphTweetEngagementScoreStore")
      )

    }

    val tweetHealthModelStore: ReadableStore[TweetId, TweetHealthScores] = {
      val underlyingStore = TweetHealthModelStore.buildReadableStore(
        stratoClient,
        Some(
          TweetHealthModelStoreConfig(
            enablePBlock = true,
            enableToxicity = true,
            enablePSpammy = true,
            enablePReported = true,
            enableSpammyTweetContent = true,
            enablePNegMultimodal = true,
          ))
      )(statsReceiver.scope("UnderlyingTweetHealthModelStore"))

      DeciderableReadableStore(
        ObservedMemcachedReadableStore.fromCacheClient(
          backingStore = underlyingStore,
          cacheClient = crMixerUnifiedCacheClient,
          ttl = 2.hours
        )(
          valueInjection = BinaryScalaCodec(TweetHealthScores),
          statsReceiver = statsReceiver.scope("memCachedTweetHealthModelStore"),
          keyToString = { k: TweetId => s"tHMS/$k" }
        ),
        decider.deciderGateBuilder.idGate(DeciderKey.enableHealthSignalsScoreDeciderKey),
        statsReceiver.scope("TweetHealthModelStore")
      ) // use s"tHMS/$k" instead of s"tweetHealthModelStore/$k" to differentiate from CR cache
    }

    val userHealthModelStore: ReadableStore[UserId, UserAgathaScores] = {
      val underlyingStore = UserHealthModelStore.buildReadableStore(stratoClient)(
        statsReceiver.scope("UnderlyingUserHealthModelStore"))
      DeciderableReadableStore(
        ObservedMemcachedReadableStore.fromCacheClient(
          backingStore = underlyingStore,
          cacheClient = crMixerUnifiedCacheClient,
          ttl = 18.hours
        )(
          valueInjection = BinaryScalaCodec(UserAgathaScores),
          statsReceiver = statsReceiver.scope("memCachedUserHealthModelStore"),
          keyToString = { k: UserId => s"uHMS/$k" }
        ),
        decider.deciderGateBuilder.idGate(DeciderKey.enableUserAgathaScoreDeciderKey),
        statsReceiver.scope("UserHealthModelStore")
      )
    }

    val userMediaRepresentationHealthStore: ReadableStore[UserId, UserMediaRepresentationScores] = {
      val underlyingStore =
        UserMediaRepresentationHealthStore.buildReadableStore(
          manhattanKVClientMtlsParams,
          statsReceiver.scope("UnderlyingUserMediaRepresentationHealthStore")
        )
      DeciderableReadableStore(
        ObservedMemcachedReadableStore.fromCacheClient(
          backingStore = underlyingStore,
          cacheClient = crMixerUnifiedCacheClient,
          ttl = 12.hours
        )(
          valueInjection = BinaryScalaCodec(UserMediaRepresentationScores),
          statsReceiver = statsReceiver.scope("memCacheUserMediaRepresentationHealthStore"),
          keyToString = { k: UserId => s"uMRHS/$k" }
        ),
        decider.deciderGateBuilder.idGate(DeciderKey.enableUserMediaRepresentationStoreDeciderKey),
        statsReceiver.scope("UserMediaRepresentationHealthStore")
      )
    }

    val magicRecsRealTimeAggregatesStore: ReadableStore[
      TweetId,
      MagicRecsRealTimeAggregatesScores
    ] = {
      val underlyingStore =
        MagicRecsRealTimeAggregatesStore.buildReadableStore(
          serviceIdentifier,
          statsReceiver.scope("UnderlyingMagicRecsRealTimeAggregatesScores")
        )
      DeciderableReadableStore(
        underlyingStore,
        decider.deciderGateBuilder.idGate(DeciderKey.enableMagicRecsRealTimeAggregatesStore),
        statsReceiver.scope("MagicRecsRealTimeAggregatesStore")
      )
    }

    val tweetInfoStore: ReadableStore[TweetId, TweetInfo] = {
      val underlyingStore = TweetInfoStore(
        TweetyPieFieldsStore.getStoreFromTweetyPie(tweetyPieService),
        userMediaRepresentationHealthStore,
        magicRecsRealTimeAggregatesStore,
        tweetEngagementScoreStore,
        blueVerifiedAnnotationStore
      )(statsReceiver.scope("tweetInfoStore"))

      val memcachedStore = ObservedMemcachedReadableStore.fromCacheClient(
        backingStore = underlyingStore,
        cacheClient = crMixerUnifiedCacheClient,
        ttl = 15.minutes,
        // Hydrating tweetInfo is now a required step for all candidates,
        // hence we needed to tune these thresholds.
        asyncUpdate = serviceIdentifier.environment == "prod"
      )(
        valueInjection = BinaryScalaCodec(TweetInfo),
        statsReceiver = statsReceiver.scope("memCachedTweetInfoStore"),
        keyToString = { k: TweetId => s"tIS/$k" }
      )

      ObservedCachedReadableStore.from(
        memcachedStore,
        ttl = 15.minutes,
        maxKeys = 8388607, // Check TweetInfo definition. size~92b. Around 736 MB
        windowSize = 10000L,
        cacheName = "tweet_info_cache",
        maxMultiGetSize = 20
      )(statsReceiver.scope("inMemoryCachedTweetInfoStore"))
    }
    tweetInfoStore
  }
}
