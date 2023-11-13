package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.LookupSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.GatingConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import javax.inject.Singleton

/**
 * In this example we build a [[StandardSimilarityEngine]] to wrap a dummy store
 */
object SimpleSimilarityEngineModule extends TwitterModule {
  @Provides
  @Singleton
  def providesSimpleSimilarityEngine(
    timeoutConfig: TimeoutConfig,
    globalStats: StatsReceiver
  ): StandardSimilarityEngine[UserId, (TweetId, Double)] = {
    // Inject your readableStore implementation here
    val dummyStore = ReadableStore.fromMap(
      Map(
        1L -> Seq((100L, 1.0), (101L, 1.0)),
        2L -> Seq((200L, 2.0), (201L, 2.0)),
        3L -> Seq((300L, 3.0), (301L, 3.0))
      ))

    new StandardSimilarityEngine[UserId, (TweetId, Double)](
      implementingStore = dummyStore,
      identifier = SimilarityEngineType.EnumUnknownSimilarityEngineType(9997),
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

/**
 * In this example we build a [[LookupSimilarityEngine]] to wrap a dummy store with 2 versions
 */
object LookupSimilarityEngineModule extends TwitterModule {
  @Provides
  @Singleton
  def providesLookupSimilarityEngine(
    timeoutConfig: TimeoutConfig,
    globalStats: StatsReceiver
  ): LookupSimilarityEngine[UserId, (TweetId, Double)] = {
    // Inject your readableStore implementation here
    val dummyStoreV1 = ReadableStore.fromMap(
      Map(
        1L -> Seq((100L, 1.0), (101L, 1.0)),
        2L -> Seq((200L, 2.0), (201L, 2.0)),
      ))

    val dummyStoreV2 = ReadableStore.fromMap(
      Map(
        1L -> Seq((100L, 1.0), (101L, 1.0)),
        2L -> Seq((200L, 2.0), (201L, 2.0)),
      ))

    new LookupSimilarityEngine[UserId, (TweetId, Double)](
      versionedStoreMap = Map(
        "V1" -> dummyStoreV1,
        "V2" -> dummyStoreV2
      ),
      identifier = SimilarityEngineType.EnumUnknownSimilarityEngineType(9998),
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
package com.twitter.cr_mixer.source_signal

import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.GraphSourceInfo
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.param.RealGraphOonParams
import com.twitter.cr_mixer.source_signal.SourceFetcher.FetcherQuery
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import com.twitter.wtf.candidate.thriftscala.CandidateSeq
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

/**
 * This store fetch user recommendations from RealGraphOON (go/realgraph) for a given userId
 */
@Singleton
case class RealGraphOonSourceGraphFetcher @Inject() (
  @Named(ModuleNames.RealGraphOonStore) realGraphOonStore: ReadableStore[UserId, CandidateSeq],
  override val timeoutConfig: TimeoutConfig,
  globalStats: StatsReceiver)
    extends SourceGraphFetcher {

  override protected val stats: StatsReceiver = globalStats.scope(identifier)
  override protected val graphSourceType: SourceType = SourceType.RealGraphOon

  override def isEnabled(query: FetcherQuery): Boolean = {
    query.params(RealGraphOonParams.EnableSourceGraphParam)
  }

  override def fetchAndProcess(
    query: FetcherQuery,
  ): Future[Option[GraphSourceInfo]] = {
    val rawSignals = trackPerItemStats(query)(
      realGraphOonStore.get(query.userId).map {
        _.map { candidateSeq =>
          candidateSeq.candidates
            .map { candidate =>
              // Bundle the userId with its score
              (candidate.userId, candidate.score)
            }.take(query.params(RealGraphOonParams.MaxConsumerSeedsNumParam))
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
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.app.Flag
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.store.strato.StratoFetchableStore
import com.twitter.hermit.store.common.ObservedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import javax.inject.Named
import javax.inject.Singleton
import com.twitter.strato.client.{Client => StratoClient}
import com.twitter.wtf.candidate.thriftscala.CandidateSeq

object RealGraphOonStoreModule extends TwitterModule {

  private val userRealGraphOonColumnPath: Flag[String] = flag[String](
    name = "crMixer.userRealGraphOonColumnPath",
    default = "recommendations/twistly/userRealgraphOon",
    help = "Strato column path for user real graph OON Store"
  )

  @Provides
  @Singleton
  @Named(ModuleNames.RealGraphOonStore)
  def providesRealGraphOonStore(
    stratoClient: StratoClient,
    statsReceiver: StatsReceiver
  ): ReadableStore[UserId, CandidateSeq] = {
    val realGraphOonStratoFetchableStore = StratoFetchableStore
      .withUnitView[UserId, CandidateSeq](stratoClient, userRealGraphOonColumnPath())

    ObservedReadableStore(
      realGraphOonStratoFetchableStore
    )(statsReceiver.scope("user_real_graph_oon_store"))
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.inject.TwitterModule
import com.twitter.storage.client.manhattan.kv.ManhattanKVClientMtlsParams
import javax.inject.Singleton

object MHMtlsParamsModule extends TwitterModule {
  @Singleton
  @Provides
  def providesManhattanMtlsParams(
    serviceIdentifier: ServiceIdentifier
  ): ManhattanKVClientMtlsParams = {
    ManhattanKVClientMtlsParams(serviceIdentifier)
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.inject.TwitterModule
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.frigate.common.store.strato.StratoFetchableStore
import com.twitter.cr_mixer.similarity_engine.TwhinCollabFilterSimilarityEngine.TwhinCollabFilterView
import com.twitter.strato.client.{Client => StratoClient}
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.storehaus.ReadableStore
import javax.inject.Named

object TwhinCollabFilterStratoStoreModule extends TwitterModule {

  val stratoColumnPath: String = "cuad/twhin/getCollabFilterTweetCandidatesProd.User"

  @Provides
  @Singleton
  @Named(ModuleNames.TwhinCollabFilterStratoStoreForFollow)
  def providesTwhinCollabFilterStratoStoreForFollow(
    stratoClient: StratoClient
  ): ReadableStore[Long, Seq[TweetId]] = {
    StratoFetchableStore.withView[Long, TwhinCollabFilterView, Seq[TweetId]](
      stratoClient,
      column = stratoColumnPath,
      view = TwhinCollabFilterView("follow_2022_03_10_c_500K")
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.TwhinCollabFilterStratoStoreForEngagement)
  def providesTwhinCollabFilterStratoStoreForEngagement(
    stratoClient: StratoClient
  ): ReadableStore[Long, Seq[TweetId]] = {
    StratoFetchableStore.withView[Long, TwhinCollabFilterView, Seq[TweetId]](
      stratoClient,
      column = stratoColumnPath,
      view = TwhinCollabFilterView("engagement_2022_04_10_c_500K"))
  }

  @Provides
  @Singleton
  @Named(ModuleNames.TwhinMultiClusterStratoStoreForFollow)
  def providesTwhinMultiClusterStratoStoreForFollow(
    stratoClient: StratoClient
  ): ReadableStore[Long, Seq[TweetId]] = {
    StratoFetchableStore.withView[Long, TwhinCollabFilterView, Seq[TweetId]](
      stratoClient,
      column = stratoColumnPath,
      view = TwhinCollabFilterView("multiclusterFollow20220921")
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.TwhinMultiClusterStratoStoreForEngagement)
  def providesTwhinMultiClusterStratoStoreForEngagement(
    stratoClient: StratoClient
  ): ReadableStore[Long, Seq[TweetId]] = {
    StratoFetchableStore.withView[Long, TwhinCollabFilterView, Seq[TweetId]](
      stratoClient,
      column = stratoColumnPath,
      view = TwhinCollabFilterView("multiclusterEng20220921"))
  }
}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.twitter.inject.TwitterModule
import com.twitter.simclusters_v2.common.UserId
import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.storage.client.manhattan.kv.ManhattanKVClientMtlsParams
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus_internal.manhattan.Apollo
import com.twitter.storehaus_internal.manhattan.ManhattanRO
import com.twitter.storehaus_internal.manhattan.ManhattanROConfig
import com.twitter.storehaus_internal.util.ApplicationID
import com.twitter.storehaus_internal.util.DatasetName
import com.twitter.storehaus_internal.util.HDFSPath
import com.twitter.bijection.scrooge.BinaryScalaCodec
import com.twitter.cr_mixer.param.decider.DeciderKey
import com.twitter.hermit.store.common.DeciderableReadableStore
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.wtf.candidate.thriftscala.CandidateSeq

object RealGraphStoreMhModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.RealGraphInStore)
  def providesRealGraphStoreMh(
    decider: CrMixerDecider,
    statsReceiver: StatsReceiver,
    manhattanKVClientMtlsParams: ManhattanKVClientMtlsParams,
    @Named(ModuleNames.UnifiedCache) crMixerUnifiedCacheClient: MemcachedClient,
  ): ReadableStore[UserId, CandidateSeq] = {

    implicit val valueCodec = new BinaryScalaCodec(CandidateSeq)
    val underlyingStore = ManhattanRO
      .getReadableStoreWithMtls[UserId, CandidateSeq](
        ManhattanROConfig(
          HDFSPath(""),
          ApplicationID("cr_mixer_apollo"),
          DatasetName("real_graph_scores_apollo"),
          Apollo),
        manhattanKVClientMtlsParams
      )

    val memCachedStore = ObservedMemcachedReadableStore
      .fromCacheClient(
        backingStore = underlyingStore,
        cacheClient = crMixerUnifiedCacheClient,
        ttl = 24.hours,
      )(
        valueInjection = valueCodec,
        statsReceiver = statsReceiver.scope("memCachedUserRealGraphMh"),
        keyToString = { k: UserId => s"uRGraph/$k" }
      )

    DeciderableReadableStore(
      memCachedStore,
      decider.deciderGateBuilder.idGate(DeciderKey.enableRealGraphMhStoreDeciderKey),
      statsReceiver.scope("RealGraphMh")
    )
  }
}
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
package com.twitter.cr_mixer.filter

import com.twitter.cr_mixer.model.CandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.UtegTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.param.UtegTweetGlobalParams
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import com.twitter.wtf.candidate.thriftscala.CandidateSeq

import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

/***
 * Filters in-network tweets
 */
@Singleton
case class InNetworkFilter @Inject() (
  @Named(ModuleNames.RealGraphInStore) realGraphStoreMh: ReadableStore[UserId, CandidateSeq],
  globalStats: StatsReceiver)
    extends FilterBase {
  override val name: String = this.getClass.getCanonicalName
  import InNetworkFilter._

  override type ConfigType = FilterConfig
  private val stats: StatsReceiver = globalStats.scope(this.getClass.getCanonicalName)
  private val filterCandidatesStats = stats.scope("filter_candidates")

  override def filter(
    candidates: Seq[Seq[InitialCandidate]],
    filterConfig: FilterConfig,
  ): Future[Seq[Seq[InitialCandidate]]] = {
    StatsUtil.trackItemsStats(filterCandidatesStats) {
      filterCandidates(candidates, filterConfig)
    }
  }

  private def filterCandidates(
    candidates: Seq[Seq[InitialCandidate]],
    filterConfig: FilterConfig,
  ): Future[Seq[Seq[InitialCandidate]]] = {

    if (!filterConfig.enableInNetworkFilter) {
      Future.value(candidates)
    } else {
      filterConfig.userIdOpt match {
        case Some(userId) =>
          realGraphStoreMh
            .get(userId).map(_.map(_.candidates.map(_.userId)).getOrElse(Seq.empty).toSet).map {
              realGraphInNetworkAuthorsSet =>
                candidates.map(_.filterNot { candidate =>
                  realGraphInNetworkAuthorsSet.contains(candidate.tweetInfo.authorId)
                })
            }
        case None => Future.value(candidates)
      }
    }
  }

  override def requestToConfig[CGQueryType <: CandidateGeneratorQuery](
    request: CGQueryType
  ): FilterConfig = {
    request match {
      case UtegTweetCandidateGeneratorQuery(userId, _, _, _, _, params, _) =>
        FilterConfig(Some(userId), params(UtegTweetGlobalParams.EnableInNetworkFilterParam))
      case _ => FilterConfig(None, false)
    }
  }
}

object InNetworkFilter {
  case class FilterConfig(
    userIdOpt: Option[UserId],
    enableInNetworkFilter: Boolean)
}
package com.twitter.cr_mixer.filter

import com.twitter.cr_mixer.model.CandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future

import javax.inject.Inject
import javax.inject.Singleton

/***
 *
 * Run filters sequentially for UTEG candidate generator. The structure is copied from PreRankFilterRunner.
 */
@Singleton
class UtegFilterRunner @Inject() (
  inNetworkFilter: InNetworkFilter,
  utegHealthFilter: UtegHealthFilter,
  retweetFilter: RetweetFilter,
  globalStats: StatsReceiver) {

  private val scopedStats = globalStats.scope(this.getClass.getCanonicalName)

  val orderedFilters: Seq[FilterBase] = Seq(
    inNetworkFilter,
    utegHealthFilter,
    retweetFilter
  )

  def runSequentialFilters[CGQueryType <: CandidateGeneratorQuery](
    request: CGQueryType,
    candidates: Seq[Seq[InitialCandidate]],
  ): Future[Seq[Seq[InitialCandidate]]] = {
    UtegFilterRunner.runSequentialFilters(
      request,
      candidates,
      orderedFilters,
      scopedStats
    )
  }

}

object UtegFilterRunner {
  private def recordCandidateStatsBeforeFilter(
    candidates: Seq[Seq[InitialCandidate]],
    statsReceiver: StatsReceiver
  ): Unit = {
    statsReceiver
      .counter("empty_sources", "before").incr(
        candidates.count {
          _.isEmpty
        }
      )
    candidates.foreach { candidate =>
      statsReceiver.counter("candidates", "before").incr(candidate.size)
    }
  }

  private def recordCandidateStatsAfterFilter(
    candidates: Seq[Seq[InitialCandidate]],
    statsReceiver: StatsReceiver
  ): Unit = {
    statsReceiver
      .counter("empty_sources", "after").incr(
        candidates.count {
          _.isEmpty
        }
      )
    candidates.foreach { candidate =>
      statsReceiver.counter("candidates", "after").incr(candidate.size)
    }
  }

  /*
  Helper function for running some candidates through a sequence of filters
   */
  private[filter] def runSequentialFilters[CGQueryType <: CandidateGeneratorQuery](
    request: CGQueryType,
    candidates: Seq[Seq[InitialCandidate]],
    filters: Seq[FilterBase],
    statsReceiver: StatsReceiver
  ): Future[Seq[Seq[InitialCandidate]]] =
    filters.foldLeft(Future.value(candidates)) {
      case (candsFut, filter) =>
        candsFut.flatMap { cands =>
          recordCandidateStatsBeforeFilter(cands, statsReceiver.scope(filter.name))
          filter
            .filter(cands, filter.requestToConfig(request))
            .map { filteredCands =>
              recordCandidateStatsAfterFilter(filteredCands, statsReceiver.scope(filter.name))
              filteredCands
            }
        }
    }
}
package com.twitter.cr_mixer.filter

import com.twitter.cr_mixer.model.CandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.util.Future
import javax.inject.Singleton

@Singleton
case class ImpressedTweetlistFilter() extends FilterBase {
  import ImpressedTweetlistFilter._

  override val name: String = this.getClass.getCanonicalName

  override type ConfigType = FilterConfig

  /*
   Filtering removes some candidates based on configurable criteria.
   */
  override def filter(
    candidates: Seq[Seq[InitialCandidate]],
    config: FilterConfig
  ): Future[Seq[Seq[InitialCandidate]]] = {
    // Remove candidates which match a source tweet, or which are passed in impressedTweetList
    val sourceTweetsMatch = candidates
      .flatMap {

        /***
         * Within a Seq[Seq[InitialCandidate]], all candidates within a inner Seq
         * are guaranteed to have the same sourceInfo. Hence, we can pick .headOption
         * to represent the whole list when filtering by the internalId of the sourceInfoOpt.
         * But of course the similarityEngineInfo could be different.
         */
        _.headOption.flatMap { candidate =>
          candidate.candidateGenerationInfo.sourceInfoOpt.map(_.internalId)
        }
      }.collect {
        case InternalId.TweetId(id) => id
      }

    val impressedTweetList: Set[TweetId] =
      config.impressedTweetList ++ sourceTweetsMatch

    val filteredCandidateMap: Seq[Seq[InitialCandidate]] =
      candidates.map {
        _.filterNot { candidate =>
          impressedTweetList.contains(candidate.tweetId)
        }
      }
    Future.value(filteredCandidateMap)
  }

  override def requestToConfig[CGQueryType <: CandidateGeneratorQuery](
    request: CGQueryType
  ): FilterConfig = {
    FilterConfig(request.impressedTweetList)
  }
}

object ImpressedTweetlistFilter {
  case class FilterConfig(impressedTweetList: Set[TweetId])
}
