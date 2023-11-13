package com.twitter.cr_mixer.candidate_generation

import com.twitter.cr_mixer.blender.SwitchBlender
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.filter.PostRankFilterRunner
import com.twitter.cr_mixer.filter.PreRankFilterRunner
import com.twitter.cr_mixer.logging.CrMixerScribeLogger
import com.twitter.cr_mixer.model.BlendedCandidate
import com.twitter.cr_mixer.model.CrCandidateGeneratorQuery
import com.twitter.cr_mixer.model.GraphSourceInfo
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.param.RankerParams
import com.twitter.cr_mixer.param.RecentNegativeSignalParams
import com.twitter.cr_mixer.ranker.SwitchRanker
import com.twitter.cr_mixer.source_signal.SourceInfoRouter
import com.twitter.cr_mixer.source_signal.UssStore.EnabledNegativeSourceTypes
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.util.Future
import com.twitter.util.JavaTimer
import com.twitter.util.Timer

import javax.inject.Inject
import javax.inject.Singleton

/**
 * For now it performs the main steps as follows:
 * 1. Source signal (via USS, FRS) fetch
 * 2. Candidate generation
 * 3. Filtering
 * 4. Interleave blender
 * 5. Ranker
 * 6. Post-ranker filter
 * 7. Truncation
 */
@Singleton
class CrCandidateGenerator @Inject() (
  sourceInfoRouter: SourceInfoRouter,
  candidateSourceRouter: CandidateSourcesRouter,
  switchBlender: SwitchBlender,
  preRankFilterRunner: PreRankFilterRunner,
  postRankFilterRunner: PostRankFilterRunner,
  switchRanker: SwitchRanker,
  crMixerScribeLogger: CrMixerScribeLogger,
  timeoutConfig: TimeoutConfig,
  globalStats: StatsReceiver) {
  private val timer: Timer = new JavaTimer(true)

  private val stats: StatsReceiver = globalStats.scope(this.getClass.getCanonicalName)

  private val fetchSourcesStats = stats.scope("fetchSources")
  private val fetchPositiveSourcesStats = stats.scope("fetchPositiveSources")
  private val fetchNegativeSourcesStats = stats.scope("fetchNegativeSources")
  private val fetchCandidatesStats = stats.scope("fetchCandidates")
  private val fetchCandidatesAfterFilterStats = stats.scope("fetchCandidatesAfterFilter")
  private val preRankFilterStats = stats.scope("preRankFilter")
  private val interleaveStats = stats.scope("interleave")
  private val rankStats = stats.scope("rank")
  private val postRankFilterStats = stats.scope("postRankFilter")
  private val blueVerifiedTweetStats = stats.scope("blueVerifiedTweetStats")
  private val blueVerifiedTweetStatsPerSimilarityEngine =
    stats.scope("blueVerifiedTweetStatsPerSimilarityEngine")

  def get(query: CrCandidateGeneratorQuery): Future[Seq[RankedCandidate]] = {
    val allStats = stats.scope("all")
    val perProductStats = stats.scope("perProduct", query.product.toString)
    val perProductBlueVerifiedStats =
      blueVerifiedTweetStats.scope("perProduct", query.product.toString)

    StatsUtil.trackItemsStats(allStats) {
      trackResultStats(perProductStats) {
        StatsUtil.trackItemsStats(perProductStats) {
          val result = for {
            (sourceSignals, sourceGraphsMap) <- StatsUtil.trackBlockStats(fetchSourcesStats) {
              fetchSources(query)
            }
            initialCandidates <- StatsUtil.trackBlockStats(fetchCandidatesAfterFilterStats) {
              // find the positive and negative signals
              val (positiveSignals, negativeSignals) = sourceSignals.partition { signal =>
                !EnabledNegativeSourceTypes.contains(signal.sourceType)
              }
              fetchPositiveSourcesStats.stat("size").add(positiveSignals.size)
              fetchNegativeSourcesStats.stat("size").add(negativeSignals.size)

              // find the positive signals to keep, removing block and muted users
              val filteredSourceInfo =
                if (negativeSignals.nonEmpty && query.params(
                    RecentNegativeSignalParams.EnableSourceParam)) {
                  filterSourceInfo(positiveSignals, negativeSignals)
                } else {
                  positiveSignals
                }

              // fetch candidates from the positive signals
              StatsUtil.trackBlockStats(fetchCandidatesStats) {
                fetchCandidates(query, filteredSourceInfo, sourceGraphsMap)
              }
            }
            filteredCandidates <- StatsUtil.trackBlockStats(preRankFilterStats) {
              preRankFilter(query, initialCandidates)
            }
            interleavedCandidates <- StatsUtil.trackItemsStats(interleaveStats) {
              interleave(query, filteredCandidates)
            }
            rankedCandidates <- StatsUtil.trackItemsStats(rankStats) {
              val candidatesToRank =
                interleavedCandidates.take(query.params(RankerParams.MaxCandidatesToRank))
              rank(query, candidatesToRank)
            }
            postRankFilterCandidates <- StatsUtil.trackItemsStats(postRankFilterStats) {
              postRankFilter(query, rankedCandidates)
            }
          } yield {
            trackTopKStats(
              800,
              postRankFilterCandidates,
              isQueryK = false,
              perProductBlueVerifiedStats)
            trackTopKStats(
              400,
              postRankFilterCandidates,
              isQueryK = false,
              perProductBlueVerifiedStats)
            trackTopKStats(
              query.maxNumResults,
              postRankFilterCandidates,
              isQueryK = true,
              perProductBlueVerifiedStats)

            val (blueVerifiedTweets, remainingTweets) =
              postRankFilterCandidates.partition(
                _.tweetInfo.hasBlueVerifiedAnnotation.contains(true))
            val topKBlueVerified = blueVerifiedTweets.take(query.maxNumResults)
            val topKRemaining = remainingTweets.take(query.maxNumResults - topKBlueVerified.size)

            trackBlueVerifiedTweetStats(topKBlueVerified, perProductBlueVerifiedStats)

            if (topKBlueVerified.nonEmpty && query.params(RankerParams.EnableBlueVerifiedTopK)) {
              topKBlueVerified ++ topKRemaining
            } else {
              postRankFilterCandidates
            }
          }
          result.raiseWithin(timeoutConfig.serviceTimeout)(timer)
        }
      }
    }
  }

  private def fetchSources(
    query: CrCandidateGeneratorQuery
  ): Future[(Set[SourceInfo], Map[String, Option[GraphSourceInfo]])] = {
    crMixerScribeLogger.scribeSignalSources(
      query,
      sourceInfoRouter
        .get(query.userId, query.product, query.userState, query.params))
  }

  private def filterSourceInfo(
    positiveSignals: Set[SourceInfo],
    negativeSignals: Set[SourceInfo]
  ): Set[SourceInfo] = {
    val filterUsers: Set[Long] = negativeSignals.flatMap {
      case SourceInfo(_, InternalId.UserId(userId), _) => Some(userId)
      case _ => None
    }

    positiveSignals.filter {
      case SourceInfo(_, InternalId.UserId(userId), _) => !filterUsers.contains(userId)
      case _ => true
    }
  }

  def fetchCandidates(
    query: CrCandidateGeneratorQuery,
    sourceSignals: Set[SourceInfo],
    sourceGraphs: Map[String, Option[GraphSourceInfo]]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    val initialCandidates = candidateSourceRouter
      .fetchCandidates(
        query.userId,
        sourceSignals,
        sourceGraphs,
        query.params
      )

    initialCandidates.map(_.flatten.map { candidate =>
      if (candidate.tweetInfo.hasBlueVerifiedAnnotation.contains(true)) {
        blueVerifiedTweetStatsPerSimilarityEngine
          .scope(query.product.toString).scope(
            candidate.candidateGenerationInfo.contributingSimilarityEngines.head.similarityEngineType.toString).counter(
            candidate.tweetInfo.authorId.toString).incr()
      }
    })

    crMixerScribeLogger.scribeInitialCandidates(
      query,
      initialCandidates
    )
  }

  private def preRankFilter(
    query: CrCandidateGeneratorQuery,
    candidates: Seq[Seq[InitialCandidate]]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    crMixerScribeLogger.scribePreRankFilterCandidates(
      query,
      preRankFilterRunner
        .runSequentialFilters(query, candidates))
  }

  private def postRankFilter(
    query: CrCandidateGeneratorQuery,
    candidates: Seq[RankedCandidate]
  ): Future[Seq[RankedCandidate]] = {
    postRankFilterRunner.run(query, candidates)
  }

  private def interleave(
    query: CrCandidateGeneratorQuery,
    candidates: Seq[Seq[InitialCandidate]]
  ): Future[Seq[BlendedCandidate]] = {
    crMixerScribeLogger.scribeInterleaveCandidates(
      query,
      switchBlender
        .blend(query.params, query.userState, candidates))
  }

  private def rank(
    query: CrCandidateGeneratorQuery,
    candidates: Seq[BlendedCandidate],
  ): Future[Seq[RankedCandidate]] = {
    crMixerScribeLogger.scribeRankedCandidates(
      query,
      switchRanker.rank(query, candidates)
    )
  }

  private def trackResultStats(
    stats: StatsReceiver
  )(
    fn: => Future[Seq[RankedCandidate]]
  ): Future[Seq[RankedCandidate]] = {
    fn.onSuccess { candidates =>
      trackReasonChosenSourceTypeStats(candidates, stats)
      trackReasonChosenSimilarityEngineStats(candidates, stats)
      trackPotentialReasonsSourceTypeStats(candidates, stats)
      trackPotentialReasonsSimilarityEngineStats(candidates, stats)
    }
  }

  private def trackReasonChosenSourceTypeStats(
    candidates: Seq[RankedCandidate],
    stats: StatsReceiver
  ): Unit = {
    candidates
      .groupBy(_.reasonChosen.sourceInfoOpt.map(_.sourceType))
      .foreach {
        case (sourceTypeOpt, rankedCands) =>
          val sourceType = sourceTypeOpt.map(_.toString).getOrElse("RequesterId") // default
          stats.stat("reasonChosen", "sourceType", sourceType, "size").add(rankedCands.size)
      }
  }

  private def trackReasonChosenSimilarityEngineStats(
    candidates: Seq[RankedCandidate],
    stats: StatsReceiver
  ): Unit = {
    candidates
      .groupBy(_.reasonChosen.similarityEngineInfo.similarityEngineType)
      .foreach {
        case (seInfoType, rankedCands) =>
          stats
            .stat("reasonChosen", "similarityEngine", seInfoType.toString, "size").add(
              rankedCands.size)
      }
  }

  private def trackPotentialReasonsSourceTypeStats(
    candidates: Seq[RankedCandidate],
    stats: StatsReceiver
  ): Unit = {
    candidates
      .flatMap(_.potentialReasons.map(_.sourceInfoOpt.map(_.sourceType)))
      .groupBy(source => source)
      .foreach {
        case (sourceInfoOpt, seq) =>
          val sourceType = sourceInfoOpt.map(_.toString).getOrElse("RequesterId") // default
          stats.stat("potentialReasons", "sourceType", sourceType, "size").add(seq.size)
      }
  }

  private def trackPotentialReasonsSimilarityEngineStats(
    candidates: Seq[RankedCandidate],
    stats: StatsReceiver
  ): Unit = {
    candidates
      .flatMap(_.potentialReasons.map(_.similarityEngineInfo.similarityEngineType))
      .groupBy(se => se)
      .foreach {
        case (seType, seq) =>
          stats.stat("potentialReasons", "similarityEngine", seType.toString, "size").add(seq.size)
      }
  }

  private def trackBlueVerifiedTweetStats(
    candidates: Seq[RankedCandidate],
    statsReceiver: StatsReceiver
  ): Unit = {
    candidates.foreach { candidate =>
      if (candidate.tweetInfo.hasBlueVerifiedAnnotation.contains(true)) {
        statsReceiver.counter(candidate.tweetInfo.authorId.toString).incr()
        statsReceiver
          .scope(candidate.tweetInfo.authorId.toString).counter(candidate.tweetId.toString).incr()
      }
    }
  }

  private def trackTopKStats(
    k: Int,
    tweetCandidates: Seq[RankedCandidate],
    isQueryK: Boolean,
    statsReceiver: StatsReceiver
  ): Unit = {
    val (topK, beyondK) = tweetCandidates.splitAt(k)

    val blueVerifiedIds = tweetCandidates.collect {
      case candidate if candidate.tweetInfo.hasBlueVerifiedAnnotation.contains(true) =>
        candidate.tweetInfo.authorId
    }.toSet

    blueVerifiedIds.foreach { blueVerifiedId =>
      val numTweetsTopK = topK.count(_.tweetInfo.authorId == blueVerifiedId)
      val numTweetsBeyondK = beyondK.count(_.tweetInfo.authorId == blueVerifiedId)

      if (isQueryK) {
        statsReceiver.scope(blueVerifiedId.toString).stat(s"topK").add(numTweetsTopK)
        statsReceiver
          .scope(blueVerifiedId.toString).stat(s"beyondK").add(numTweetsBeyondK)
      } else {
        statsReceiver.scope(blueVerifiedId.toString).stat(s"top$k").add(numTweetsTopK)
        statsReceiver
          .scope(blueVerifiedId.toString).stat(s"beyond$k").add(numTweetsBeyondK)
      }
    }
  }
}
 package com.twitter.cr_mixer.candidate_generation

import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.InitialAdsCandidate
import com.twitter.cr_mixer.model.ModelConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.ConsumersBasedUserAdGraphParams
import com.twitter.cr_mixer.param.ConsumerBasedWalsParams
import com.twitter.cr_mixer.param.ConsumerEmbeddingBasedCandidateGenerationParams
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.param.InterestedInParams
import com.twitter.cr_mixer.param.ProducerBasedCandidateGenerationParams
import com.twitter.cr_mixer.param.SimClustersANNParams
import com.twitter.cr_mixer.param.TweetBasedCandidateGenerationParams
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.similarity_engine.ConsumerBasedWalsSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.ConsumersBasedUserAdGraphSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.FilterUtil
import com.twitter.cr_mixer.similarity_engine.HnswANNEngineQuery
import com.twitter.cr_mixer.similarity_engine.HnswANNSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.ProducerBasedUserAdGraphSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimClustersANNSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.SimClustersANNSimilarityEngine.Query
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.TweetBasedUserAdGraphSimilarityEngine
import com.twitter.cr_mixer.thriftscala.LineItemInfo
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.common.ModelVersions
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.EmbeddingType
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.timelines.configapi.Params
import com.twitter.util.Future

import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

@Singleton
case class AdsCandidateSourcesRouter @Inject() (
  activePromotedTweetStore: ReadableStore[TweetId, Seq[LineItemInfo]],
  decider: CrMixerDecider,
  @Named(ModuleNames.SimClustersANNSimilarityEngine) simClustersANNSimilarityEngine: StandardSimilarityEngine[
    Query,
    TweetWithScore
  ],
  @Named(ModuleNames.TweetBasedUserAdGraphSimilarityEngine)
  tweetBasedUserAdGraphSimilarityEngine: StandardSimilarityEngine[
    TweetBasedUserAdGraphSimilarityEngine.Query,
    TweetWithScore
  ],
  @Named(ModuleNames.ConsumersBasedUserAdGraphSimilarityEngine)
  consumersBasedUserAdGraphSimilarityEngine: StandardSimilarityEngine[
    ConsumersBasedUserAdGraphSimilarityEngine.Query,
    TweetWithScore
  ],
  @Named(ModuleNames.ProducerBasedUserAdGraphSimilarityEngine)
  producerBasedUserAdGraphSimilarityEngine: StandardSimilarityEngine[
    ProducerBasedUserAdGraphSimilarityEngine.Query,
    TweetWithScore
  ],
  @Named(ModuleNames.TweetBasedTwHINANNSimilarityEngine)
  tweetBasedTwHINANNSimilarityEngine: HnswANNSimilarityEngine,
  @Named(ModuleNames.ConsumerEmbeddingBasedTwHINANNSimilarityEngine) consumerTwHINANNSimilarityEngine: HnswANNSimilarityEngine,
  @Named(ModuleNames.ConsumerBasedWalsSimilarityEngine)
  consumerBasedWalsSimilarityEngine: StandardSimilarityEngine[
    ConsumerBasedWalsSimilarityEngine.Query,
    TweetWithScore
  ],
  globalStats: StatsReceiver,
) {

  import AdsCandidateSourcesRouter._

  val stats: StatsReceiver = globalStats.scope(this.getClass.getSimpleName)

  def fetchCandidates(
    requestUserId: UserId,
    sourceSignals: Set[SourceInfo],
    realGraphSeeds: Map[UserId, Double],
    params: configapi.Params
  ): Future[Seq[Seq[InitialAdsCandidate]]] = {

    val simClustersANN1ConfigId = params(SimClustersANNParams.SimClustersANN1ConfigId)

    val tweetBasedSANNMinScore = params(
      TweetBasedCandidateGenerationParams.SimClustersMinScoreParam)
    val tweetBasedSANN1Candidates =
      if (params(TweetBasedCandidateGenerationParams.EnableSimClustersANN1Param)) {
        Future.collect(
          CandidateSourcesRouter.getTweetBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getSimClustersANNCandidates(
              requestUserId,
              Some(sourceInfo),
              params,
              simClustersANN1ConfigId,
              tweetBasedSANNMinScore)
          })
      } else Future.value(Seq.empty)

    val simClustersANN2ConfigId = params(SimClustersANNParams.SimClustersANN2ConfigId)
    val tweetBasedSANN2Candidates =
      if (params(TweetBasedCandidateGenerationParams.EnableSimClustersANN2Param)) {
        Future.collect(
          CandidateSourcesRouter.getTweetBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getSimClustersANNCandidates(
              requestUserId,
              Some(sourceInfo),
              params,
              simClustersANN2ConfigId,
              tweetBasedSANNMinScore)
          })
      } else Future.value(Seq.empty)

    val tweetBasedUagCandidates =
      if (params(TweetBasedCandidateGenerationParams.EnableUAGParam)) {
        Future.collect(
          CandidateSourcesRouter.getTweetBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getTweetBasedUserAdGraphCandidates(Some(sourceInfo), params)
          })
      } else Future.value(Seq.empty)

    val realGraphInNetworkBasedUagCandidates =
      if (params(ConsumersBasedUserAdGraphParams.EnableSourceParam)) {
        getRealGraphConsumersBasedUserAdGraphCandidates(realGraphSeeds, params).map(Seq(_))
      } else Future.value(Seq.empty)

    val producerBasedUagCandidates =
      if (params(ProducerBasedCandidateGenerationParams.EnableUAGParam)) {
        Future.collect(
          CandidateSourcesRouter.getProducerBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getProducerBasedUserAdGraphCandidates(Some(sourceInfo), params)
          })
      } else Future.value(Seq.empty)

    val tweetBasedTwhinAdsCandidates =
      if (params(TweetBasedCandidateGenerationParams.EnableTwHINParam)) {
        Future.collect(
          CandidateSourcesRouter.getTweetBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getTwHINAdsCandidates(
              tweetBasedTwHINANNSimilarityEngine,
              SimilarityEngineType.TweetBasedTwHINANN,
              requestUserId,
              Some(sourceInfo),
              ModelConfig.DebuggerDemo)
          })
      } else Future.value(Seq.empty)

    val producerBasedSANNMinScore = params(
      ProducerBasedCandidateGenerationParams.SimClustersMinScoreParam)
    val producerBasedSANN1Candidates =
      if (params(ProducerBasedCandidateGenerationParams.EnableSimClustersANN1Param)) {
        Future.collect(
          CandidateSourcesRouter.getProducerBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getSimClustersANNCandidates(
              requestUserId,
              Some(sourceInfo),
              params,
              simClustersANN1ConfigId,
              producerBasedSANNMinScore)
          })
      } else Future.value(Seq.empty)
    val producerBasedSANN2Candidates =
      if (params(ProducerBasedCandidateGenerationParams.EnableSimClustersANN2Param)) {
        Future.collect(
          CandidateSourcesRouter.getProducerBasedSourceInfo(sourceSignals).toSeq.map { sourceInfo =>
            getSimClustersANNCandidates(
              requestUserId,
              Some(sourceInfo),
              params,
              simClustersANN2ConfigId,
              producerBasedSANNMinScore)
          })
      } else Future.value(Seq.empty)

    val interestedInMinScore = params(InterestedInParams.MinScoreParam)
    val interestedInSANN1Candidates = if (params(InterestedInParams.EnableSimClustersANN1Param)) {
      getSimClustersANNCandidates(
        requestUserId,
        None,
        params,
        simClustersANN1ConfigId,
        interestedInMinScore).map(Seq(_))
    } else Future.value(Seq.empty)

    val interestedInSANN2Candidates = if (params(InterestedInParams.EnableSimClustersANN2Param)) {
      getSimClustersANNCandidates(
        requestUserId,
        None,
        params,
        simClustersANN2ConfigId,
        interestedInMinScore).map(Seq(_))
    } else Future.value(Seq.empty)

    val consumerTwHINAdsCandidates =
      if (params(ConsumerEmbeddingBasedCandidateGenerationParams.EnableTwHINParam)) {
        getTwHINAdsCandidates(
          consumerTwHINANNSimilarityEngine,
          SimilarityEngineType.ConsumerEmbeddingBasedTwHINANN,
          requestUserId,
          None,
          ModelConfig.DebuggerDemo).map(Seq(_))
      } else Future.value(Seq.empty)

    val consumerBasedWalsCandidates =
      if (params(
          ConsumerBasedWalsParams.EnableSourceParam
        )) {
        getConsumerBasedWalsCandidates(sourceSignals, params)
      }.map {
        Seq(_)
      }
      else Future.value(Seq.empty)

    Future
      .collect(Seq(
        tweetBasedSANN1Candidates,
        tweetBasedSANN2Candidates,
        tweetBasedUagCandidates,
        tweetBasedTwhinAdsCandidates,
        producerBasedUagCandidates,
        producerBasedSANN1Candidates,
        producerBasedSANN2Candidates,
        realGraphInNetworkBasedUagCandidates,
        interestedInSANN1Candidates,
        interestedInSANN2Candidates,
        consumerTwHINAdsCandidates,
        consumerBasedWalsCandidates,
      )).map(_.flatten).map { tweetsWithCGInfoSeq =>
        Future.collect(
          tweetsWithCGInfoSeq.map(candidates => convertToInitialCandidates(candidates, stats)))
      }.flatten.map { candidatesLists =>
        val result = candidatesLists.filter(_.nonEmpty)
        stats.stat("numOfSequences").add(result.size)
        stats.stat("flattenCandidatesWithDup").add(result.flatten.size)
        result
      }
  }

  private[candidate_generation] def convertToInitialCandidates(
    candidates: Seq[TweetWithCandidateGenerationInfo],
    stats: StatsReceiver
  ): Future[Seq[InitialAdsCandidate]] = {
    val tweetIds = candidates.map(_.tweetId).toSet
    stats.stat("initialCandidateSizeBeforeLineItemFilter").add(tweetIds.size)
    Future.collect(activePromotedTweetStore.multiGet(tweetIds)).map { lineItemInfos =>
      /** *
       * If lineItemInfo does not exist, we will filter out the promoted tweet as it cannot be targeted and ranked in admixer
       */
      val filteredCandidates = candidates.collect {
        case candidate if lineItemInfos.getOrElse(candidate.tweetId, None).isDefined =>
          val lineItemInfo = lineItemInfos(candidate.tweetId)
            .getOrElse(throw new IllegalStateException("Check previous line's condition"))

          InitialAdsCandidate(
            tweetId = candidate.tweetId,
            lineItemInfo = lineItemInfo,
            candidate.candidateGenerationInfo
          )
      }
      stats.stat("initialCandidateSizeAfterLineItemFilter").add(filteredCandidates.size)
      filteredCandidates
    }
  }

  private[candidate_generation] def getSimClustersANNCandidates(
    requestUserId: UserId,
    sourceInfo: Option[SourceInfo],
    params: configapi.Params,
    configId: String,
    minScore: Double
  ) = {

    val simClustersModelVersion =
      ModelVersions.Enum.enumToSimClustersModelVersionMap(params(GlobalParams.ModelVersionParam))

    val embeddingType =
      if (sourceInfo.isEmpty) {
        params(InterestedInParams.InterestedInEmbeddingIdParam).embeddingType
      } else getSimClustersANNEmbeddingType(sourceInfo.get)
    val query = SimClustersANNSimilarityEngine.fromParams(
      if (sourceInfo.isEmpty) InternalId.UserId(requestUserId) else sourceInfo.get.internalId,
      embeddingType,
      simClustersModelVersion,
      configId,
      params
    )

    // dark traffic to simclusters-ann-2
    if (decider.isAvailable(DeciderConstants.enableSimClustersANN2DarkTrafficDeciderKey)) {
      val simClustersANN2ConfigId = params(SimClustersANNParams.SimClustersANN2ConfigId)
      val sann2Query = SimClustersANNSimilarityEngine.fromParams(
        if (sourceInfo.isEmpty) InternalId.UserId(requestUserId) else sourceInfo.get.internalId,
        embeddingType,
        simClustersModelVersion,
        simClustersANN2ConfigId,
        params
      )
      simClustersANNSimilarityEngine
        .getCandidates(sann2Query)
    }

    simClustersANNSimilarityEngine
      .getCandidates(query).map(_.getOrElse(Seq.empty)).map(_.filter(_.score > minScore).map {
        tweetWithScore =>
          val similarityEngineInfo = SimClustersANNSimilarityEngine
            .toSimilarityEngineInfo(query, tweetWithScore.score)
          TweetWithCandidateGenerationInfo(
            tweetWithScore.tweetId,
            CandidateGenerationInfo(
              sourceInfo,
              similarityEngineInfo,
              Seq(similarityEngineInfo)
            ))
      })
  }

  private[candidate_generation] def getProducerBasedUserAdGraphCandidates(
    sourceInfo: Option[SourceInfo],
    params: configapi.Params
  ) = {

    val query = ProducerBasedUserAdGraphSimilarityEngine.fromParams(
      sourceInfo.get.internalId,
      params
    )
    producerBasedUserAdGraphSimilarityEngine
      .getCandidates(query).map(_.getOrElse(Seq.empty)).map(_.map { tweetWithScore =>
        val similarityEngineInfo = ProducerBasedUserAdGraphSimilarityEngine
          .toSimilarityEngineInfo(tweetWithScore.score)
        TweetWithCandidateGenerationInfo(
          tweetWithScore.tweetId,
          CandidateGenerationInfo(
            sourceInfo,
            similarityEngineInfo,
            Seq(similarityEngineInfo)
          ))
      })
  }

  private[candidate_generation] def getTweetBasedUserAdGraphCandidates(
    sourceInfo: Option[SourceInfo],
    params: configapi.Params
  ) = {

    val query = TweetBasedUserAdGraphSimilarityEngine.fromParams(
      sourceInfo.get.internalId,
      params
    )
    tweetBasedUserAdGraphSimilarityEngine
      .getCandidates(query).map(_.getOrElse(Seq.empty)).map(_.map { tweetWithScore =>
        val similarityEngineInfo = TweetBasedUserAdGraphSimilarityEngine
          .toSimilarityEngineInfo(tweetWithScore.score)
        TweetWithCandidateGenerationInfo(
          tweetWithScore.tweetId,
          CandidateGenerationInfo(
            sourceInfo,
            similarityEngineInfo,
            Seq(similarityEngineInfo)
          ))
      })
  }

  private[candidate_generation] def getRealGraphConsumersBasedUserAdGraphCandidates(
    realGraphSeeds: Map[UserId, Double],
    params: configapi.Params
  ) = {

    val query = ConsumersBasedUserAdGraphSimilarityEngine
      .fromParams(realGraphSeeds, params)

    // The internalId is a placeholder value. We do not plan to store the full seedUserId set.
    val sourceInfo = SourceInfo(
      sourceType = SourceType.RealGraphIn,
      internalId = InternalId.UserId(0L),
      sourceEventTime = None
    )
    consumersBasedUserAdGraphSimilarityEngine
      .getCandidates(query).map(_.getOrElse(Seq.empty)).map(_.map { tweetWithScore =>
        val similarityEngineInfo = ConsumersBasedUserAdGraphSimilarityEngine
          .toSimilarityEngineInfo(tweetWithScore.score)
        TweetWithCandidateGenerationInfo(
          tweetWithScore.tweetId,
          CandidateGenerationInfo(
            Some(sourceInfo),
            similarityEngineInfo,
            Seq.empty // Atomic Similarity Engine. Hence it has no contributing SEs
          )
        )
      })
  }

  private[candidate_generation] def getTwHINAdsCandidates(
    similarityEngine: HnswANNSimilarityEngine,
    similarityEngineType: SimilarityEngineType,
    requestUserId: UserId,
    sourceInfo: Option[SourceInfo], // if none, then it's consumer-based similarity engine
    model: String
  ): Future[Seq[TweetWithCandidateGenerationInfo]] = {
    val internalId =
      if (sourceInfo.nonEmpty) sourceInfo.get.internalId else InternalId.UserId(requestUserId)
    similarityEngine
      .getCandidates(buildHnswANNQuery(internalId, model)).map(_.getOrElse(Seq.empty)).map(_.map {
        tweetWithScore =>
          val similarityEngineInfo = SimilarityEngineInfo(
            similarityEngineType = similarityEngineType,
            modelId = Some(model),
            score = Some(tweetWithScore.score))
          TweetWithCandidateGenerationInfo(
            tweetWithScore.tweetId,
            CandidateGenerationInfo(
              None,
              similarityEngineInfo,
              Seq(similarityEngineInfo)
            ))
      })
  }

  private[candidate_generation] def getConsumerBasedWalsCandidates(
    sourceSignals: Set[SourceInfo],
    params: configapi.Params
  ): Future[Seq[TweetWithCandidateGenerationInfo]] = {
    // Fetch source signals and filter them based on age.
    val signals = FilterUtil.tweetSourceAgeFilter(
      getConsumerBasedWalsSourceInfo(sourceSignals).toSeq,
      params(ConsumerBasedWalsParams.MaxTweetSignalAgeHoursParam))

    val candidatesOptFut = consumerBasedWalsSimilarityEngine.getCandidates(
      ConsumerBasedWalsSimilarityEngine.fromParams(signals, params)
    )
    val tweetsWithCandidateGenerationInfoOptFut = candidatesOptFut.map {
      _.map { tweetsWithScores =>
        val sortedCandidates = tweetsWithScores.sortBy(-_.score)
        val filteredCandidates =
          FilterUtil.tweetAgeFilter(sortedCandidates, params(GlobalParams.MaxTweetAgeHoursParam))
        consumerBasedWalsSimilarityEngine.getScopedStats
          .stat("filteredCandidates_size").add(filteredCandidates.size)

        val tweetsWithCandidateGenerationInfo = filteredCandidates.map { tweetWithScore =>
          {
            val similarityEngineInfo =
              ConsumerBasedWalsSimilarityEngine.toSimilarityEngineInfo(tweetWithScore.score)
            TweetWithCandidateGenerationInfo(
              tweetWithScore.tweetId,
              CandidateGenerationInfo(
                None,
                similarityEngineInfo,
                Seq.empty // Atomic Similarity Engine. Hence it has no contributing SEs
              )
            )
          }
        }
        val maxCandidateNum = params(GlobalParams.MaxCandidateNumPerSourceKeyParam)
        tweetsWithCandidateGenerationInfo.take(maxCandidateNum)
      }
    }
    for {
      tweetsWithCandidateGenerationInfoOpt <- tweetsWithCandidateGenerationInfoOptFut
    } yield tweetsWithCandidateGenerationInfoOpt.toSeq.flatten
  }
}

object AdsCandidateSourcesRouter {
  def getSimClustersANNEmbeddingType(
    sourceInfo: SourceInfo
  ): EmbeddingType = {
    sourceInfo.sourceType match {
      case SourceType.TweetFavorite | SourceType.Retweet | SourceType.OriginalTweet |
          SourceType.Reply | SourceType.TweetShare | SourceType.NotificationClick |
          SourceType.GoodTweetClick | SourceType.VideoTweetQualityView |
          SourceType.VideoTweetPlayback50 =>
        EmbeddingType.LogFavLongestL2EmbeddingTweet
      case SourceType.UserFollow | SourceType.UserRepeatedProfileVisit | SourceType.RealGraphOon |
          SourceType.FollowRecommendation | SourceType.UserTrafficAttributionProfileVisit |
          SourceType.GoodProfileClick | SourceType.TwiceUserId =>
        EmbeddingType.FavBasedProducer
      case _ => throw new IllegalArgumentException("sourceInfo.sourceType not supported")
    }
  }

  def buildHnswANNQuery(internalId: InternalId, modelId: String): HnswANNEngineQuery = {
    HnswANNEngineQuery(
      sourceId = internalId,
      modelId = modelId,
      params = Params.Empty
    )
  }

  def getConsumerBasedWalsSourceInfo(
    sourceSignals: Set[SourceInfo]
  ): Set[SourceInfo] = {
    val AllowedSourceTypesForConsumerBasedWalsSE = Set(
      SourceType.TweetFavorite.value,
      SourceType.Retweet.value,
      SourceType.TweetDontLike.value, //currently no-op
      SourceType.TweetReport.value, //currently no-op
      SourceType.AccountMute.value, //currently no-op
      SourceType.AccountBlock.value //currently no-op
    )
    sourceSignals.collect {
      case sourceInfo
          if AllowedSourceTypesForConsumerBasedWalsSE.contains(sourceInfo.sourceType.value) =>
        sourceInfo
    }
  }
}
