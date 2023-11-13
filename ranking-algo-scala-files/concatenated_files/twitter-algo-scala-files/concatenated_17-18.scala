package com.twitter.cr_mixer.candidate_generation

import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.param.InterestedInParams
import com.twitter.cr_mixer.param.SimClustersANNParams
import com.twitter.cr_mixer.similarity_engine.EngineQuery
import com.twitter.cr_mixer.similarity_engine.SimClustersANNSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.base.CandidateSource
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.common.ModelVersions
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Singleton
import javax.inject.Named
import com.twitter.cr_mixer.model.ModuleNames

/**
 * This store looks for similar tweets for a given UserId that generates UserInterestedIn
 * from SimClustersANN. It will be a standalone CandidateGeneration class moving forward.
 *
 * After the abstraction improvement (apply SimilarityEngine trait)
 * these CG will be subjected to change.
 */
@Singleton
case class SimClustersInterestedInCandidateGeneration @Inject() (
  @Named(ModuleNames.SimClustersANNSimilarityEngine)
  simClustersANNSimilarityEngine: StandardSimilarityEngine[
    SimClustersANNSimilarityEngine.Query,
    TweetWithScore
  ],
  statsReceiver: StatsReceiver)
    extends CandidateSource[
      SimClustersInterestedInCandidateGeneration.Query,
      Seq[TweetWithCandidateGenerationInfo]
    ] {

  override def name: String = this.getClass.getSimpleName
  private val stats = statsReceiver.scope(name)
  private val fetchCandidatesStat = stats.scope("fetchCandidates")

  override def get(
    query: SimClustersInterestedInCandidateGeneration.Query
  ): Future[Option[Seq[Seq[TweetWithCandidateGenerationInfo]]]] = {

    query.internalId match {
      case _: InternalId.UserId =>
        StatsUtil.trackOptionItemsStats(fetchCandidatesStat) {
          // UserInterestedIn Queries
          val userInterestedInCandidateResultFut =
            if (query.enableUserInterestedIn && query.enableProdSimClustersANNSimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.interestedInSimClustersANNQuery,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userInterestedInExperimentalSANNCandidateResultFut =
            if (query.enableUserInterestedIn && query.enableExperimentalSimClustersANNSimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.interestedInExperimentalSimClustersANNQuery,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userInterestedInSANN1CandidateResultFut =
            if (query.enableUserInterestedIn && query.enableSimClustersANN1SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.interestedInSimClustersANN1Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userInterestedInSANN2CandidateResultFut =
            if (query.enableUserInterestedIn && query.enableSimClustersANN2SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.interestedInSimClustersANN2Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userInterestedInSANN3CandidateResultFut =
            if (query.enableUserInterestedIn && query.enableSimClustersANN3SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.interestedInSimClustersANN3Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userInterestedInSANN5CandidateResultFut =
            if (query.enableUserInterestedIn && query.enableSimClustersANN5SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.interestedInSimClustersANN5Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userInterestedInSANN4CandidateResultFut =
            if (query.enableUserInterestedIn && query.enableSimClustersANN4SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.interestedInSimClustersANN4Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None
          // UserNextInterestedIn Queries
          val userNextInterestedInCandidateResultFut =
            if (query.enableUserNextInterestedIn && query.enableProdSimClustersANNSimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.nextInterestedInSimClustersANNQuery,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userNextInterestedInExperimentalSANNCandidateResultFut =
            if (query.enableUserNextInterestedIn && query.enableExperimentalSimClustersANNSimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.nextInterestedInExperimentalSimClustersANNQuery,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userNextInterestedInSANN1CandidateResultFut =
            if (query.enableUserNextInterestedIn && query.enableSimClustersANN1SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.nextInterestedInSimClustersANN1Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userNextInterestedInSANN2CandidateResultFut =
            if (query.enableUserNextInterestedIn && query.enableSimClustersANN2SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.nextInterestedInSimClustersANN2Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userNextInterestedInSANN3CandidateResultFut =
            if (query.enableUserNextInterestedIn && query.enableSimClustersANN3SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.nextInterestedInSimClustersANN3Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userNextInterestedInSANN5CandidateResultFut =
            if (query.enableUserNextInterestedIn && query.enableSimClustersANN5SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.nextInterestedInSimClustersANN5Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userNextInterestedInSANN4CandidateResultFut =
            if (query.enableUserNextInterestedIn && query.enableSimClustersANN4SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.nextInterestedInSimClustersANN4Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          // AddressBookInterestedIn Queries
          val userAddressBookInterestedInCandidateResultFut =
            if (query.enableAddressBookNextInterestedIn && query.enableProdSimClustersANNSimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.addressbookInterestedInSimClustersANNQuery,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userAddressBookExperimentalSANNCandidateResultFut =
            if (query.enableAddressBookNextInterestedIn && query.enableExperimentalSimClustersANNSimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.addressbookInterestedInExperimentalSimClustersANNQuery,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userAddressBookSANN1CandidateResultFut =
            if (query.enableAddressBookNextInterestedIn && query.enableSimClustersANN1SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.addressbookInterestedInSimClustersANN1Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userAddressBookSANN2CandidateResultFut =
            if (query.enableAddressBookNextInterestedIn && query.enableSimClustersANN2SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.addressbookInterestedInSimClustersANN2Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userAddressBookSANN3CandidateResultFut =
            if (query.enableAddressBookNextInterestedIn && query.enableSimClustersANN3SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.addressbookInterestedInSimClustersANN3Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userAddressBookSANN5CandidateResultFut =
            if (query.enableAddressBookNextInterestedIn && query.enableSimClustersANN5SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.addressbookInterestedInSimClustersANN5Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          val userAddressBookSANN4CandidateResultFut =
            if (query.enableAddressBookNextInterestedIn && query.enableSimClustersANN4SimilarityEngine)
              getInterestedInCandidateResult(
                simClustersANNSimilarityEngine,
                query.addressbookInterestedInSimClustersANN4Query,
                query.simClustersInterestedInMinScore)
            else
              Future.None

          Future
            .collect(
              Seq(
                userInterestedInCandidateResultFut,
                userNextInterestedInCandidateResultFut,
                userAddressBookInterestedInCandidateResultFut,
                userInterestedInExperimentalSANNCandidateResultFut,
                userNextInterestedInExperimentalSANNCandidateResultFut,
                userAddressBookExperimentalSANNCandidateResultFut,
                userInterestedInSANN1CandidateResultFut,
                userNextInterestedInSANN1CandidateResultFut,
                userAddressBookSANN1CandidateResultFut,
                userInterestedInSANN2CandidateResultFut,
                userNextInterestedInSANN2CandidateResultFut,
                userAddressBookSANN2CandidateResultFut,
                userInterestedInSANN3CandidateResultFut,
                userNextInterestedInSANN3CandidateResultFut,
                userAddressBookSANN3CandidateResultFut,
                userInterestedInSANN5CandidateResultFut,
                userNextInterestedInSANN5CandidateResultFut,
                userAddressBookSANN5CandidateResultFut,
                userInterestedInSANN4CandidateResultFut,
                userNextInterestedInSANN4CandidateResultFut,
                userAddressBookSANN4CandidateResultFut
              )
            ).map { candidateResults =>
              Some(
                candidateResults.map(candidateResult => candidateResult.getOrElse(Seq.empty))
              )
            }
        }
      case _ =>
        stats.counter("sourceId_is_not_userId_cnt").incr()
        Future.None
    }
  }

  private def simClustersCandidateMinScoreFilter(
    simClustersAnnCandidates: Seq[TweetWithScore],
    simClustersInterestedInMinScore: Double,
    simClustersANNConfigId: String
  ): Seq[TweetWithScore] = {
    val filteredCandidates = simClustersAnnCandidates
      .filter { candidate =>
        candidate.score > simClustersInterestedInMinScore
      }

    stats.stat(simClustersANNConfigId, "simClustersAnnCandidates_size").add(filteredCandidates.size)
    stats.counter(simClustersANNConfigId, "simClustersAnnRequests").incr()
    if (filteredCandidates.isEmpty)
      stats.counter(simClustersANNConfigId, "emptyFilteredSimClustersAnnCandidates").incr()

    filteredCandidates.map { candidate =>
      TweetWithScore(candidate.tweetId, candidate.score)
    }
  }

  private def getInterestedInCandidateResult(
    simClustersANNSimilarityEngine: StandardSimilarityEngine[
      SimClustersANNSimilarityEngine.Query,
      TweetWithScore
    ],
    simClustersANNQuery: EngineQuery[SimClustersANNSimilarityEngine.Query],
    simClustersInterestedInMinScore: Double,
  ): Future[Option[Seq[TweetWithCandidateGenerationInfo]]] = {
    val interestedInCandidatesFut =
      simClustersANNSimilarityEngine.getCandidates(simClustersANNQuery)

    val interestedInCandidateResultFut = interestedInCandidatesFut.map { interestedInCandidates =>
      stats.stat("candidateSize").add(interestedInCandidates.size)

      val embeddingCandidatesStat = stats.scope(
        simClustersANNQuery.storeQuery.simClustersANNQuery.sourceEmbeddingId.embeddingType.name)

      embeddingCandidatesStat.stat("candidateSize").add(interestedInCandidates.size)
      if (interestedInCandidates.isEmpty) {
        embeddingCandidatesStat.counter("empty_results").incr()
      }
      embeddingCandidatesStat.counter("requests").incr()

      val filteredTweets = simClustersCandidateMinScoreFilter(
        interestedInCandidates.toSeq.flatten,
        simClustersInterestedInMinScore,
        simClustersANNQuery.storeQuery.simClustersANNConfigId)

      val interestedInTweetsWithCGInfo = filteredTweets.map { tweetWithScore =>
        TweetWithCandidateGenerationInfo(
          tweetWithScore.tweetId,
          CandidateGenerationInfo(
            None,
            SimClustersANNSimilarityEngine
              .toSimilarityEngineInfo(simClustersANNQuery, tweetWithScore.score),
            Seq.empty // SANN is an atomic SE, and hence it has no contributing SEs
          )
        )
      }

      val interestedInResults = if (interestedInTweetsWithCGInfo.nonEmpty) {
        Some(interestedInTweetsWithCGInfo)
      } else None
      interestedInResults
    }
    interestedInCandidateResultFut
  }
}

object SimClustersInterestedInCandidateGeneration {

  case class Query(
    internalId: InternalId,
    enableUserInterestedIn: Boolean,
    enableUserNextInterestedIn: Boolean,
    enableAddressBookNextInterestedIn: Boolean,
    enableProdSimClustersANNSimilarityEngine: Boolean,
    enableExperimentalSimClustersANNSimilarityEngine: Boolean,
    enableSimClustersANN1SimilarityEngine: Boolean,
    enableSimClustersANN2SimilarityEngine: Boolean,
    enableSimClustersANN3SimilarityEngine: Boolean,
    enableSimClustersANN5SimilarityEngine: Boolean,
    enableSimClustersANN4SimilarityEngine: Boolean,
    simClustersInterestedInMinScore: Double,
    simClustersNextInterestedInMinScore: Double,
    simClustersAddressBookInterestedInMinScore: Double,
    interestedInSimClustersANNQuery: EngineQuery[SimClustersANNSimilarityEngine.Query],
    nextInterestedInSimClustersANNQuery: EngineQuery[SimClustersANNSimilarityEngine.Query],
    addressbookInterestedInSimClustersANNQuery: EngineQuery[SimClustersANNSimilarityEngine.Query],
    interestedInExperimentalSimClustersANNQuery: EngineQuery[SimClustersANNSimilarityEngine.Query],
    nextInterestedInExperimentalSimClustersANNQuery: EngineQuery[
      SimClustersANNSimilarityEngine.Query
    ],
    addressbookInterestedInExperimentalSimClustersANNQuery: EngineQuery[
      SimClustersANNSimilarityEngine.Query
    ],
    interestedInSimClustersANN1Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    nextInterestedInSimClustersANN1Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    addressbookInterestedInSimClustersANN1Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    interestedInSimClustersANN2Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    nextInterestedInSimClustersANN2Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    addressbookInterestedInSimClustersANN2Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    interestedInSimClustersANN3Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    nextInterestedInSimClustersANN3Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    addressbookInterestedInSimClustersANN3Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    interestedInSimClustersANN5Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    nextInterestedInSimClustersANN5Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    addressbookInterestedInSimClustersANN5Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    interestedInSimClustersANN4Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    nextInterestedInSimClustersANN4Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
    addressbookInterestedInSimClustersANN4Query: EngineQuery[SimClustersANNSimilarityEngine.Query],
  )

  def fromParams(
    internalId: InternalId,
    params: configapi.Params,
  ): Query = {
    // SimClusters common configs
    val simClustersModelVersion =
      ModelVersions.Enum.enumToSimClustersModelVersionMap(params(GlobalParams.ModelVersionParam))
    val simClustersANNConfigId = params(SimClustersANNParams.SimClustersANNConfigId)
    val experimentalSimClustersANNConfigId = params(
      SimClustersANNParams.ExperimentalSimClustersANNConfigId)
    val simClustersANN1ConfigId = params(SimClustersANNParams.SimClustersANN1ConfigId)
    val simClustersANN2ConfigId = params(SimClustersANNParams.SimClustersANN2ConfigId)
    val simClustersANN3ConfigId = params(SimClustersANNParams.SimClustersANN3ConfigId)
    val simClustersANN5ConfigId = params(SimClustersANNParams.SimClustersANN5ConfigId)
    val simClustersANN4ConfigId = params(SimClustersANNParams.SimClustersANN4ConfigId)

    val simClustersInterestedInMinScore = params(InterestedInParams.MinScoreParam)
    val simClustersNextInterestedInMinScore = params(
      InterestedInParams.MinScoreSequentialModelParam)
    val simClustersAddressBookInterestedInMinScore = params(
      InterestedInParams.MinScoreAddressBookParam)

    // InterestedIn embeddings parameters
    val interestedInEmbedding = params(InterestedInParams.InterestedInEmbeddingIdParam)
    val nextInterestedInEmbedding = params(InterestedInParams.NextInterestedInEmbeddingIdParam)
    val addressbookInterestedInEmbedding = params(
      InterestedInParams.AddressBookInterestedInEmbeddingIdParam)

    // Prod SimClustersANN Query
    val interestedInSimClustersANNQuery =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        interestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANNConfigId,
        params)

    val nextInterestedInSimClustersANNQuery =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        nextInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANNConfigId,
        params)

    val addressbookInterestedInSimClustersANNQuery =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        addressbookInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANNConfigId,
        params)

    // Experimental SANN cluster Query
    val interestedInExperimentalSimClustersANNQuery =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        interestedInEmbedding.embeddingType,
        simClustersModelVersion,
        experimentalSimClustersANNConfigId,
        params)

    val nextInterestedInExperimentalSimClustersANNQuery =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        nextInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        experimentalSimClustersANNConfigId,
        params)

    val addressbookInterestedInExperimentalSimClustersANNQuery =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        addressbookInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        experimentalSimClustersANNConfigId,
        params)

    // SimClusters ANN cluster 1 Query
    val interestedInSimClustersANN1Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        interestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN1ConfigId,
        params)

    val nextInterestedInSimClustersANN1Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        nextInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN1ConfigId,
        params)

    val addressbookInterestedInSimClustersANN1Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        addressbookInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN1ConfigId,
        params)

    // SimClusters ANN cluster 2 Query
    val interestedInSimClustersANN2Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        interestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN2ConfigId,
        params)

    val nextInterestedInSimClustersANN2Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        nextInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN2ConfigId,
        params)

    val addressbookInterestedInSimClustersANN2Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        addressbookInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN2ConfigId,
        params)

    // SimClusters ANN cluster 3 Query
    val interestedInSimClustersANN3Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        interestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN3ConfigId,
        params)

    val nextInterestedInSimClustersANN3Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        nextInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN3ConfigId,
        params)

    val addressbookInterestedInSimClustersANN3Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        addressbookInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN3ConfigId,
        params)

    // SimClusters ANN cluster 5 Query
    val interestedInSimClustersANN5Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        interestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN5ConfigId,
        params)
    // SimClusters ANN cluster 4 Query
    val interestedInSimClustersANN4Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        interestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN4ConfigId,
        params)

    val nextInterestedInSimClustersANN5Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        nextInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN5ConfigId,
        params)

    val nextInterestedInSimClustersANN4Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        nextInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN4ConfigId,
        params)

    val addressbookInterestedInSimClustersANN5Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        addressbookInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN5ConfigId,
        params)

    val addressbookInterestedInSimClustersANN4Query =
      SimClustersANNSimilarityEngine.fromParams(
        internalId,
        addressbookInterestedInEmbedding.embeddingType,
        simClustersModelVersion,
        simClustersANN4ConfigId,
        params)

    Query(
      internalId = internalId,
      enableUserInterestedIn = params(InterestedInParams.EnableSourceParam),
      enableUserNextInterestedIn = params(InterestedInParams.EnableSourceSequentialModelParam),
      enableAddressBookNextInterestedIn = params(InterestedInParams.EnableSourceAddressBookParam),
      enableProdSimClustersANNSimilarityEngine =
        params(InterestedInParams.EnableProdSimClustersANNParam),
      enableExperimentalSimClustersANNSimilarityEngine =
        params(InterestedInParams.EnableExperimentalSimClustersANNParam),
      enableSimClustersANN1SimilarityEngine = params(InterestedInParams.EnableSimClustersANN1Param),
      enableSimClustersANN2SimilarityEngine = params(InterestedInParams.EnableSimClustersANN2Param),
      enableSimClustersANN3SimilarityEngine = params(InterestedInParams.EnableSimClustersANN3Param),
      enableSimClustersANN5SimilarityEngine = params(InterestedInParams.EnableSimClustersANN5Param),
      enableSimClustersANN4SimilarityEngine = params(InterestedInParams.EnableSimClustersANN4Param),
      simClustersInterestedInMinScore = simClustersInterestedInMinScore,
      simClustersNextInterestedInMinScore = simClustersNextInterestedInMinScore,
      simClustersAddressBookInterestedInMinScore = simClustersAddressBookInterestedInMinScore,
      interestedInSimClustersANNQuery = interestedInSimClustersANNQuery,
      nextInterestedInSimClustersANNQuery = nextInterestedInSimClustersANNQuery,
      addressbookInterestedInSimClustersANNQuery = addressbookInterestedInSimClustersANNQuery,
      interestedInExperimentalSimClustersANNQuery = interestedInExperimentalSimClustersANNQuery,
      nextInterestedInExperimentalSimClustersANNQuery =
        nextInterestedInExperimentalSimClustersANNQuery,
      addressbookInterestedInExperimentalSimClustersANNQuery =
        addressbookInterestedInExperimentalSimClustersANNQuery,
      interestedInSimClustersANN1Query = interestedInSimClustersANN1Query,
      nextInterestedInSimClustersANN1Query = nextInterestedInSimClustersANN1Query,
      addressbookInterestedInSimClustersANN1Query = addressbookInterestedInSimClustersANN1Query,
      interestedInSimClustersANN2Query = interestedInSimClustersANN2Query,
      nextInterestedInSimClustersANN2Query = nextInterestedInSimClustersANN2Query,
      addressbookInterestedInSimClustersANN2Query = addressbookInterestedInSimClustersANN2Query,
      interestedInSimClustersANN3Query = interestedInSimClustersANN3Query,
      nextInterestedInSimClustersANN3Query = nextInterestedInSimClustersANN3Query,
      addressbookInterestedInSimClustersANN3Query = addressbookInterestedInSimClustersANN3Query,
      interestedInSimClustersANN5Query = interestedInSimClustersANN5Query,
      nextInterestedInSimClustersANN5Query = nextInterestedInSimClustersANN5Query,
      addressbookInterestedInSimClustersANN5Query = addressbookInterestedInSimClustersANN5Query,
      interestedInSimClustersANN4Query = interestedInSimClustersANN4Query,
      nextInterestedInSimClustersANN4Query = nextInterestedInSimClustersANN4Query,
      addressbookInterestedInSimClustersANN4Query = addressbookInterestedInSimClustersANN4Query,
    )
  }
}
package com.twitter.cr_mixer.candidate_generation

import com.twitter.contentrecommender.thriftscala.TweetInfo
import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.GraphSourceInfo
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.ModelConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.model.TripTweetWithScore
import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.model.TweetWithScoreAndSocialProof
import com.twitter.cr_mixer.param.ConsumerBasedWalsParams
import com.twitter.cr_mixer.param.ConsumerEmbeddingBasedCandidateGenerationParams
import com.twitter.cr_mixer.param.ConsumersBasedUserVideoGraphParams
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.cr_mixer.similarity_engine.ConsumersBasedUserVideoGraphSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.ConsumerBasedWalsSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.ConsumerEmbeddingBasedTripSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.ConsumerEmbeddingBasedTwHINSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.ConsumerEmbeddingBasedTwoTowerSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.EngineQuery
import com.twitter.cr_mixer.similarity_engine.FilterUtil
import com.twitter.cr_mixer.similarity_engine.HnswANNEngineQuery
import com.twitter.cr_mixer.similarity_engine.HnswANNSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.ProducerBasedUnifiedSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.StandardSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.TripEngineQuery
import com.twitter.cr_mixer.similarity_engine.TweetBasedUnifiedSimilarityEngine
import com.twitter.cr_mixer.similarity_engine.UserTweetEntityGraphSimilarityEngine
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

/**
 * Route the SourceInfo to the associated Candidate Engines.
 */
@Singleton
case class CandidateSourcesRouter @Inject() (
  customizedRetrievalCandidateGeneration: CustomizedRetrievalCandidateGeneration,
  simClustersInterestedInCandidateGeneration: SimClustersInterestedInCandidateGeneration,
  @Named(ModuleNames.TweetBasedUnifiedSimilarityEngine)
  tweetBasedUnifiedSimilarityEngine: StandardSimilarityEngine[
    TweetBasedUnifiedSimilarityEngine.Query,
    TweetWithCandidateGenerationInfo
  ],
  @Named(ModuleNames.ProducerBasedUnifiedSimilarityEngine)
  producerBasedUnifiedSimilarityEngine: StandardSimilarityEngine[
    ProducerBasedUnifiedSimilarityEngine.Query,
    TweetWithCandidateGenerationInfo
  ],
  @Named(ModuleNames.ConsumerEmbeddingBasedTripSimilarityEngine)
  consumerEmbeddingBasedTripSimilarityEngine: StandardSimilarityEngine[
    TripEngineQuery,
    TripTweetWithScore
  ],
  @Named(ModuleNames.ConsumerEmbeddingBasedTwHINANNSimilarityEngine)
  consumerBasedTwHINANNSimilarityEngine: HnswANNSimilarityEngine,
  @Named(ModuleNames.ConsumerEmbeddingBasedTwoTowerANNSimilarityEngine)
  consumerBasedTwoTowerSimilarityEngine: HnswANNSimilarityEngine,
  @Named(ModuleNames.ConsumersBasedUserVideoGraphSimilarityEngine)
  consumersBasedUserVideoGraphSimilarityEngine: StandardSimilarityEngine[
    ConsumersBasedUserVideoGraphSimilarityEngine.Query,
    TweetWithScore
  ],
  @Named(ModuleNames.UserTweetEntityGraphSimilarityEngine) userTweetEntityGraphSimilarityEngine: StandardSimilarityEngine[
    UserTweetEntityGraphSimilarityEngine.Query,
    TweetWithScoreAndSocialProof
  ],
  @Named(ModuleNames.ConsumerBasedWalsSimilarityEngine)
  consumerBasedWalsSimilarityEngine: StandardSimilarityEngine[
    ConsumerBasedWalsSimilarityEngine.Query,
    TweetWithScore
  ],
  tweetInfoStore: ReadableStore[TweetId, TweetInfo],
  globalStats: StatsReceiver,
) {

  import CandidateSourcesRouter._
  val stats: StatsReceiver = globalStats.scope(this.getClass.getSimpleName)

  def fetchCandidates(
    requestUserId: UserId,
    sourceSignals: Set[SourceInfo],
    sourceGraphs: Map[String, Option[GraphSourceInfo]],
    params: configapi.Params,
  ): Future[Seq[Seq[InitialCandidate]]] = {

    val tweetBasedCandidatesFuture = getCandidates(
      getTweetBasedSourceInfo(sourceSignals),
      params,
      TweetBasedUnifiedSimilarityEngine.fromParams,
      tweetBasedUnifiedSimilarityEngine.getCandidates)

    val producerBasedCandidatesFuture =
      getCandidates(
        getProducerBasedSourceInfo(sourceSignals),
        params,
        ProducerBasedUnifiedSimilarityEngine.fromParams(_, _),
        producerBasedUnifiedSimilarityEngine.getCandidates
      )

    val simClustersInterestedInBasedCandidatesFuture =
      getCandidatesPerSimilarityEngineModel(
        requestUserId,
        params,
        SimClustersInterestedInCandidateGeneration.fromParams,
        simClustersInterestedInCandidateGeneration.get)

    val consumerEmbeddingBasedLogFavBasedTripCandidatesFuture =
      if (params(
          ConsumerEmbeddingBasedCandidateGenerationParams.EnableLogFavBasedSimClustersTripParam)) {
        getSimClustersTripCandidates(
          params,
          ConsumerEmbeddingBasedTripSimilarityEngine.fromParams(
            ModelConfig.ConsumerLogFavBasedInterestedInEmbedding,
            InternalId.UserId(requestUserId),
            params
          ),
          consumerEmbeddingBasedTripSimilarityEngine
        ).map {
          Seq(_)
        }
      } else
        Future.Nil

    val consumersBasedUvgRealGraphInCandidatesFuture =
      if (params(ConsumersBasedUserVideoGraphParams.EnableSourceParam)) {
        val realGraphInGraphSourceInfoOpt =
          getGraphSourceInfoBySourceType(SourceType.RealGraphIn.name, sourceGraphs)

        getGraphBasedCandidates(
          params,
          ConsumersBasedUserVideoGraphSimilarityEngine
            .fromParamsForRealGraphIn(
              realGraphInGraphSourceInfoOpt
                .map { graphSourceInfo => graphSourceInfo.seedWithScores }.getOrElse(Map.empty),
              params),
          consumersBasedUserVideoGraphSimilarityEngine,
          ConsumersBasedUserVideoGraphSimilarityEngine.toSimilarityEngineInfo,
          realGraphInGraphSourceInfoOpt
        ).map {
          Seq(_)
        }
      } else Future.Nil

    val consumerEmbeddingBasedFollowBasedTripCandidatesFuture =
      if (params(
          ConsumerEmbeddingBasedCandidateGenerationParams.EnableFollowBasedSimClustersTripParam)) {
        getSimClustersTripCandidates(
          params,
          ConsumerEmbeddingBasedTripSimilarityEngine.fromParams(
            ModelConfig.ConsumerFollowBasedInterestedInEmbedding,
            InternalId.UserId(requestUserId),
            params
          ),
          consumerEmbeddingBasedTripSimilarityEngine
        ).map {
          Seq(_)
        }
      } else
        Future.Nil

    val consumerBasedWalsCandidatesFuture =
      if (params(
          ConsumerBasedWalsParams.EnableSourceParam
        )) {
        getConsumerBasedWalsCandidates(sourceSignals, params)
      }.map { Seq(_) }
      else Future.Nil

    val consumerEmbeddingBasedTwHINCandidatesFuture =
      if (params(ConsumerEmbeddingBasedCandidateGenerationParams.EnableTwHINParam)) {
        getHnswCandidates(
          params,
          ConsumerEmbeddingBasedTwHINSimilarityEngine.fromParams(
            InternalId.UserId(requestUserId),
            params),
          consumerBasedTwHINANNSimilarityEngine
        ).map { Seq(_) }
      } else Future.Nil

    val consumerEmbeddingBasedTwoTowerCandidatesFuture =
      if (params(ConsumerEmbeddingBasedCandidateGenerationParams.EnableTwoTowerParam)) {
        getHnswCandidates(
          params,
          ConsumerEmbeddingBasedTwoTowerSimilarityEngine.fromParams(
            InternalId.UserId(requestUserId),
            params),
          consumerBasedTwoTowerSimilarityEngine
        ).map {
          Seq(_)
        }
      } else Future.Nil

    val customizedRetrievalBasedCandidatesFuture =
      getCandidatesPerSimilarityEngineModel(
        requestUserId,
        params,
        CustomizedRetrievalCandidateGeneration.fromParams,
        customizedRetrievalCandidateGeneration.get)

    Future
      .collect(
        Seq(
          tweetBasedCandidatesFuture,
          producerBasedCandidatesFuture,
          simClustersInterestedInBasedCandidatesFuture,
          consumerBasedWalsCandidatesFuture,
          consumerEmbeddingBasedLogFavBasedTripCandidatesFuture,
          consumerEmbeddingBasedFollowBasedTripCandidatesFuture,
          consumerEmbeddingBasedTwHINCandidatesFuture,
          consumerEmbeddingBasedTwoTowerCandidatesFuture,
          consumersBasedUvgRealGraphInCandidatesFuture,
          customizedRetrievalBasedCandidatesFuture
        )).map { candidatesList =>
        // remove empty innerSeq
        val result = candidatesList.flatten.filter(_.nonEmpty)
        stats.stat("numOfSequences").add(result.size)
        stats.stat("flattenCandidatesWithDup").add(result.flatten.size)

        result
      }
  }

  private def getGraphBasedCandidates[QueryType](
    params: configapi.Params,
    query: EngineQuery[QueryType],
    engine: StandardSimilarityEngine[QueryType, TweetWithScore],
    toSimilarityEngineInfo: Double => SimilarityEngineInfo,
    graphSourceInfoOpt: Option[GraphSourceInfo] = None
  ): Future[Seq[InitialCandidate]] = {
    val candidatesOptFut = engine.getCandidates(query)
    val tweetsWithCandidateGenerationInfoOptFut = candidatesOptFut.map {
      _.map { tweetsWithScores =>
        val sortedCandidates = tweetsWithScores.sortBy(-_.score)
        engine.getScopedStats.stat("sortedCandidates_size").add(sortedCandidates.size)
        val tweetsWithCandidateGenerationInfo = sortedCandidates.map { tweetWithScore =>
          {
            val similarityEngineInfo = toSimilarityEngineInfo(tweetWithScore.score)
            val sourceInfo = graphSourceInfoOpt.map { graphSourceInfo =>
              // The internalId is a placeholder value. We do not plan to store the full seedUserId set.
              SourceInfo(
                sourceType = graphSourceInfo.sourceType,
                internalId = InternalId.UserId(0L),
                sourceEventTime = None
              )
            }
            TweetWithCandidateGenerationInfo(
              tweetWithScore.tweetId,
              CandidateGenerationInfo(
                sourceInfo,
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
      initialCandidates <- convertToInitialCandidates(
        tweetsWithCandidateGenerationInfoOpt.toSeq.flatten)
    } yield initialCandidates
  }

  private def getCandidates[QueryType](
    sourceSignals: Set[SourceInfo],
    params: configapi.Params,
    fromParams: (SourceInfo, configapi.Params) => QueryType,
    getFunc: QueryType => Future[Option[Seq[TweetWithCandidateGenerationInfo]]]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    val queries = sourceSignals.map { sourceInfo =>
      fromParams(sourceInfo, params)
    }.toSeq

    Future
      .collect {
        queries.map { query =>
          for {
            candidates <- getFunc(query)
            prefilterCandidates <- convertToInitialCandidates(candidates.toSeq.flatten)
          } yield {
            prefilterCandidates
          }
        }
      }
  }

  private def getConsumerBasedWalsCandidates(
    sourceSignals: Set[SourceInfo],
    params: configapi.Params
  ): Future[Seq[InitialCandidate]] = {
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
      initialCandidates <- convertToInitialCandidates(
        tweetsWithCandidateGenerationInfoOpt.toSeq.flatten)
    } yield initialCandidates
  }

  private def getSimClustersTripCandidates(
    params: configapi.Params,
    query: TripEngineQuery,
    engine: StandardSimilarityEngine[
      TripEngineQuery,
      TripTweetWithScore
    ],
  ): Future[Seq[InitialCandidate]] = {
    val tweetsWithCandidatesGenerationInfoOptFut =
      engine.getCandidates(EngineQuery(query, params)).map {
        _.map {
          _.map { tweetWithScore =>
            // define filters
            TweetWithCandidateGenerationInfo(
              tweetWithScore.tweetId,
              CandidateGenerationInfo(
                None,
                SimilarityEngineInfo(
                  SimilarityEngineType.ExploreTripOfflineSimClustersTweets,
                  None,
                  Some(tweetWithScore.score)),
                Seq.empty
              )
            )
          }
        }
      }
    for {
      tweetsWithCandidateGenerationInfoOpt <- tweetsWithCandidatesGenerationInfoOptFut
      initialCandidates <- convertToInitialCandidates(
        tweetsWithCandidateGenerationInfoOpt.toSeq.flatten)
    } yield initialCandidates
  }

  private def getHnswCandidates(
    params: configapi.Params,
    query: HnswANNEngineQuery,
    engine: HnswANNSimilarityEngine,
  ): Future[Seq[InitialCandidate]] = {
    val candidatesOptFut = engine.getCandidates(query)
    val tweetsWithCandidateGenerationInfoOptFut = candidatesOptFut.map {
      _.map { tweetsWithScores =>
        val sortedCandidates = tweetsWithScores.sortBy(-_.score)
        val filteredCandidates =
          FilterUtil.tweetAgeFilter(sortedCandidates, params(GlobalParams.MaxTweetAgeHoursParam))
        engine.getScopedStats.stat("filteredCandidates_size").add(filteredCandidates.size)
        val tweetsWithCandidateGenerationInfo = filteredCandidates.map { tweetWithScore =>
          {
            val similarityEngineInfo =
              engine.toSimilarityEngineInfo(query, tweetWithScore.score)
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
      initialCandidates <- convertToInitialCandidates(
        tweetsWithCandidateGenerationInfoOpt.toSeq.flatten)
    } yield initialCandidates
  }

  /**
   * Returns candidates from each similarity engine separately.
   * For 1 requestUserId, it will fetch results from each similarity engine e_i,
   * and returns Seq[Seq[TweetCandidate]].
   */
  private def getCandidatesPerSimilarityEngineModel[QueryType](
    requestUserId: UserId,
    params: configapi.Params,
    fromParams: (InternalId, configapi.Params) => QueryType,
    getFunc: QueryType => Future[
      Option[Seq[Seq[TweetWithCandidateGenerationInfo]]]
    ]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    val query = fromParams(InternalId.UserId(requestUserId), params)
    getFunc(query).flatMap { candidatesPerSimilarityEngineModelOpt =>
      val candidatesPerSimilarityEngineModel = candidatesPerSimilarityEngineModelOpt.toSeq.flatten
      Future.collect {
        candidatesPerSimilarityEngineModel.map(convertToInitialCandidates)
      }
    }
  }

  private[candidate_generation] def convertToInitialCandidates(
    candidates: Seq[TweetWithCandidateGenerationInfo],
  ): Future[Seq[InitialCandidate]] = {
    val tweetIds = candidates.map(_.tweetId).toSet
    Future.collect(tweetInfoStore.multiGet(tweetIds)).map { tweetInfos =>
      /***
       * If tweetInfo does not exist, we will filter out this tweet candidate.
       */
      candidates.collect {
        case candidate if tweetInfos.getOrElse(candidate.tweetId, None).isDefined =>
          val tweetInfo = tweetInfos(candidate.tweetId)
            .getOrElse(throw new IllegalStateException("Check previous line's condition"))

          InitialCandidate(
            tweetId = candidate.tweetId,
            tweetInfo = tweetInfo,
            candidate.candidateGenerationInfo
          )
      }
    }
  }
}

object CandidateSourcesRouter {
  def getGraphSourceInfoBySourceType(
    sourceTypeStr: String,
    sourceGraphs: Map[String, Option[GraphSourceInfo]]
  ): Option[GraphSourceInfo] = {
    sourceGraphs.getOrElse(sourceTypeStr, None)
  }

  def getTweetBasedSourceInfo(
    sourceSignals: Set[SourceInfo]
  ): Set[SourceInfo] = {
    sourceSignals.collect {
      case sourceInfo
          if AllowedSourceTypesForTweetBasedUnifiedSE.contains(sourceInfo.sourceType.value) =>
        sourceInfo
    }
  }

  def getProducerBasedSourceInfo(
    sourceSignals: Set[SourceInfo]
  ): Set[SourceInfo] = {
    sourceSignals.collect {
      case sourceInfo
          if AllowedSourceTypesForProducerBasedUnifiedSE.contains(sourceInfo.sourceType.value) =>
        sourceInfo
    }
  }

  def getConsumerBasedWalsSourceInfo(
    sourceSignals: Set[SourceInfo]
  ): Set[SourceInfo] = {
    sourceSignals.collect {
      case sourceInfo
          if AllowedSourceTypesForConsumerBasedWalsSE.contains(sourceInfo.sourceType.value) =>
        sourceInfo
    }
  }

  /***
   * Signal funneling should not exist in CG or even in any SimilarityEngine.
   * They will be in Router, or eventually, in CrCandidateGenerator.
   */
  val AllowedSourceTypesForConsumerBasedWalsSE = Set(
    SourceType.TweetFavorite.value,
    SourceType.Retweet.value,
    SourceType.TweetDontLike.value, //currently no-op
    SourceType.TweetReport.value, //currently no-op
    SourceType.AccountMute.value, //currently no-op
    SourceType.AccountBlock.value //currently no-op
  )
  val AllowedSourceTypesForTweetBasedUnifiedSE = Set(
    SourceType.TweetFavorite.value,
    SourceType.Retweet.value,
    SourceType.OriginalTweet.value,
    SourceType.Reply.value,
    SourceType.TweetShare.value,
    SourceType.NotificationClick.value,
    SourceType.GoodTweetClick.value,
    SourceType.VideoTweetQualityView.value,
    SourceType.VideoTweetPlayback50.value,
    SourceType.TweetAggregation.value,
  )
  val AllowedSourceTypesForProducerBasedUnifiedSE = Set(
    SourceType.UserFollow.value,
    SourceType.UserRepeatedProfileVisit.value,
    SourceType.RealGraphOon.value,
    SourceType.FollowRecommendation.value,
    SourceType.UserTrafficAttributionProfileVisit.value,
    SourceType.GoodProfileClick.value,
    SourceType.ProducerAggregation.value,
  )
}
package com.twitter.cr_mixer
package exception

case class InvalidSANNConfigException(msg: String) extends Exception(msg)
package com.twitter.cr_mixer.ranker

import com.twitter.cr_mixer.model.BlendedCandidate
import com.twitter.cr_mixer.model.CrCandidateGeneratorQuery
import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future
import com.twitter.util.JavaTimer
import com.twitter.util.Time
import com.twitter.util.Timer
import javax.inject.Inject
import javax.inject.Singleton

/**
 * CR-Mixer internal ranker
 */
@Singleton
class SwitchRanker @Inject() (
  defaultRanker: DefaultRanker,
  globalStats: StatsReceiver) {
  private val stats: StatsReceiver = globalStats.scope(this.getClass.getCanonicalName)
  implicit val timer: Timer = new JavaTimer(true)

  def rank(
    query: CrCandidateGeneratorQuery,
    candidates: Seq[BlendedCandidate],
  ): Future[Seq[RankedCandidate]] = {
    defaultRanker.rank(candidates)
  }

}

object SwitchRanker {

  /** Prefers candidates generated from sources with the latest timestamps.
   * The newer the source signal, the higher a candidate ranks.
   * This ordering biases against consumer-based candidates because their timestamp defaults to 0
   */
  val TimestampOrder: Ordering[RankedCandidate] =
    math.Ordering
      .by[RankedCandidate, Time](
        _.reasonChosen.sourceInfoOpt
          .flatMap(_.sourceEventTime)
          .getOrElse(Time.fromMilliseconds(0L)))
      .reverse
}
package com.twitter.cr_mixer.ranker

import com.twitter.cr_mixer.model.BlendedCandidate
import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.util.Future
import javax.inject.Singleton

/**
 * Keep the same order as the input.
 */
@Singleton
class DefaultRanker() {
  def rank(
    candidates: Seq[BlendedCandidate],
  ): Future[Seq[RankedCandidate]] = {
    val candidateSize = candidates.size
    val rankedCandidates = candidates.zipWithIndex.map {
      case (candidate, index) =>
        candidate.toRankedCandidate((candidateSize - index).toDouble)
    }
    Future.value(rankedCandidates)
  }
}
package com.twitter.cr_mixer.blender

import com.twitter.cr_mixer.model.BlendedCandidate
import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.simclusters_v2.common.TweetId
import scala.collection.mutable

object BlendedCandidatesBuilder {

  /**
   * @param inputCandidates input candidate prior to interleaving
   * @param interleavedCandidates after interleaving. These tweets are de-duplicated.
   */
  def build(
    inputCandidates: Seq[Seq[InitialCandidate]],
    interleavedCandidates: Seq[InitialCandidate]
  ): Seq[BlendedCandidate] = {
    val cgInfoLookupMap = buildCandidateToCGInfosMap(inputCandidates)
    interleavedCandidates.map { interleavedCandidate =>
      interleavedCandidate.toBlendedCandidate(cgInfoLookupMap(interleavedCandidate.tweetId))
    }
  }

  /**
   * The same tweet can be generated by different sources.
   * This function tells you which CandidateGenerationInfo generated a given tweet
   */
  private def buildCandidateToCGInfosMap(
    candidateSeq: Seq[Seq[InitialCandidate]],
  ): Map[TweetId, Seq[CandidateGenerationInfo]] = {
    val tweetIdMap = mutable.HashMap[TweetId, Seq[CandidateGenerationInfo]]()

    candidateSeq.foreach { candidates =>
      candidates.foreach { candidate =>
        val candidateGenerationInfoSeq = {
          tweetIdMap.getOrElse(candidate.tweetId, Seq.empty)
        }
        val candidateGenerationInfo = candidate.candidateGenerationInfo
        tweetIdMap.put(
          candidate.tweetId,
          candidateGenerationInfoSeq ++ Seq(candidateGenerationInfo))
      }
    }
    tweetIdMap.toMap
  }

}
package com.twitter.cr_mixer.blender

import com.twitter.cr_mixer.model.BlendedAdsCandidate
import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.InitialAdsCandidate
import com.twitter.cr_mixer.util.InterleaveUtil
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Singleton
import scala.collection.mutable

@Singleton
case class AdsBlender @Inject() (globalStats: StatsReceiver) {

  private val name: String = this.getClass.getCanonicalName
  private val stats: StatsReceiver = globalStats.scope(name)

  /**
   * Interleaves candidates by iteratively choosing InterestedIn candidates and TWISTLY candidates
   * in turn. InterestedIn candidates have no source signal, whereas TWISTLY candidates do. TWISTLY
   * candidates themselves are interleaved by source before equal blending with InterestedIn
   * candidates.
   */
  def blend(
    inputCandidates: Seq[Seq[InitialAdsCandidate]],
  ): Future[Seq[BlendedAdsCandidate]] = {

    // Filter out empty candidate sequence
    val candidates = inputCandidates.filter(_.nonEmpty)
    val (interestedInCandidates, twistlyCandidates) =
      candidates.partition(_.head.candidateGenerationInfo.sourceInfoOpt.isEmpty)
    // First interleave twistly candidates
    val interleavedTwistlyCandidates = InterleaveUtil.interleave(twistlyCandidates)

    val twistlyAndInterestedInCandidates =
      Seq(interestedInCandidates.flatten, interleavedTwistlyCandidates)

    // then interleave  twistly candidates with interested in to make them even
    val interleavedCandidates = InterleaveUtil.interleave(twistlyAndInterestedInCandidates)

    stats.stat("candidates").add(interleavedCandidates.size)

    val blendedCandidates = buildBlendedAdsCandidate(inputCandidates, interleavedCandidates)
    Future.value(blendedCandidates)
  }
  private def buildBlendedAdsCandidate(
    inputCandidates: Seq[Seq[InitialAdsCandidate]],
    interleavedCandidates: Seq[InitialAdsCandidate]
  ): Seq[BlendedAdsCandidate] = {
    val cgInfoLookupMap = buildCandidateToCGInfosMap(inputCandidates)
    interleavedCandidates.map { interleavedCandidate =>
      interleavedCandidate.toBlendedAdsCandidate(cgInfoLookupMap(interleavedCandidate.tweetId))
    }
  }

  private def buildCandidateToCGInfosMap(
    candidateSeq: Seq[Seq[InitialAdsCandidate]],
  ): Map[TweetId, Seq[CandidateGenerationInfo]] = {
    val tweetIdMap = mutable.HashMap[TweetId, Seq[CandidateGenerationInfo]]()

    candidateSeq.foreach { candidates =>
      candidates.foreach { candidate =>
        val candidateGenerationInfoSeq = {
          tweetIdMap.getOrElse(candidate.tweetId, Seq.empty)
        }
        val candidateGenerationInfo = candidate.candidateGenerationInfo
        tweetIdMap.put(
          candidate.tweetId,
          candidateGenerationInfoSeq ++ Seq(candidateGenerationInfo))
      }
    }
    tweetIdMap.toMap
  }

}
package com.twitter.cr_mixer.blender

import com.twitter.cr_mixer.model.BlendedCandidate
import com.twitter.cr_mixer.model.CrCandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.param.BlenderParams
import com.twitter.cr_mixer.util.CountWeightedInterleaveUtil
import com.twitter.cr_mixer.util.InterleaveUtil
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.timelines.configapi.Params
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Singleton

/**
 * A weighted round robin interleaving algorithm.
 * The weight of each blending group based on the count of candidates in each blending group.
 * The more candidates under a blending group, the more candidates are selected from it during round
 * robin, which in effect prioritizes this group.
 *
 * Weights sum up to 1. For example:
 * total candidates = 8
 *             Group                       Weight
 *         [A1, A2, A3, A4]          4/8 = 0.5  // select 50% of results from group A
 *         [B1, B2]                  2/8 = 0.25 // 25% from group B
 *         [C1, C2]                  2/8 = 0.25 // 25% from group C
 *
 * Blended results = [A1, A2, B1, C1, A3, A4, B2, C2]
 * See @linht's go/weighted-interleave
 */
@Singleton
case class CountWeightedInterleaveBlender @Inject() (globalStats: StatsReceiver) {
  import CountWeightedInterleaveBlender._

  private val name: String = this.getClass.getCanonicalName
  private val stats: StatsReceiver = globalStats.scope(name)

  def blend(
    query: CrCandidateGeneratorQuery,
    inputCandidates: Seq[Seq[InitialCandidate]]
  ): Future[Seq[BlendedCandidate]] = {
    val weightedBlenderQuery = CountWeightedInterleaveBlender.paramToQuery(query.params)
    countWeightedInterleave(weightedBlenderQuery, inputCandidates)
  }

  private[blender] def countWeightedInterleave(
    query: WeightedBlenderQuery,
    inputCandidates: Seq[Seq[InitialCandidate]],
  ): Future[Seq[BlendedCandidate]] = {

    val candidatesAndWeightKeyByIndexId: Seq[(Seq[InitialCandidate], Double)] = {
      CountWeightedInterleaveUtil.buildInitialCandidatesWithWeightKeyByFeature(
        inputCandidates,
        query.rankerWeightShrinkage)
    }

    val interleavedCandidates =
      InterleaveUtil.weightedInterleave(candidatesAndWeightKeyByIndexId, query.maxWeightAdjustments)

    stats.stat("candidates").add(interleavedCandidates.size)

    val blendedCandidates = BlendedCandidatesBuilder.build(inputCandidates, interleavedCandidates)
    Future.value(blendedCandidates)
  }
}

object CountWeightedInterleaveBlender {

  /**
   * We pass two parameters to the weighted interleaver:
   * @param rankerWeightShrinkage shrinkage parameter between [0, 1] that determines how close we
   *                              stay to uniform sampling. The bigger the shrinkage the
   *                              closer we are to uniform round robin
   * @param maxWeightAdjustments max number of weighted sampling to do prior to defaulting to
   *                             uniform. Set so that we avoid infinite loops (e.g. if weights are
   *                             0)
   */
  case class WeightedBlenderQuery(
    rankerWeightShrinkage: Double,
    maxWeightAdjustments: Int)

  def paramToQuery(params: Params): WeightedBlenderQuery = {
    val rankerWeightShrinkage: Double =
      params(BlenderParams.RankingInterleaveWeightShrinkageParam)
    val maxWeightAdjustments: Int =
      params(BlenderParams.RankingInterleaveMaxWeightAdjustments)

    WeightedBlenderQuery(rankerWeightShrinkage, maxWeightAdjustments)
  }
}
package com.twitter.cr_mixer.blender

import com.twitter.cr_mixer.blender.ImplicitSignalBackFillBlender.BackFillSourceTypes
import com.twitter.cr_mixer.blender.ImplicitSignalBackFillBlender.BackFillSourceTypesWithVideo
import com.twitter.cr_mixer.model.BlendedCandidate
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.param.BlenderParams
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.cr_mixer.util.InterleaveUtil
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.timelines.configapi.Params
import com.twitter.util.Future
import javax.inject.Inject

case class SourceTypeBackFillBlender @Inject() (globalStats: StatsReceiver) {

  private val name: String = this.getClass.getCanonicalName
  private val stats: StatsReceiver = globalStats.scope(name)

  /**
   *  Partition the candidates based on source type
   *  Interleave the two partitions of candidates separately
   *  Then append the back fill candidates to the end
   */
  def blend(
    params: Params,
    inputCandidates: Seq[Seq[InitialCandidate]],
  ): Future[Seq[BlendedCandidate]] = {

    // Filter out empty candidate sequence
    val candidates = inputCandidates.filter(_.nonEmpty)

    val backFillSourceTypes =
      if (params(BlenderParams.SourceTypeBackFillEnableVideoBackFill)) BackFillSourceTypesWithVideo
      else BackFillSourceTypes
    // partition candidates based on their source types
    val (backFillCandidates, regularCandidates) =
      candidates.partition(
        _.head.candidateGenerationInfo.sourceInfoOpt
          .exists(sourceInfo => backFillSourceTypes.contains(sourceInfo.sourceType)))

    val interleavedRegularCandidates = InterleaveUtil.interleave(regularCandidates)
    val interleavedBackFillCandidates =
      InterleaveUtil.interleave(backFillCandidates)
    stats.stat("backFillCandidates").add(interleavedBackFillCandidates.size)
    // Append interleaved backfill candidates to the end
    val interleavedCandidates = interleavedRegularCandidates ++ interleavedBackFillCandidates

    stats.stat("candidates").add(interleavedCandidates.size)

    val blendedCandidates = BlendedCandidatesBuilder.build(inputCandidates, interleavedCandidates)
    Future.value(blendedCandidates)
  }

}

object ImplicitSignalBackFillBlender {
  final val BackFillSourceTypesWithVideo: Set[SourceType] = Set(
    SourceType.UserRepeatedProfileVisit,
    SourceType.VideoTweetPlayback50,
    SourceType.VideoTweetQualityView)

  final val BackFillSourceTypes: Set[SourceType] = Set(SourceType.UserRepeatedProfileVisit)
}
package com.twitter.cr_mixer.featureswitch

import com.twitter.abdecider.LoggingABDecider
import com.twitter.abdecider.UserRecipient
import com.twitter.cr_mixer.{thriftscala => t}
import com.twitter.core_workflows.user_model.thriftscala.UserState
import com.twitter.discovery.common.configapi.FeatureContextBuilder
import com.twitter.featureswitches.FSRecipient
import com.twitter.featureswitches.UserAgent
import com.twitter.featureswitches.{Recipient => FeatureSwitchRecipient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.product_mixer.core.thriftscala.ClientContext
import com.twitter.timelines.configapi.Config
import com.twitter.timelines.configapi.FeatureValue
import com.twitter.timelines.configapi.ForcedFeatureContext
import com.twitter.timelines.configapi.OrElseFeatureContext
import com.twitter.timelines.configapi.Params
import com.twitter.timelines.configapi.RequestContext
import com.twitter.timelines.configapi.abdecider.LoggingABDeciderExperimentContext
import javax.inject.Inject
import javax.inject.Singleton

/** Singleton object for building [[Params]] to override */
@Singleton
class ParamsBuilder @Inject() (
  globalStats: StatsReceiver,
  abDecider: LoggingABDecider,
  featureContextBuilder: FeatureContextBuilder,
  config: Config) {

  private val stats = globalStats.scope("params")

  def buildFromClientContext(
    clientContext: ClientContext,
    product: t.Product,
    userState: UserState,
    userRoleOverride: Option[Set[String]] = None,
    featureOverrides: Map[String, FeatureValue] = Map.empty,
  ): Params = {
    clientContext.userId match {
      case Some(userId) =>
        val userRecipient = buildFeatureSwitchRecipient(
          userId,
          userRoleOverride,
          clientContext,
          product,
          userState
        )

        val featureContext = OrElseFeatureContext(
          ForcedFeatureContext(featureOverrides),
          featureContextBuilder(
            Some(userId),
            Some(userRecipient)
          ))

        config(
          requestContext = RequestContext(
            userId = Some(userId),
            experimentContext = LoggingABDeciderExperimentContext(
              abDecider,
              Some(UserRecipient(userId, Some(userId)))),
            featureContext = featureContext
          ),
          stats
        )
      case None =>
        val guestRecipient =
          buildFeatureSwitchRecipientWithGuestId(clientContext: ClientContext, product, userState)

        val featureContext = OrElseFeatureContext(
          ForcedFeatureContext(featureOverrides),
          featureContextBuilder(
            clientContext.userId,
            Some(guestRecipient)
          )
        ) //ExperimentContext with GuestRecipient is not supported  as there is no active use-cases yet in CrMixer

        config(
          requestContext = RequestContext(
            userId = clientContext.userId,
            featureContext = featureContext
          ),
          stats
        )
    }
  }

  private def buildFeatureSwitchRecipientWithGuestId(
    clientContext: ClientContext,
    product: t.Product,
    userState: UserState
  ): FeatureSwitchRecipient = {

    val recipient = FSRecipient(
      userId = None,
      userRoles = None,
      deviceId = clientContext.deviceId,
      guestId = clientContext.guestId,
      languageCode = clientContext.languageCode,
      countryCode = clientContext.countryCode,
      userAgent = clientContext.userAgent.flatMap(UserAgent(_)),
      isVerified = None,
      isTwoffice = None,
      tooClient = None,
      highWaterMark = None
    )

    recipient.withCustomFields(
      (ParamsBuilder.ProductCustomField, product.toString),
      (ParamsBuilder.UserStateCustomField, userState.toString)
    )
  }

  private def buildFeatureSwitchRecipient(
    userId: Long,
    userRolesOverride: Option[Set[String]],
    clientContext: ClientContext,
    product: t.Product,
    userState: UserState
  ): FeatureSwitchRecipient = {
    val userRoles = userRolesOverride match {
      case Some(overrides) => Some(overrides)
      case _ => clientContext.userRoles.map(_.toSet)
    }

    val recipient = FSRecipient(
      userId = Some(userId),
      userRoles = userRoles,
      deviceId = clientContext.deviceId,
      guestId = clientContext.guestId,
      languageCode = clientContext.languageCode,
      countryCode = clientContext.countryCode,
      userAgent = clientContext.userAgent.flatMap(UserAgent(_)),
      isVerified = None,
      isTwoffice = None,
      tooClient = None,
      highWaterMark = None
    )

    recipient.withCustomFields(
      (ParamsBuilder.ProductCustomField, product.toString),
      (ParamsBuilder.UserStateCustomField, userState.toString)
    )
  }
}

object ParamsBuilder {
  private val ProductCustomField = "product_id"
  private val UserStateCustomField = "user_state"
}
package com.twitter.cr_mixer.blender

import com.twitter.cr_mixer.model.BlendedCandidate
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.util.InterleaveUtil
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
case class InterleaveBlender @Inject() (globalStats: StatsReceiver) {

  private val name: String = this.getClass.getCanonicalName
  private val stats: StatsReceiver = globalStats.scope(name)

  /**
   * Interleaves candidates, by taking 1 candidate from each Seq[Seq[InitialCandidate]] in sequence,
   * until we run out of candidates.
   */
  def blend(
    inputCandidates: Seq[Seq[InitialCandidate]],
  ): Future[Seq[BlendedCandidate]] = {

    val interleavedCandidates = InterleaveUtil.interleave(inputCandidates)

    stats.stat("candidates").add(interleavedCandidates.size)

    val blendedCandidates = BlendedCandidatesBuilder.build(inputCandidates, interleavedCandidates)
    Future.value(blendedCandidates)
  }

}
package com.twitter.cr_mixer.blender

import com.twitter.cr_mixer.model.BlendedCandidate
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.param.BlenderParams
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.timelines.configapi.Params
import com.twitter.util.Future
import com.twitter.util.Time
import javax.inject.Inject

case class ContentSignalBlender @Inject() (globalStats: StatsReceiver) {

  private val name: String = this.getClass.getCanonicalName
  private val stats: StatsReceiver = globalStats.scope(name)

  /**
   *  Exposes multiple types of sorting relying only on Content Based signals
   *  Candidate Recency, Random, FavoriteCount and finally Standardized, which standardizes the scores
   *  that come from the active SimilarityEngine and then sort on the standardized scores.
   */
  def blend(
    params: Params,
    inputCandidates: Seq[Seq[InitialCandidate]],
  ): Future[Seq[BlendedCandidate]] = {
    // Filter out empty candidate sequence
    val candidates = inputCandidates.filter(_.nonEmpty)
    val sortedCandidates = params(BlenderParams.ContentBlenderTypeSortingAlgorithmParam) match {
      case BlenderParams.ContentBasedSortingAlgorithmEnum.CandidateRecency =>
        candidates.flatten.sortBy(c => getSnowflakeTimeStamp(c.tweetId)).reverse
      case BlenderParams.ContentBasedSortingAlgorithmEnum.RandomSorting =>
        candidates.flatten.sortBy(_ => scala.util.Random.nextDouble())
      case BlenderParams.ContentBasedSortingAlgorithmEnum.FavoriteCount =>
        candidates.flatten.sortBy(-_.tweetInfo.favCount)
      case BlenderParams.ContentBasedSortingAlgorithmEnum.SimilarityToSignalSorting =>
        standardizeAndSortByScore(flattenAndGroupByEngineTypeOrFirstContribEngine(candidates))
      case _ =>
        candidates.flatten.sortBy(-_.tweetInfo.favCount)
    }

    stats.stat("candidates").add(sortedCandidates.size)

    val blendedCandidates =
      BlendedCandidatesBuilder.build(inputCandidates, removeDuplicates(sortedCandidates))
    Future.value(blendedCandidates)
  }

  private def removeDuplicates(candidates: Seq[InitialCandidate]): Seq[InitialCandidate] = {
    val seen = collection.mutable.Set.empty[Long]
    candidates.filter { c =>
      if (seen.contains(c.tweetId)) {
        false
      } else {
        seen += c.tweetId
        true
      }
    }
  }

  private def groupByEngineTypeOrFirstContribEngine(
    candidates: Seq[InitialCandidate]
  ): Map[SimilarityEngineType, Seq[InitialCandidate]] = {
    val grouped = candidates.groupBy { candidate =>
      val contrib = candidate.candidateGenerationInfo.contributingSimilarityEngines
      if (contrib.nonEmpty) {
        contrib.head.similarityEngineType
      } else {
        candidate.candidateGenerationInfo.similarityEngineInfo.similarityEngineType
      }
    }
    grouped
  }

  private def flattenAndGroupByEngineTypeOrFirstContribEngine(
    candidates: Seq[Seq[InitialCandidate]]
  ): Seq[Seq[InitialCandidate]] = {
    val flat = candidates.flatten
    val grouped = groupByEngineTypeOrFirstContribEngine(flat)
    grouped.values.toSeq
  }

  private def standardizeAndSortByScore(
    candidates: Seq[Seq[InitialCandidate]]
  ): Seq[InitialCandidate] = {
    candidates
      .map { innerSeq =>
        val meanScore = innerSeq
          .map(c => c.candidateGenerationInfo.similarityEngineInfo.score.getOrElse(0.0))
          .sum / innerSeq.length
        val stdDev = scala.math
          .sqrt(
            innerSeq
              .map(c => c.candidateGenerationInfo.similarityEngineInfo.score.getOrElse(0.0))
              .map(a => a - meanScore)
              .map(a => a * a)
              .sum / innerSeq.length)
        innerSeq
          .map(c =>
            (
              c,
              c.candidateGenerationInfo.similarityEngineInfo.score
                .map { score =>
                  if (stdDev != 0) (score - meanScore) / stdDev
                  else 0.0
                }
                .getOrElse(0.0)))
      }.flatten.sortBy { case (_, standardizedScore) => -standardizedScore }
      .map { case (candidate, _) => candidate }
  }

  private def getSnowflakeTimeStamp(tweetId: Long): Time = {
    val isSnowflake = SnowflakeId.isSnowflakeId(tweetId)
    if (isSnowflake) {
      SnowflakeId(tweetId).time
    } else {
      Time.fromMilliseconds(0L)
    }
  }
}
package com.twitter.cr_mixer.blender

import com.twitter.core_workflows.user_model.thriftscala.UserState
import com.twitter.cr_mixer.model.BlendedCandidate
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.param.BlenderParams
import com.twitter.cr_mixer.param.BlenderParams.BlendingAlgorithmEnum
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.timelines.configapi.Params
import com.twitter.util.Future
import com.twitter.util.Time
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
case class SwitchBlender @Inject() (
  defaultBlender: InterleaveBlender,
  sourceTypeBackFillBlender: SourceTypeBackFillBlender,
  adsBlender: AdsBlender,
  contentSignalBlender: ContentSignalBlender,
  globalStats: StatsReceiver) {

  private val stats = globalStats.scope(this.getClass.getCanonicalName)

  def blend(
    params: Params,
    userState: UserState,
    inputCandidates: Seq[Seq[InitialCandidate]],
  ): Future[Seq[BlendedCandidate]] = {
    // Take out empty seq
    val nonEmptyCandidates = inputCandidates.collect {
      case candidates if candidates.nonEmpty =>
        candidates
    }
    stats.stat("num_of_sequences").add(inputCandidates.size)

    // Sort the seqs in an order
    val innerSignalSorting = params(BlenderParams.SignalTypeSortingAlgorithmParam) match {
      case BlenderParams.ContentBasedSortingAlgorithmEnum.SourceSignalRecency =>
        SwitchBlender.TimestampOrder
      case BlenderParams.ContentBasedSortingAlgorithmEnum.RandomSorting => SwitchBlender.RandomOrder
      case _ => SwitchBlender.TimestampOrder
    }

    val candidatesToBlend = nonEmptyCandidates.sortBy(_.head)(innerSignalSorting)
    // Blend based on specified blender rules
    params(BlenderParams.BlendingAlgorithmParam) match {
      case BlendingAlgorithmEnum.RoundRobin =>
        defaultBlender.blend(candidatesToBlend)
      case BlendingAlgorithmEnum.SourceTypeBackFill =>
        sourceTypeBackFillBlender.blend(params, candidatesToBlend)
      case BlendingAlgorithmEnum.SourceSignalSorting =>
        contentSignalBlender.blend(params, candidatesToBlend)
      case _ => defaultBlender.blend(candidatesToBlend)
    }
  }
}

object SwitchBlender {

  /**
   * Prefers candidates generated from sources with the latest timestamps.
   * The newer the source signal, the higher a candidate ranks.
   * This ordering biases against consumer-based candidates because their timestamp defaults to 0
   *
   * Within a Seq[Seq[Candidate]], all candidates within a inner Seq
   * are guaranteed to have the same sourceInfo because they are grouped by (sourceInfo, SE model).
   * Hence, we can pick .headOption to represent the whole list when filtering by the internalId of the sourceInfoOpt.
   * But of course the similarityEngine score in a CGInfo could be different.
   */
  val TimestampOrder: Ordering[InitialCandidate] =
    math.Ordering
      .by[InitialCandidate, Time](
        _.candidateGenerationInfo.sourceInfoOpt
          .flatMap(_.sourceEventTime)
          .getOrElse(Time.fromMilliseconds(0L)))
      .reverse

  private val RandomOrder: Ordering[InitialCandidate] =
    Ordering.by[InitialCandidate, Double](_ => scala.util.Random.nextDouble())
}
package com.twitter.cr_mixer
package featureswitch

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.abdecider.LoggingABDecider
import com.twitter.abdecider.Recipient
import com.twitter.abdecider.Bucket
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.util.Local
import scala.collection.concurrent.{Map => ConcurrentMap}

/**
 * Wraps a LoggingABDecider, so all impressed buckets are recorded to a 'LocalContext' on a given request.
 *
 * Contexts (https://twitter.github.io/finagle/guide/Contexts.html) are Finagle's mechanism for
 * storing state/variables without having to pass these variables all around the request.
 *
 * In order for this class to be used the [[SetImpressedBucketsLocalContextFilter]] must be applied
 * at the beginning of the request, to initialize a concurrent map used to store impressed buckets.
 *
 * Whenever we get an a/b impression, the bucket information is logged to the concurrent hashmap.
 */
case class CrMixerLoggingABDecider(
  loggingAbDecider: LoggingABDecider,
  statsReceiver: StatsReceiver)
    extends LoggingABDecider {

  private val scopedStatsReceiver = statsReceiver.scope("cr_logging_ab_decider")

  override def impression(
    experimentName: String,
    recipient: Recipient
  ): Option[Bucket] = {

    StatsUtil.trackNonFutureBlockStats(scopedStatsReceiver.scope("log_impression")) {
      val maybeBuckets = loggingAbDecider.impression(experimentName, recipient)
      maybeBuckets.foreach { b =>
        scopedStatsReceiver.counter("impressions").incr()
        CrMixerImpressedBuckets.recordImpressedBucket(b)
      }
      maybeBuckets
    }
  }

  override def track(
    experimentName: String,
    eventName: String,
    recipient: Recipient
  ): Unit = {
    loggingAbDecider.track(experimentName, eventName, recipient)
  }

  override def bucket(
    experimentName: String,
    recipient: Recipient
  ): Option[Bucket] = {
    loggingAbDecider.bucket(experimentName, recipient)
  }

  override def experiments: Seq[String] = loggingAbDecider.experiments

  override def experiment(experimentName: String) =
    loggingAbDecider.experiment(experimentName)
}

object CrMixerImpressedBuckets {
  private[featureswitch] val localImpressedBucketsMap = new Local[ConcurrentMap[Bucket, Boolean]]

  /**
   * Gets all impressed buckets for this request.
   **/
  def getAllImpressedBuckets: Option[List[Bucket]] = {
    localImpressedBucketsMap.apply().map(_.map { case (k, _) => k }.toList)
  }

  private[featureswitch] def recordImpressedBucket(bucket: Bucket) = {
    localImpressedBucketsMap().foreach { m => m += bucket -> true }
  }
}
package com.twitter.cr_mixer.scribe

/**
 * Categories define scribe categories used in cr-mixer service.
 */
object ScribeCategories {
  lazy val AllCategories =
    List(AbDecider, TopLevelApiDdgMetrics, TweetsRecs)

  /**
   * AbDecider represents scribe logs for experiments
   */
  lazy val AbDecider: ScribeCategory = ScribeCategory(
    "abdecider_scribe",
    "client_event"
  )

  /**
   * Top-Level Client event scribe logs, to record changes in system metrics (e.g. latency,
   * candidates returned, empty rate ) per experiment bucket, and store them in DDG metric group
   */
  lazy val TopLevelApiDdgMetrics: ScribeCategory = ScribeCategory(
    "top_level_api_ddg_metrics_scribe",
    "client_event"
  )

  lazy val TweetsRecs: ScribeCategory = ScribeCategory(
    "get_tweets_recommendations_scribe",
    "cr_mixer_get_tweets_recommendations"
  )

  lazy val VITTweetsRecs: ScribeCategory = ScribeCategory(
    "get_vit_tweets_recommendations_scribe",
    "cr_mixer_get_vit_tweets_recommendations"
  )

  lazy val RelatedTweets: ScribeCategory = ScribeCategory(
    "get_related_tweets_scribe",
    "cr_mixer_get_related_tweets"
  )

  lazy val UtegTweets: ScribeCategory = ScribeCategory(
    "get_uteg_tweets_scribe",
    "cr_mixer_get_uteg_tweets"
  )

  lazy val AdsRecommendations: ScribeCategory = ScribeCategory(
    "get_ads_recommendations_scribe",
    "cr_mixer_get_ads_recommendations"
  )
}

/**
 * Category represents each scribe log data.
 *
 * @param loggerFactoryNode loggerFactory node name in cr-mixer associated with this scribe category
 * @param scribeCategory    scribe category name (globally unique at Twitter)
 */
case class ScribeCategory(
  loggerFactoryNode: String,
  scribeCategory: String) {
  def getProdLoggerFactoryNode: String = loggerFactoryNode
  def getStagingLoggerFactoryNode: String = "staging_" + loggerFactoryNode
}
package com.twitter.cr_mixer.service

import com.twitter.product_mixer.core.functional_component.common.alert.Destination
import com.twitter.product_mixer.core.functional_component.common.alert.NotificationGroup

/**
 * Notifications (email, pagerduty, etc) can be specific per-alert but it is common for multiple
 * products to share notification configuration.
 *
 * Our configuration uses only email notifications because SampleMixer is a demonstration service
 * with neither internal nor customer-facing users. You will likely want to use a PagerDuty
 * destination instead. For example:
 * {{{
 *   critical = Destination(pagerDutyKey = Some("your-pagerduty-key"))
 * }}}
 *
 *
 * For more information about how to get a PagerDuty key, see:
 * https://docbird.twitter.biz/mon/how-to-guides.html?highlight=notificationgroup#set-up-email-pagerduty-and-slack-notifications
 */
object CrMixerAlertNotificationConfig {
  val DefaultNotificationGroup: NotificationGroup = NotificationGroup(
    warn = Destination(emails = Seq("no-reply@twitter.com")),
    critical = Destination(emails = Seq("no-reply@twitter.com"))
  )
}
package com.twitter.cr_mixer.model

import com.twitter.core_workflows.user_model.thriftscala.UserState
import com.twitter.cr_mixer.thriftscala.Product
import com.twitter.product_mixer.core.thriftscala.ClientContext
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.simclusters_v2.thriftscala.TopicId
import com.twitter.timelines.configapi.Params

sealed trait CandidateGeneratorQuery {
  val product: Product
  val maxNumResults: Int
  val impressedTweetList: Set[TweetId]
  val params: Params
  val requestUUID: Long
}

sealed trait HasUserId {
  val userId: UserId
}

case class CrCandidateGeneratorQuery(
  userId: UserId,
  product: Product,
  userState: UserState,
  maxNumResults: Int,
  impressedTweetList: Set[TweetId],
  params: Params,
  requestUUID: Long,
  languageCode: Option[String] = None)
    extends CandidateGeneratorQuery
    with HasUserId

case class UtegTweetCandidateGeneratorQuery(
  userId: UserId,
  product: Product,
  userState: UserState,
  maxNumResults: Int,
  impressedTweetList: Set[TweetId],
  params: Params,
  requestUUID: Long)
    extends CandidateGeneratorQuery
    with HasUserId

case class RelatedTweetCandidateGeneratorQuery(
  internalId: InternalId,
  clientContext: ClientContext, // To scribe LogIn/LogOut requests
  product: Product,
  maxNumResults: Int,
  impressedTweetList: Set[TweetId],
  params: Params,
  requestUUID: Long)
    extends CandidateGeneratorQuery

case class RelatedVideoTweetCandidateGeneratorQuery(
  internalId: InternalId,
  clientContext: ClientContext, // To scribe LogIn/LogOut requests
  product: Product,
  maxNumResults: Int,
  impressedTweetList: Set[TweetId],
  params: Params,
  requestUUID: Long)
    extends CandidateGeneratorQuery

case class FrsTweetCandidateGeneratorQuery(
  userId: UserId,
  product: Product,
  maxNumResults: Int,
  impressedUserList: Set[UserId],
  impressedTweetList: Set[TweetId],
  params: Params,
  languageCodeOpt: Option[String] = None,
  countryCodeOpt: Option[String] = None,
  requestUUID: Long)
    extends CandidateGeneratorQuery

case class AdsCandidateGeneratorQuery(
  userId: UserId,
  product: Product,
  userState: UserState,
  maxNumResults: Int,
  params: Params,
  requestUUID: Long)

case class TopicTweetCandidateGeneratorQuery(
  userId: UserId,
  topicIds: Set[TopicId],
  product: Product,
  maxNumResults: Int,
  impressedTweetList: Set[TweetId],
  params: Params,
  requestUUID: Long,
  isVideoOnly: Boolean)
    extends CandidateGeneratorQuery
package com.twitter.cr_mixer.util

import com.twitter.cr_mixer.model.Candidate
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.param.BlenderParams.BlendGroupingMethodEnum
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.simclusters_v2.thriftscala.InternalId

object CountWeightedInterleaveUtil {

  /**
   * Grouping key for interleaving candidates
   *
   * @param sourceInfoOpt optional SourceInfo, containing the source information
   * @param similarityEngineTypeOpt optional SimilarityEngineType, containing similarity engine
   *                                information
   * @param modelIdOpt optional modelId, containing the model ID
   * @param authorIdOpt optional authorId, containing the tweet author ID
   * @param groupIdOpt optional groupId, containing the ID corresponding to the blending group
   */
  case class GroupingKey(
    sourceInfoOpt: Option[SourceInfo],
    similarityEngineTypeOpt: Option[SimilarityEngineType],
    modelIdOpt: Option[String],
    authorIdOpt: Option[Long],
    groupIdOpt: Option[Int])

  /**
   * Converts candidates to grouping key based upon the feature that we interleave with.
   */
  def toGroupingKey[CandidateType <: Candidate](
    candidate: CandidateType,
    interleaveFeature: Option[BlendGroupingMethodEnum.Value],
    groupId: Option[Int],
  ): GroupingKey = {
    val grouping: GroupingKey = candidate match {
      case c: RankedCandidate =>
        interleaveFeature.getOrElse(BlendGroupingMethodEnum.SourceKeyDefault) match {
          case BlendGroupingMethodEnum.SourceKeyDefault =>
            GroupingKey(
              sourceInfoOpt = c.reasonChosen.sourceInfoOpt,
              similarityEngineTypeOpt =
                Some(c.reasonChosen.similarityEngineInfo.similarityEngineType),
              modelIdOpt = c.reasonChosen.similarityEngineInfo.modelId,
              authorIdOpt = None,
              groupIdOpt = groupId
            )
          // Some candidate sources don't have a sourceType, so it defaults to similarityEngine
          case BlendGroupingMethodEnum.SourceTypeSimilarityEngine =>
            val sourceInfoOpt = c.reasonChosen.sourceInfoOpt.map(_.sourceType).map { sourceType =>
              SourceInfo(
                sourceType = sourceType,
                internalId = InternalId.UserId(0),
                sourceEventTime = None)
            }
            GroupingKey(
              sourceInfoOpt = sourceInfoOpt,
              similarityEngineTypeOpt =
                Some(c.reasonChosen.similarityEngineInfo.similarityEngineType),
              modelIdOpt = c.reasonChosen.similarityEngineInfo.modelId,
              authorIdOpt = None,
              groupIdOpt = groupId
            )
          case BlendGroupingMethodEnum.AuthorId =>
            GroupingKey(
              sourceInfoOpt = None,
              similarityEngineTypeOpt = None,
              modelIdOpt = None,
              authorIdOpt = Some(c.tweetInfo.authorId),
              groupIdOpt = groupId
            )
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported interleave feature: $interleaveFeature")
        }
      case _ =>
        GroupingKey(
          sourceInfoOpt = None,
          similarityEngineTypeOpt = None,
          modelIdOpt = None,
          authorIdOpt = None,
          groupIdOpt = groupId
        )
    }
    grouping
  }

  /**
   * Rather than manually calculating and maintaining the weights to rank with, we instead
   * calculate the weights on the fly, based upon the frequencies of the candidates within each
   * group. To ensure that diversity of the feature is maintained, we additionally employ a
   * 'shrinkage' parameter which enforces more diversity by moving the weights closer to uniformity.
   * More details are available at go/weighted-interleave.
   *
   * @param candidateSeqKeyByFeature candidate to key.
   * @param rankerWeightShrinkage value between [0, 1] with 1 being complete uniformity.
   * @return Interleaving weights keyed by feature.
   */
  private def calculateWeightsKeyByFeature[CandidateType <: Candidate](
    candidateSeqKeyByFeature: Map[GroupingKey, Seq[CandidateType]],
    rankerWeightShrinkage: Double
  ): Map[GroupingKey, Double] = {
    val maxNumberCandidates: Double = candidateSeqKeyByFeature.values
      .map { candidates =>
        candidates.size
      }.max.toDouble
    candidateSeqKeyByFeature.map {
      case (featureKey: GroupingKey, candidateSeq: Seq[CandidateType]) =>
        val observedWeight: Double = candidateSeq.size.toDouble / maxNumberCandidates
        // How much to shrink empirical estimates to 1 (Default is to make all weights 1).
        val finalWeight =
          (1.0 - rankerWeightShrinkage) * observedWeight + rankerWeightShrinkage * 1.0
        featureKey -> finalWeight
    }
  }

  /**
   * Builds out the groups and weights for weighted interleaving of the candidates.
   * More details are available at go/weighted-interleave.
   *
   * @param rankedCandidateSeq candidates to interleave.
   * @param rankerWeightShrinkage value between [0, 1] with 1 being complete uniformity.
   * @return Candidates grouped by feature key and with calculated interleaving weights.
   */
  def buildRankedCandidatesWithWeightKeyByFeature(
    rankedCandidateSeq: Seq[RankedCandidate],
    rankerWeightShrinkage: Double,
    interleaveFeature: BlendGroupingMethodEnum.Value
  ): Seq[(Seq[RankedCandidate], Double)] = {
    // To accommodate the re-grouping in InterleaveRanker
    // In InterleaveBlender, we have already abandoned the grouping keys, and use Seq[Seq[]] to do interleave
    // Since that we build the candidateSeq with groupingKey, we can guarantee there is no empty candidateSeq
    val candidateSeqKeyByFeature: Map[GroupingKey, Seq[RankedCandidate]] =
      rankedCandidateSeq.groupBy { candidate: RankedCandidate =>
        toGroupingKey(candidate, Some(interleaveFeature), None)
      }

    // These weights [0, 1] are used to do weighted interleaving
    // The default value of 1.0 ensures the group is always sampled.
    val candidateWeightsKeyByFeature: Map[GroupingKey, Double] =
      calculateWeightsKeyByFeature(candidateSeqKeyByFeature, rankerWeightShrinkage)

    candidateSeqKeyByFeature.map {
      case (groupingKey: GroupingKey, candidateSeq: Seq[RankedCandidate]) =>
        Tuple2(
          candidateSeq.sortBy(-_.predictionScore),
          candidateWeightsKeyByFeature.getOrElse(groupingKey, 1.0))
    }.toSeq
  }

  /**
   * Takes current grouping (as implied by the outer Seq) and computes blending weights.
   *
   * @param initialCandidatesSeqSeq grouped candidates to interleave.
   * @param rankerWeightShrinkage value between [0, 1] with 1 being complete uniformity.
   * @return Grouped candidates with calculated interleaving weights.
   */
  def buildInitialCandidatesWithWeightKeyByFeature(
    initialCandidatesSeqSeq: Seq[Seq[InitialCandidate]],
    rankerWeightShrinkage: Double,
  ): Seq[(Seq[InitialCandidate], Double)] = {
    val candidateSeqKeyByFeature: Map[GroupingKey, Seq[InitialCandidate]] =
      initialCandidatesSeqSeq.zipWithIndex.map(_.swap).toMap.map {
        case (groupId: Int, initialCandidatesSeq: Seq[InitialCandidate]) =>
          toGroupingKey(initialCandidatesSeq.head, None, Some(groupId)) -> initialCandidatesSeq
      }

    // These weights [0, 1] are used to do weighted interleaving
    // The default value of 1.0 ensures the group is always sampled.
    val candidateWeightsKeyByFeature =
      calculateWeightsKeyByFeature(candidateSeqKeyByFeature, rankerWeightShrinkage)

    candidateSeqKeyByFeature.map {
      case (groupingKey: GroupingKey, candidateSeq: Seq[InitialCandidate]) =>
        Tuple2(candidateSeq, candidateWeightsKeyByFeature.getOrElse(groupingKey, 1.0))
    }.toSeq
  }
}
