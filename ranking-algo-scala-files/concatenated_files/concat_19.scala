package com.twitter.cr_mixer.util

import com.twitter.search.common.schema.earlybird.EarlybirdFieldConstants.EarlybirdFieldConstant
import com.twitter.search.queryparser.query.search.SearchOperator
import com.twitter.search.queryparser.query.search.SearchOperatorConstants
import com.twitter.search.queryparser.query.{Query => EbQuery}
import com.twitter.search.queryparser.query.Conjunction
import scala.collection.JavaConverters._
import com.twitter.search.earlybird.thriftscala.ThriftSearchResultMetadataOptions
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.search.queryparser.query.Query
import com.twitter.util.Duration
import com.twitter.search.common.query.thriftjava.thriftscala.CollectorTerminationParams

object EarlybirdSearchUtil {
  val EarlybirdClientId: String = "cr-mixer.prod"

  val Mentions: String = EarlybirdFieldConstant.MENTIONS_FACET
  val Hashtags: String = EarlybirdFieldConstant.HASHTAGS_FACET
  val FacetsToFetch: Seq[String] = Seq(Mentions, Hashtags)

  val MetadataOptions: ThriftSearchResultMetadataOptions = ThriftSearchResultMetadataOptions(
    getTweetUrls = true,
    getResultLocation = false,
    getLuceneScore = false,
    getInReplyToStatusId = true,
    getReferencedTweetAuthorId = true,
    getMediaBits = true,
    getAllFeatures = true,
    getFromUserId = true,
    returnSearchResultFeatures = true,
    // Set getExclusiveConversationAuthorId in order to retrieve Exclusive / SuperFollow tweets.
    getExclusiveConversationAuthorId = true
  )

  // Filter out retweets and replies
  val TweetTypesToExclude: Seq[String] =
    Seq(
      SearchOperatorConstants.NATIVE_RETWEETS,
      SearchOperatorConstants.REPLIES)

  def GetCollectorTerminationParams(
    maxNumHitsPerShard: Int,
    processingTimeout: Duration
  ): Option[CollectorTerminationParams] = {
    Some(
      CollectorTerminationParams(
        // maxHitsToProcess is used for early termination on each EB shard
        maxHitsToProcess = Some(maxNumHitsPerShard),
        timeoutMs = processingTimeout.inMilliseconds.toInt
      ))
  }

  /**
   * Get EarlybirdQuery
   * This function creates a EBQuery based on the search input
   */
  def GetEarlybirdQuery(
    beforeTweetIdExclusive: Option[TweetId],
    afterTweetIdExclusive: Option[TweetId],
    excludedTweetIds: Set[TweetId],
    filterOutRetweetsAndReplies: Boolean
  ): Option[EbQuery] =
    CreateConjunction(
      Seq(
        CreateRangeQuery(beforeTweetIdExclusive, afterTweetIdExclusive),
        CreateExcludedTweetIdsQuery(excludedTweetIds),
        CreateTweetTypesFilters(filterOutRetweetsAndReplies)
      ).flatten)

  def CreateRangeQuery(
    beforeTweetIdExclusive: Option[TweetId],
    afterTweetIdExclusive: Option[TweetId]
  ): Option[EbQuery] = {
    val beforeIdClause = beforeTweetIdExclusive.map { beforeId =>
      // MAX_ID is an inclusive value therefore we subtract 1 from beforeId.
      new SearchOperator(SearchOperator.Type.MAX_ID, (beforeId - 1).toString)
    }
    val afterIdClause = afterTweetIdExclusive.map { afterId =>
      new SearchOperator(SearchOperator.Type.SINCE_ID, afterId.toString)
    }
    CreateConjunction(Seq(beforeIdClause, afterIdClause).flatten)
  }

  def CreateTweetTypesFilters(filterOutRetweetsAndReplies: Boolean): Option[EbQuery] = {
    if (filterOutRetweetsAndReplies) {
      val tweetTypeFilters = TweetTypesToExclude.map { searchOperator =>
        new SearchOperator(SearchOperator.Type.EXCLUDE, searchOperator)
      }
      CreateConjunction(tweetTypeFilters)
    } else None
  }

  def CreateConjunction(clauses: Seq[EbQuery]): Option[EbQuery] = {
    clauses.size match {
      case 0 => None
      case 1 => Some(clauses.head)
      case _ => Some(new Conjunction(clauses.asJava))
    }
  }

  def CreateExcludedTweetIdsQuery(tweetIds: Set[TweetId]): Option[EbQuery] = {
    if (tweetIds.nonEmpty) {
      Some(
        new SearchOperator.Builder()
          .setType(SearchOperator.Type.NAMED_MULTI_TERM_DISJUNCTION)
          .addOperand(EarlybirdFieldConstant.ID_FIELD.getFieldName)
          .addOperand(EXCLUDE_TWEET_IDS)
          .setOccur(Query.Occur.MUST_NOT)
          .build())
    } else None
  }

  /**
   * Get NamedDisjunctions with excludedTweetIds
   */
  def GetNamedDisjunctions(excludedTweetIds: Set[TweetId]): Option[Map[String, Seq[Long]]] =
    if (excludedTweetIds.nonEmpty)
      createNamedDisjunctionsExcludedTweetIds(excludedTweetIds)
    else None

  val EXCLUDE_TWEET_IDS = "exclude_tweet_ids"
  private def createNamedDisjunctionsExcludedTweetIds(
    tweetIds: Set[TweetId]
  ): Option[Map[String, Seq[Long]]] = {
    if (tweetIds.nonEmpty) {
      Some(Map(EXCLUDE_TWEET_IDS -> tweetIds.toSeq))
    } else None
  }
}
package com.twitter.cr_mixer.util

import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.thriftscala.MetricTag
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.cr_mixer.thriftscala.SourceType

object MetricTagUtil {

  def buildMetricTags(candidate: RankedCandidate): Seq[MetricTag] = {
    val interestedInMetricTag = isFromInterestedIn(candidate)

    val cgInfoMetricTags = candidate.potentialReasons
      .flatMap { cgInfo =>
        val sourceMetricTag = cgInfo.sourceInfoOpt.flatMap { sourceInfo =>
          toMetricTagFromSource(sourceInfo.sourceType)
        }
        val similarityEngineTags = toMetricTagFromSimilarityEngine(
          cgInfo.similarityEngineInfo,
          cgInfo.contributingSimilarityEngines)

        val combinedMetricTag = cgInfo.sourceInfoOpt.flatMap { sourceInfo =>
          toMetricTagFromSourceAndSimilarityEngine(sourceInfo, cgInfo.similarityEngineInfo)
        }

        Seq(sourceMetricTag) ++ similarityEngineTags ++ Seq(combinedMetricTag)
      }.flatten.toSet
    (interestedInMetricTag ++ cgInfoMetricTags).toSeq
  }

  /***
   * match a sourceType to a metricTag
   */
  private def toMetricTagFromSource(sourceType: SourceType): Option[MetricTag] = {
    sourceType match {
      case SourceType.TweetFavorite => Some(MetricTag.TweetFavorite) // Personalized Topics in Home
      case SourceType.Retweet => Some(MetricTag.Retweet) // Personalized Topics in Home
      case SourceType.NotificationClick =>
        Some(MetricTag.PushOpenOrNtabClick) // Health Filter in MR
      case SourceType.OriginalTweet =>
        Some(MetricTag.OriginalTweet)
      case SourceType.Reply =>
        Some(MetricTag.Reply)
      case SourceType.TweetShare =>
        Some(MetricTag.TweetShare)
      case SourceType.UserFollow =>
        Some(MetricTag.UserFollow)
      case SourceType.UserRepeatedProfileVisit =>
        Some(MetricTag.UserRepeatedProfileVisit)
      case SourceType.TwiceUserId =>
        Some(MetricTag.TwiceUserId)
      case _ => None
    }
  }

  /***
   * If the SEInfo is built by a unified sim engine, we un-wrap the contributing sim engines.
   * If not, we log the sim engine as usual.
   * @param seInfo (CandidateGenerationInfo.similarityEngineInfo): SimilarityEngineInfo,
   * @param cseInfo (CandidateGenerationInfo.contributingSimilarityEngines): Seq[SimilarityEngineInfo]
   */
  private def toMetricTagFromSimilarityEngine(
    seInfo: SimilarityEngineInfo,
    cseInfo: Seq[SimilarityEngineInfo]
  ): Seq[Option[MetricTag]] = {
    seInfo.similarityEngineType match {
      case SimilarityEngineType.TweetBasedUnifiedSimilarityEngine => // un-wrap the unified sim engine
        cseInfo.map { contributingSimEngine =>
          toMetricTagFromSimilarityEngine(contributingSimEngine, Seq.empty)
        }.flatten
      case SimilarityEngineType.ProducerBasedUnifiedSimilarityEngine => // un-wrap the unified sim engine
        cseInfo.map { contributingSimEngine =>
          toMetricTagFromSimilarityEngine(contributingSimEngine, Seq.empty)
        }.flatten
      // SimClustersANN can either be called on its own, or be called under unified sim engine
      case SimilarityEngineType.SimClustersANN => // the old "UserInterestedIn" will be replaced by this. Also, OfflineTwice
        Seq(Some(MetricTag.SimClustersANN), seInfo.modelId.flatMap(toMetricTagFromModelId(_)))
      case SimilarityEngineType.ConsumerEmbeddingBasedTwHINANN =>
        Seq(Some(MetricTag.ConsumerEmbeddingBasedTwHINANN))
      case SimilarityEngineType.TwhinCollabFilter => Seq(Some(MetricTag.TwhinCollabFilter))
      // In the current implementation, TweetBasedUserTweetGraph/TweetBasedTwHINANN has a tag when
      // it's either a base SE or a contributing SE. But for now they only show up in contributing SE.
      case SimilarityEngineType.TweetBasedUserTweetGraph =>
        Seq(Some(MetricTag.TweetBasedUserTweetGraph))
      case SimilarityEngineType.TweetBasedTwHINANN =>
        Seq(Some(MetricTag.TweetBasedTwHINANN))
      case _ => Seq.empty
    }
  }

  /***
   * pass in a model id, and match it with the metric tag type.
   */
  private def toMetricTagFromModelId(
    modelId: String
  ): Option[MetricTag] = {

    val pushOpenBasedModelRegex = "(.*_Model20m145k2020_20220819)".r

    modelId match {
      case pushOpenBasedModelRegex(_*) =>
        Some(MetricTag.RequestHealthFilterPushOpenBasedTweetEmbedding)
      case _ => None
    }
  }

  private def toMetricTagFromSourceAndSimilarityEngine(
    sourceInfo: SourceInfo,
    seInfo: SimilarityEngineInfo
  ): Option[MetricTag] = {
    sourceInfo.sourceType match {
      case SourceType.Lookalike
          if seInfo.similarityEngineType == SimilarityEngineType.ConsumersBasedUserTweetGraph =>
        Some(MetricTag.LookalikeUTG)
      case _ => None
    }
  }

  /**
   * Special use case: used by Notifications team to generate the UserInterestedIn CRT push copy.
   *
   * if we have different types of InterestedIn (eg. UserInterestedIn, NextInterestedIn),
   * this if statement will have to be refactored to contain the real UserInterestedIn.
   * @return
   */
  private def isFromInterestedIn(candidate: RankedCandidate): Set[MetricTag] = {
    if (candidate.reasonChosen.sourceInfoOpt.isEmpty
      && candidate.reasonChosen.similarityEngineInfo.similarityEngineType == SimilarityEngineType.SimClustersANN) {
      Set(MetricTag.UserInterestedIn)
    } else Set.empty
  }

}
package com.twitter.cr_mixer.util

import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.cr_mixer.thriftscala.TweetRecommendation
import javax.inject.Inject
import com.twitter.finagle.stats.StatsReceiver
import javax.inject.Singleton
import com.twitter.relevance_platform.common.stats.BucketTimestampStats

@Singleton
class SignalTimestampStatsUtil @Inject() (statsReceiver: StatsReceiver) {
  import SignalTimestampStatsUtil._

  private val signalDelayAgePerDayStats =
    new BucketTimestampStats[TweetRecommendation](
      BucketTimestampStats.MillisecondsPerDay,
      _.latestSourceSignalTimestampInMillis.getOrElse(0),
      Some(SignalTimestampMaxDays))(
      statsReceiver.scope("signal_timestamp_per_day")
    ) // only stats past 90 days
  private val signalDelayAgePerHourStats =
    new BucketTimestampStats[TweetRecommendation](
      BucketTimestampStats.MillisecondsPerHour,
      _.latestSourceSignalTimestampInMillis.getOrElse(0),
      Some(SignalTimestampMaxHours))(
      statsReceiver.scope("signal_timestamp_per_hour")
    ) // only stats past 24 hours
  private val signalDelayAgePerMinStats =
    new BucketTimestampStats[TweetRecommendation](
      BucketTimestampStats.MillisecondsPerMinute,
      _.latestSourceSignalTimestampInMillis.getOrElse(0),
      Some(SignalTimestampMaxMins))(
      statsReceiver.scope("signal_timestamp_per_min")
    ) // only stats past 60 minutes

  def statsSignalTimestamp(
    tweets: Seq[TweetRecommendation],
  ): Seq[TweetRecommendation] = {
    signalDelayAgePerMinStats.count(tweets)
    signalDelayAgePerHourStats.count(tweets)
    signalDelayAgePerDayStats.count(tweets)
  }
}

object SignalTimestampStatsUtil {
  val SignalTimestampMaxMins = 60 // stats at most 60 mins
  val SignalTimestampMaxHours = 24 // stats at most 24 hours
  val SignalTimestampMaxDays = 90 // stats at most 90 days

  def buildLatestSourceSignalTimestamp(candidate: RankedCandidate): Option[Long] = {
    val timestampSeq = candidate.potentialReasons
      .collect {
        case CandidateGenerationInfo(Some(SourceInfo(sourceType, _, Some(sourceEventTime))), _, _)
            if sourceType == SourceType.TweetFavorite =>
          sourceEventTime.inMilliseconds
      }
    if (timestampSeq.nonEmpty) {
      Some(timestampSeq.max(Ordering.Long))
    } else {
      None
    }
  }
}
package com.twitter.cr_mixer.util

import com.twitter.cr_mixer.model.Candidate
import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.simclusters_v2.common.TweetId
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object InterleaveUtil {

  /**
   * Interleaves candidates by iteratively taking one candidate from the 1st Seq and adding it to the result.
   * Once we take a candidate from a Seq, we move this Seq to the end of the queue to process,
   * and remove the candidate from that Seq.
   *
   * We keep a mutable.Set[TweetId] buffer to ensure there are no duplicates.
   *
   * @param candidates candidates assumed to be sorted by eventTime (latest event comes first)
   * @return interleaved candidates
   */
  def interleave[CandidateType <: Candidate](
    candidates: Seq[Seq[CandidateType]]
  ): Seq[CandidateType] = {

    // copy candidates into a mutable map so this method is thread-safe
    val candidatesPerSequence = candidates.map { tweetCandidates =>
      mutable.Queue() ++= tweetCandidates
    }

    val seen = mutable.Set[TweetId]()

    val candidateSeqQueue = mutable.Queue() ++= candidatesPerSequence

    val result = ArrayBuffer[CandidateType]()

    while (candidateSeqQueue.nonEmpty) {
      val candidatesQueue = candidateSeqQueue.head

      if (candidatesQueue.nonEmpty) {
        val candidate = candidatesQueue.dequeue()
        val candidateTweetId = candidate.tweetId
        val seenCandidate = seen.contains(candidateTweetId)
        if (!seenCandidate) {
          result += candidate
          seen.add(candidate.tweetId)
          candidateSeqQueue.enqueue(
            candidateSeqQueue.dequeue()
          ) // move this Seq to end
        }
      } else {
        candidateSeqQueue.dequeue() //finished processing this Seq
      }
    }
    //convert result to immutable seq
    result.toList
  }

  /**
   * Interleaves candidates by iteratively
   * 1. Checking weight to see if enough accumulation has occurred to sample from
   * 2. If yes, taking one candidate from the the Seq and adding it to the result.
   * 3. Move this Seq to the end of the queue to process (and remove the candidate from that Seq if
   *    we sampled it from step 2).
   *
   * We keep count of the iterations to prevent infinite loops.
   * We keep a mutable.Set[TweetId] buffer to ensure there are no duplicates.
   *
   * @param candidatesAndWeight candidates assumed to be sorted by eventTime (latest event comes first),
   *                            along with sampling weights to help prioritize important groups.
   * @param maxWeightAdjustments Maximum number of iterations to account for weighting before
   *                             defaulting to uniform interleaving.
   * @return interleaved candidates
   */
  def weightedInterleave[CandidateType <: Candidate](
    candidatesAndWeight: Seq[(Seq[CandidateType], Double)],
    maxWeightAdjustments: Int = 0
  ): Seq[CandidateType] = {

    // Set to avoid numerical issues around 1.0
    val min_weight = 1 - 1e-30

    // copy candidates into a mutable map so this method is thread-safe
    // adds a counter to use towards sampling
    val candidatesAndWeightsPerSequence: Seq[
      (mutable.Queue[CandidateType], InterleaveWeights)
    ] =
      candidatesAndWeight.map { candidatesAndWeight =>
        (mutable.Queue() ++= candidatesAndWeight._1, InterleaveWeights(candidatesAndWeight._2, 0.0))
      }

    val seen: mutable.Set[TweetId] = mutable.Set[TweetId]()

    val candidateSeqQueue: mutable.Queue[(mutable.Queue[CandidateType], InterleaveWeights)] =
      mutable.Queue() ++= candidatesAndWeightsPerSequence

    val result: ArrayBuffer[CandidateType] = ArrayBuffer[CandidateType]()
    var number_iterations: Int = 0

    while (candidateSeqQueue.nonEmpty) {
      val (candidatesQueue, currentWeights) = candidateSeqQueue.head
      if (candidatesQueue.nonEmpty) {
        // Confirm weighting scheme
        currentWeights.summed_weight += currentWeights.weight
        number_iterations += 1
        if (currentWeights.summed_weight >= min_weight || number_iterations >= maxWeightAdjustments) {
          // If we sample, then adjust the counter
          currentWeights.summed_weight -= 1.0
          val candidate = candidatesQueue.dequeue()
          val candidateTweetId = candidate.tweetId
          val seenCandidate = seen.contains(candidateTweetId)
          if (!seenCandidate) {
            result += candidate
            seen.add(candidate.tweetId)
            candidateSeqQueue.enqueue(candidateSeqQueue.dequeue()) // move this Seq to end
          }
        } else {
          candidateSeqQueue.enqueue(candidateSeqQueue.dequeue()) // move this Seq to end
        }
      } else {
        candidateSeqQueue.dequeue() //finished processing this Seq
      }
    }
    //convert result to immutable seq
    result.toList
  }

  def buildCandidatesKeyByCGInfo(
    candidates: Seq[RankedCandidate],
  ): Seq[Seq[RankedCandidate]] = {
    // To accommodate the re-grouping in InterleaveRanker
    // In InterleaveBlender, we have already abandoned the grouping keys, and use Seq[Seq[]] to do interleave
    // Since that we build the candidateSeq with groupingKey, we can guarantee there is no empty candidateSeq
    val candidateSeqKeyByCG =
      candidates.groupBy(candidate => GroupingKey.toGroupingKey(candidate.reasonChosen))
    candidateSeqKeyByCG.map {
      case (groupingKey, candidateSeq) =>
        candidateSeq.sortBy(-_.predictionScore)
    }.toSeq
  }
}

case class GroupingKey(
  sourceInfoOpt: Option[SourceInfo],
  similarityEngineType: SimilarityEngineType,
  modelId: Option[String]) {}

object GroupingKey {
  def toGroupingKey(candidateGenerationInfo: CandidateGenerationInfo): GroupingKey = {
    GroupingKey(
      candidateGenerationInfo.sourceInfoOpt,
      candidateGenerationInfo.similarityEngineInfo.similarityEngineType,
      candidateGenerationInfo.similarityEngineInfo.modelId
    )
  }
}

case class InterleaveWeights(weight: Double, var summed_weight: Double)
package com.twitter.cr_mixer.util

import com.twitter.cr_mixer.model.CandidateGenerationInfo
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.thriftscala.CandidateGenerationKey
import com.twitter.cr_mixer.thriftscala.SimilarityEngine
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.util.Time

object CandidateGenerationKeyUtil {
  private val PlaceholderUserId = 0L // this default value will not be used

  private val DefaultSourceInfo: SourceInfo = SourceInfo(
    sourceType = SourceType.RequestUserId,
    sourceEventTime = None,
    internalId = InternalId.UserId(PlaceholderUserId)
  )

  def toThrift(
    candidateGenerationInfo: CandidateGenerationInfo,
    requestUserId: UserId
  ): CandidateGenerationKey = {
    CandidateGenerationKey(
      sourceType = candidateGenerationInfo.sourceInfoOpt.getOrElse(DefaultSourceInfo).sourceType,
      sourceEventTime = candidateGenerationInfo.sourceInfoOpt
        .getOrElse(DefaultSourceInfo).sourceEventTime.getOrElse(Time.fromMilliseconds(0L)).inMillis,
      id = candidateGenerationInfo.sourceInfoOpt
        .map(_.internalId).getOrElse(InternalId.UserId(requestUserId)),
      modelId = candidateGenerationInfo.similarityEngineInfo.modelId.getOrElse(""),
      similarityEngineType =
        Some(candidateGenerationInfo.similarityEngineInfo.similarityEngineType),
      contributingSimilarityEngine =
        Some(candidateGenerationInfo.contributingSimilarityEngines.map(se =>
          SimilarityEngine(se.similarityEngineType, se.modelId, se.score)))
    )
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
package com.twitter.cr_mixer.param

import com.twitter.cr_mixer.model.ModelConfig
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object TweetBasedTwHINParams {
  object ModelIdParam
      extends FSParam[String](
        name = "tweet_based_twhin_model_id",
        default = ModelConfig.TweetBasedTwHINRegularUpdateAll20221024,
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(ModelIdParam)

  lazy val config: BaseConfig = {
    val stringFSOverrides =
      FeatureSwitchOverrideUtil.getStringFSOverrides(
        ModelIdParam
      )

    BaseConfigBuilder()
      .set(stringFSOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
import com.twitter.simclusters_v2.common.ModelVersions
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.DurationConversion
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSEnumParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.HasDurationConversion
import com.twitter.timelines.configapi.Param
import com.twitter.util.Duration

/**
 * Instantiate Params that do not relate to a specific product.
 * The params in this file correspond to config repo file
 * [[https://sourcegraph.twitter.biz/config-git.twitter.biz/config/-/blob/features/cr-mixer/main/twistly_core.yml]]
 */
object GlobalParams {

  object MaxCandidatesPerRequestParam
      extends FSBoundedParam[Int](
        name = "twistly_core_max_candidates_per_request",
        default = 100,
        min = 0,
        max = 9000
      )

  object ModelVersionParam
      extends FSEnumParam[ModelVersions.Enum.type](
        name = "twistly_core_simclusters_model_version_id",
        default = ModelVersions.Enum.Model20M145K2020,
        enum = ModelVersions.Enum
      )

  object UnifiedMaxSourceKeyNum
      extends FSBoundedParam[Int](
        name = "twistly_core_unified_max_sourcekey_num",
        default = 15,
        min = 0,
        max = 100
      )

  object MaxCandidateNumPerSourceKeyParam
      extends FSBoundedParam[Int](
        name = "twistly_core_candidate_per_sourcekey_max_num",
        default = 200,
        min = 0,
        max = 1000
      )

  // 1 hours to 30 days
  object MaxTweetAgeHoursParam
      extends FSBoundedParam[Duration](
        name = "twistly_core_max_tweet_age_hours",
        default = 720.hours,
        min = 1.hours,
        max = 720.hours
      )
      with HasDurationConversion {

    override val durationConversion: DurationConversion = DurationConversion.FromHours
  }

  val AllParams: Seq[Param[_] with FSName] = Seq(
    MaxCandidatesPerRequestParam,
    UnifiedMaxSourceKeyNum,
    MaxCandidateNumPerSourceKeyParam,
    ModelVersionParam,
    MaxTweetAgeHoursParam
  )

  lazy val config: BaseConfig = {

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides()

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MaxCandidatesPerRequestParam,
      UnifiedMaxSourceKeyNum,
      MaxCandidateNumPerSourceKeyParam
    )

    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
      ModelVersionParam
    )

    val boundedDurationFSOverrides =
      FeatureSwitchOverrideUtil.getBoundedDurationFSOverrides(MaxTweetAgeHoursParam)

    val seqOverrides = FeatureSwitchOverrideUtil.getLongSeqFSOverrides()

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(intOverrides: _*)
      .set(boundedDurationFSOverrides: _*)
      .set(enumOverrides: _*)
      .set(seqOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object ProducerBasedUserTweetGraphParams {

  object MinCoOccurrenceParam
      extends FSBoundedParam[Int](
        name = "producer_based_user_tweet_graph_min_co_occurrence",
        default = 4,
        min = 0,
        max = 500
      )

  object MinScoreParam
      extends FSBoundedParam[Double](
        name = "producer_based_user_tweet_graph_min_score",
        default = 3.0,
        min = 0.0,
        max = 10.0
      )

  object MaxNumFollowersParam
      extends FSBoundedParam[Int](
        name = "producer_based_user_tweet_graph_max_num_followers",
        default = 500,
        min = 100,
        max = 1000
      )

  val AllParams: Seq[Param[_] with FSName] =
    Seq(MinCoOccurrenceParam, MaxNumFollowersParam, MinScoreParam)

  lazy val config: BaseConfig = {

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MinCoOccurrenceParam,
      MaxNumFollowersParam,
    )

    val doubleOverrides = FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(MinScoreParam)

    BaseConfigBuilder()
      .set(intOverrides: _*)
      .set(doubleOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.model

import com.twitter.simclusters_v2.common.TweetId

/***
 * Bind a tweetId with a raw score generated from one single Similarity Engine
 */
case class TweetWithScore(tweetId: TweetId, score: Double)
