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
