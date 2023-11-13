package com.twitter.cr_mixer.filter

import com.twitter.cr_mixer.model.CandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.param.UtegTweetGlobalParams
import com.twitter.util.Future

import javax.inject.Inject
import javax.inject.Singleton

/***
 * Filters candidates that are retweets
 */
@Singleton
case class RetweetFilter @Inject() () extends FilterBase {
  override def name: String = this.getClass.getCanonicalName
  override type ConfigType = Boolean

  override def filter(
    candidates: Seq[Seq[InitialCandidate]],
    config: ConfigType
  ): Future[Seq[Seq[InitialCandidate]]] = {
    if (config) {
      Future.value(
        candidates.map { candidateSeq =>
          candidateSeq.filterNot { candidate =>
            candidate.tweetInfo.isRetweet.getOrElse(false)
          }
        }
      )
    } else {
      Future.value(candidates)
    }
  }

  override def requestToConfig[CGQueryType <: CandidateGeneratorQuery](
    query: CGQueryType
  ): ConfigType = {
    query.params(UtegTweetGlobalParams.EnableRetweetFilterParam)
  }
}
package com.twitter.cr_mixer.filter

import com.twitter.cr_mixer.model.CandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class PreRankFilterRunner @Inject() (
  impressedTweetListFilter: ImpressedTweetlistFilter,
  tweetAgeFilter: TweetAgeFilter,
  videoTweetFilter: VideoTweetFilter,
  tweetReplyFilter: ReplyFilter,
  globalStats: StatsReceiver) {

  private val scopedStats = globalStats.scope(this.getClass.getCanonicalName)

  /***
   * The order of the filters does not matter as long as we do not apply .take(N) truncation
   * across all filters. In other words, it is fine that we first do tweetAgeFilter, and then
   * we do impressedTweetListFilter, or the other way around.
   * Same idea applies to the signal based filter - it is ok that we apply signal based filters
   * before impressedTweetListFilter.
   *
   * We move all signal based filters before tweetAgeFilter and impressedTweetListFilter
   * as a set of early filters.
   */
  val orderedFilters = Seq(
    tweetAgeFilter,
    impressedTweetListFilter,
    videoTweetFilter,
    tweetReplyFilter
  )

  def runSequentialFilters[CGQueryType <: CandidateGeneratorQuery](
    request: CGQueryType,
    candidates: Seq[Seq[InitialCandidate]],
  ): Future[Seq[Seq[InitialCandidate]]] = {
    PreRankFilterRunner.runSequentialFilters(
      request,
      candidates,
      orderedFilters,
      scopedStats
    )
  }

}

object PreRankFilterRunner {
  private def recordCandidateStatsBeforeFilter(
    candidates: Seq[Seq[InitialCandidate]],
    statsReceiver: StatsReceiver
  ): Unit = {
    statsReceiver
      .counter("empty_sources", "before").incr(
        candidates.count { _.isEmpty }
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
        candidates.count { _.isEmpty }
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
package com.twitter.cr_mixer.source_signal

import com.twitter.core_workflows.user_model.thriftscala.UserState
import com.twitter.cr_mixer.model.GraphSourceInfo
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.source_signal.SourceFetcher.FetcherQuery
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.cr_mixer.thriftscala.{Product => TProduct}
import com.twitter.simclusters_v2.common.UserId
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
case class SourceInfoRouter @Inject() (
  ussSourceSignalFetcher: UssSourceSignalFetcher,
  frsSourceSignalFetcher: FrsSourceSignalFetcher,
  frsSourceGraphFetcher: FrsSourceGraphFetcher,
  realGraphOonSourceGraphFetcher: RealGraphOonSourceGraphFetcher,
  realGraphInSourceGraphFetcher: RealGraphInSourceGraphFetcher,
) {

  def get(
    userId: UserId,
    product: TProduct,
    userState: UserState,
    params: configapi.Params
  ): Future[(Set[SourceInfo], Map[String, Option[GraphSourceInfo]])] = {

    val fetcherQuery = FetcherQuery(userId, product, userState, params)
    Future.join(
      getSourceSignals(fetcherQuery),
      getSourceGraphs(fetcherQuery)
    )
  }

  private def getSourceSignals(
    fetcherQuery: FetcherQuery
  ): Future[Set[SourceInfo]] = {
    Future
      .join(
        ussSourceSignalFetcher.get(fetcherQuery),
        frsSourceSignalFetcher.get(fetcherQuery)).map {
        case (ussSignalsOpt, frsSignalsOpt) =>
          (ussSignalsOpt.getOrElse(Seq.empty) ++ frsSignalsOpt.getOrElse(Seq.empty)).toSet
      }
  }

  private def getSourceGraphs(
    fetcherQuery: FetcherQuery
  ): Future[Map[String, Option[GraphSourceInfo]]] = {

    Future
      .join(
        frsSourceGraphFetcher.get(fetcherQuery),
        realGraphOonSourceGraphFetcher.get(fetcherQuery),
        realGraphInSourceGraphFetcher.get(fetcherQuery)
      ).map {
        case (frsGraphOpt, realGraphOonGraphOpt, realGraphInGraphOpt) =>
          Map(
            SourceType.FollowRecommendation.name -> frsGraphOpt,
            SourceType.RealGraphOon.name -> realGraphOonGraphOpt,
            SourceType.RealGraphIn.name -> realGraphInGraphOpt,
          )
      }
  }
}
package com.twitter.cr_mixer.filter

import com.twitter.cr_mixer.model.CandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.param.GlobalParams
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import javax.inject.Singleton
import com.twitter.conversions.DurationOps._

@Singleton
case class TweetAgeFilter() extends FilterBase {
  override val name: String = this.getClass.getCanonicalName

  override type ConfigType = Duration

  override def filter(
    candidates: Seq[Seq[InitialCandidate]],
    maxTweetAge: Duration
  ): Future[Seq[Seq[InitialCandidate]]] = {
    if (maxTweetAge >= 720.hours) {
      Future.value(candidates)
    } else {
      // Tweet IDs are approximately chronological (see http://go/snowflake),
      // so we are building the earliest tweet id once,
      // and pass that as the value to filter candidates for each CandidateGenerationModel.
      val earliestTweetId = SnowflakeId.firstIdFor(Time.now - maxTweetAge)
      Future.value(candidates.map(_.filter(_.tweetId >= earliestTweetId)))
    }
  }

  override def requestToConfig[CGQueryType <: CandidateGeneratorQuery](
    query: CGQueryType
  ): Duration = {
    query.params(GlobalParams.MaxTweetAgeHoursParam)
  }
}
package com.twitter.cr_mixer.filter

import com.twitter.cr_mixer.model.CandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.util.Future

trait FilterBase {
  def name: String

  type ConfigType

  def filter(
    candidates: Seq[Seq[InitialCandidate]],
    config: ConfigType
  ): Future[Seq[Seq[InitialCandidate]]]

  /**
   * Build the config params here. passing in param() into the filter is strongly discouraged
   * because param() can be slow when called many times
   */
  def requestToConfig[CGQueryType <: CandidateGeneratorQuery](request: CGQueryType): ConfigType
}
package com.twitter.cr_mixer.filter
import com.twitter.cr_mixer.model.CrCandidateGeneratorQuery
import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
case class PostRankFilterRunner @Inject() (
  globalStats: StatsReceiver) {

  private val scopedStats = globalStats.scope(this.getClass.getCanonicalName)

  private val beforeCount = scopedStats.stat("candidate_count", "before")
  private val afterCount = scopedStats.stat("candidate_count", "after")

  def run(
    query: CrCandidateGeneratorQuery,
    candidates: Seq[RankedCandidate]
  ): Future[Seq[RankedCandidate]] = {

    beforeCount.add(candidates.size)

    Future(
      removeBadRecentNotificationCandidates(candidates)
    ).map { results =>
      afterCount.add(results.size)
      results
    }
  }

  /**
   * Remove "bad" quality candidates generated by recent notifications
   * A candidate is bad when it is generated by a single RecentNotification
   * SourceKey.
   * e.x:
   * tweetA {recent notification1} -> bad
   * tweetB {recent notification1 recent notification2} -> good
   *tweetC {recent notification1 recent follow1} -> bad
   * SD-19397
   */
  private[filter] def removeBadRecentNotificationCandidates(
    candidates: Seq[RankedCandidate]
  ): Seq[RankedCandidate] = {
    candidates.filterNot {
      isBadQualityRecentNotificationCandidate
    }
  }

  private def isBadQualityRecentNotificationCandidate(candidate: RankedCandidate): Boolean = {
    candidate.potentialReasons.size == 1 &&
    candidate.potentialReasons.head.sourceInfoOpt.nonEmpty &&
    candidate.potentialReasons.head.sourceInfoOpt.get.sourceType == SourceType.NotificationClick
  }

}
package com.twitter.cr_mixer.filter

import com.twitter.cr_mixer.model.CandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.util.Future

import javax.inject.Inject
import javax.inject.Singleton

/***
 * Filters candidates that are replies
 */
@Singleton
case class ReplyFilter @Inject() () extends FilterBase {
  override def name: String = this.getClass.getCanonicalName
  override type ConfigType = Boolean

  override def filter(
    candidates: Seq[Seq[InitialCandidate]],
    config: ConfigType
  ): Future[Seq[Seq[InitialCandidate]]] = {
    if (config) {
      Future.value(
        candidates.map { candidateSeq =>
          candidateSeq.filterNot { candidate =>
            candidate.tweetInfo.isReply.getOrElse(false)
          }
        }
      )
    } else {
      Future.value(candidates)
    }
  }

  override def requestToConfig[CGQueryType <: CandidateGeneratorQuery](
    query: CGQueryType
  ): ConfigType = {
    true
  }
}
package com.twitter.cr_mixer.filter

import com.twitter.contentrecommender.thriftscala.TweetInfo
import com.twitter.cr_mixer.model.CandidateGeneratorQuery
import com.twitter.cr_mixer.model.CrCandidateGeneratorQuery
import com.twitter.cr_mixer.model.HealthThreshold
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.util.Future
import javax.inject.Singleton

@Singleton
trait TweetInfoHealthFilterBase extends FilterBase {
  override def name: String = this.getClass.getCanonicalName
  override type ConfigType = HealthThreshold.Enum.Value
  def thresholdToPropertyMap: Map[HealthThreshold.Enum.Value, TweetInfo => Option[Boolean]]
  def getFilterParamFn: CandidateGeneratorQuery => HealthThreshold.Enum.Value

  override def filter(
    candidates: Seq[Seq[InitialCandidate]],
    config: HealthThreshold.Enum.Value
  ): Future[Seq[Seq[InitialCandidate]]] = {
    Future.value(candidates.map { seq =>
      seq.filter(p => thresholdToPropertyMap(config)(p.tweetInfo).getOrElse(true))
    })
  }

  /**
   * Build the config params here. passing in param() into the filter is strongly discouraged
   * because param() can be slow when called many times
   */
  override def requestToConfig[CGQueryType <: CandidateGeneratorQuery](
    query: CGQueryType
  ): HealthThreshold.Enum.Value = {
    query match {
      case q: CrCandidateGeneratorQuery => getFilterParamFn(q)
      case _ => HealthThreshold.Enum.Off
    }
  }
}
package com.twitter.cr_mixer.filter

import com.twitter.cr_mixer.filter.VideoTweetFilter.FilterConfig
import com.twitter.cr_mixer.model.CandidateGeneratorQuery
import com.twitter.cr_mixer.model.CrCandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.RelatedTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.RelatedVideoTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.param.VideoTweetFilterParams
import com.twitter.util.Future
import javax.inject.Singleton

@Singleton
case class VideoTweetFilter() extends FilterBase {
  override val name: String = this.getClass.getCanonicalName

  override type ConfigType = FilterConfig

  override def filter(
    candidates: Seq[Seq[InitialCandidate]],
    config: ConfigType
  ): Future[Seq[Seq[InitialCandidate]]] = {
    Future.value(candidates.map {
      _.flatMap {
        candidate =>
          if (!config.enableVideoTweetFilter) {
            Some(candidate)
          } else {
            // if hasVideo is true, hasImage, hasGif should be false
            val hasVideo = checkTweetInfoAttribute(candidate.tweetInfo.hasVideo)
            val isHighMediaResolution =
              checkTweetInfoAttribute(candidate.tweetInfo.isHighMediaResolution)
            val isQuoteTweet = checkTweetInfoAttribute(candidate.tweetInfo.isQuoteTweet)
            val isReply = checkTweetInfoAttribute(candidate.tweetInfo.isReply)
            val hasMultipleMedia = checkTweetInfoAttribute(candidate.tweetInfo.hasMultipleMedia)
            val hasUrl = checkTweetInfoAttribute(candidate.tweetInfo.hasUrl)

            if (hasVideo && isHighMediaResolution && !isQuoteTweet &&
              !isReply && !hasMultipleMedia && !hasUrl) {
              Some(candidate)
            } else {
              None
            }
          }
      }
    })
  }

  def checkTweetInfoAttribute(attributeOpt: => Option[Boolean]): Boolean = {
    if (attributeOpt.isDefined)
      attributeOpt.get
    else {
      // takes Quoted Tweet (TweetInfo.isQuoteTweet) as an example,
      // if the attributeOpt is None, we by default say it is not a quoted tweet
      // similarly, if TweetInfo.hasVideo is a None,
      // we say it does not have video.
      false
    }
  }

  override def requestToConfig[CGQueryType <: CandidateGeneratorQuery](
    query: CGQueryType
  ): FilterConfig = {
    val enableVideoTweetFilter = query match {
      case _: CrCandidateGeneratorQuery | _: RelatedTweetCandidateGeneratorQuery |
          _: RelatedVideoTweetCandidateGeneratorQuery =>
        query.params(VideoTweetFilterParams.EnableVideoTweetFilterParam)
      case _ => false // e.g., GetRelatedTweets()
    }
    FilterConfig(
      enableVideoTweetFilter = enableVideoTweetFilter
    )
  }
}

object VideoTweetFilter {
  // extend the filterConfig to add more flags if needed.
  // now they are hardcoded according to the prod setting
  case class FilterConfig(
    enableVideoTweetFilter: Boolean)
}
package com.twitter.cr_mixer.filter

import com.twitter.cr_mixer.model.CandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.param.UtegTweetGlobalParams
import com.twitter.util.Future

import javax.inject.Inject
import javax.inject.Singleton

/**
 * Remove unhealthy candidates
 * Currently Timeline Ranker applies a check on the following three scores:
 *  - toxicityScore
 *  - pBlockScore
 *  - pReportedTweetScore
 *
 * Where isPassTweetHealthFilterStrict checks two additions scores with the same threshold:
 *  - pSpammyTweetScore
 *  - spammyTweetContentScore
 *
 * We've verified that both filters behave very similarly.
 */
@Singleton
case class UtegHealthFilter @Inject() () extends FilterBase {
  override def name: String = this.getClass.getCanonicalName
  override type ConfigType = Boolean

  override def filter(
    candidates: Seq[Seq[InitialCandidate]],
    config: ConfigType
  ): Future[Seq[Seq[InitialCandidate]]] = {
    if (config) {
      Future.value(
        candidates.map { candidateSeq =>
          candidateSeq.filter { candidate =>
            candidate.tweetInfo.isPassTweetHealthFilterStrict.getOrElse(false)
          }
        }
      )
    } else {
      Future.value(candidates)
    }
  }

  override def requestToConfig[CGQueryType <: CandidateGeneratorQuery](
    query: CGQueryType
  ): ConfigType = {
    query.params(UtegTweetGlobalParams.EnableTLRHealthFilterParam)
  }
}
