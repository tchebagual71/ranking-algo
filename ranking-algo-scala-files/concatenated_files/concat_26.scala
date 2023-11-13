package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.google.inject.Singleton
import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.EarlybirdClientId
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.FacetsToFetch
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.GetCollectorTerminationParams
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.GetEarlybirdQuery
import com.twitter.cr_mixer.util.EarlybirdSearchUtil.MetadataOptions
import com.twitter.finagle.memcached.{Client => MemcachedClient}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.SeqLongInjection
import com.twitter.hashing.KeyHasher
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.inject.TwitterModule
import com.twitter.search.common.query.thriftjava.thriftscala.CollectorParams
import com.twitter.search.earlybird.thriftscala.EarlybirdRequest
import com.twitter.search.earlybird.thriftscala.EarlybirdResponseCode
import com.twitter.search.earlybird.thriftscala.EarlybirdService
import com.twitter.search.earlybird.thriftscala.ThriftSearchQuery
import com.twitter.search.earlybird.thriftscala.ThriftSearchRankingMode
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Duration
import com.twitter.util.Future
import javax.inject.Named

object EarlybirdRecencyBasedCandidateStoreModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.EarlybirdRecencyBasedWithoutRetweetsRepliesTweetsCache)
  def providesEarlybirdRecencyBasedWithoutRetweetsRepliesCandidateStore(
    statsReceiver: StatsReceiver,
    earlybirdSearchClient: EarlybirdService.MethodPerEndpoint,
    @Named(ModuleNames.EarlybirdTweetsCache) earlybirdRecencyBasedTweetsCache: MemcachedClient,
    timeoutConfig: TimeoutConfig
  ): ReadableStore[UserId, Seq[TweetId]] = {
    val stats = statsReceiver.scope("EarlybirdRecencyBasedWithoutRetweetsRepliesCandidateStore")
    val underlyingStore = new ReadableStore[UserId, Seq[TweetId]] {
      override def get(userId: UserId): Future[Option[Seq[TweetId]]] = {
        // Home based EB filters out retweets and replies
        val earlybirdRequest =
          buildEarlybirdRequest(
            userId,
            FilterOutRetweetsAndReplies,
            DefaultMaxNumTweetPerUser,
            timeoutConfig.earlybirdServerTimeout)
        getEarlybirdSearchResult(earlybirdSearchClient, earlybirdRequest, stats)
      }
    }
    ObservedMemcachedReadableStore.fromCacheClient(
      backingStore = underlyingStore,
      cacheClient = earlybirdRecencyBasedTweetsCache,
      ttl = MemcacheKeyTimeToLiveDuration,
      asyncUpdate = true
    )(
      valueInjection = SeqLongInjection,
      statsReceiver = statsReceiver.scope("earlybird_recency_based_tweets_home_memcache"),
      keyToString = { k =>
        f"uEBRBHM:${keyHasher.hashKey(k.toString.getBytes)}%X" // prefix = EarlyBirdRecencyBasedHoMe
      }
    )
  }

  @Provides
  @Singleton
  @Named(ModuleNames.EarlybirdRecencyBasedWithRetweetsRepliesTweetsCache)
  def providesEarlybirdRecencyBasedWithRetweetsRepliesCandidateStore(
    statsReceiver: StatsReceiver,
    earlybirdSearchClient: EarlybirdService.MethodPerEndpoint,
    @Named(ModuleNames.EarlybirdTweetsCache) earlybirdRecencyBasedTweetsCache: MemcachedClient,
    timeoutConfig: TimeoutConfig
  ): ReadableStore[UserId, Seq[TweetId]] = {
    val stats = statsReceiver.scope("EarlybirdRecencyBasedWithRetweetsRepliesCandidateStore")
    val underlyingStore = new ReadableStore[UserId, Seq[TweetId]] {
      override def get(userId: UserId): Future[Option[Seq[TweetId]]] = {
        val earlybirdRequest = buildEarlybirdRequest(
          userId,
          // Notifications based EB keeps retweets and replies
          NotFilterOutRetweetsAndReplies,
          DefaultMaxNumTweetPerUser,
          processingTimeout = timeoutConfig.earlybirdServerTimeout
        )
        getEarlybirdSearchResult(earlybirdSearchClient, earlybirdRequest, stats)
      }
    }
    ObservedMemcachedReadableStore.fromCacheClient(
      backingStore = underlyingStore,
      cacheClient = earlybirdRecencyBasedTweetsCache,
      ttl = MemcacheKeyTimeToLiveDuration,
      asyncUpdate = true
    )(
      valueInjection = SeqLongInjection,
      statsReceiver = statsReceiver.scope("earlybird_recency_based_tweets_notifications_memcache"),
      keyToString = { k =>
        f"uEBRBN:${keyHasher.hashKey(k.toString.getBytes)}%X" // prefix = EarlyBirdRecencyBasedNotifications
      }
    )
  }

  private val keyHasher: KeyHasher = KeyHasher.FNV1A_64

  /**
   * Note the DefaultMaxNumTweetPerUser is used to adjust the result size per cache entry.
   * If the value changes, it will increase the size of the memcache.
   */
  private val DefaultMaxNumTweetPerUser: Int = 100
  private val FilterOutRetweetsAndReplies = true
  private val NotFilterOutRetweetsAndReplies = false
  private val MemcacheKeyTimeToLiveDuration: Duration = Duration.fromMinutes(15)

  private def buildEarlybirdRequest(
    seedUserId: UserId,
    filterOutRetweetsAndReplies: Boolean,
    maxNumTweetsPerSeedUser: Int,
    processingTimeout: Duration
  ): EarlybirdRequest =
    EarlybirdRequest(
      searchQuery = getThriftSearchQuery(
        seedUserId = seedUserId,
        filterOutRetweetsAndReplies = filterOutRetweetsAndReplies,
        maxNumTweetsPerSeedUser = maxNumTweetsPerSeedUser,
        processingTimeout = processingTimeout
      ),
      clientId = Some(EarlybirdClientId),
      timeoutMs = processingTimeout.inMilliseconds.intValue(),
      getOlderResults = Some(false),
      adjustedProtectedRequestParams = None,
      adjustedFullArchiveRequestParams = None,
      getProtectedTweetsOnly = Some(false),
      skipVeryRecentTweets = true,
    )

  private def getThriftSearchQuery(
    seedUserId: UserId,
    filterOutRetweetsAndReplies: Boolean,
    maxNumTweetsPerSeedUser: Int,
    processingTimeout: Duration
  ): ThriftSearchQuery = ThriftSearchQuery(
    serializedQuery = GetEarlybirdQuery(
      None,
      None,
      Set.empty,
      filterOutRetweetsAndReplies
    ).map(_.serialize),
    fromUserIDFilter64 = Some(Seq(seedUserId)),
    numResults = maxNumTweetsPerSeedUser,
    rankingMode = ThriftSearchRankingMode.Recency,
    collectorParams = Some(
      CollectorParams(
        // numResultsToReturn defines how many results each EB shard will return to search root
        numResultsToReturn = maxNumTweetsPerSeedUser,
        // terminationParams.maxHitsToProcess is used for early terminating per shard results fetching.
        terminationParams =
          GetCollectorTerminationParams(maxNumTweetsPerSeedUser, processingTimeout)
      )),
    facetFieldNames = Some(FacetsToFetch),
    resultMetadataOptions = Some(MetadataOptions),
    searchStatusIds = None
  )

  private def getEarlybirdSearchResult(
    earlybirdSearchClient: EarlybirdService.MethodPerEndpoint,
    request: EarlybirdRequest,
    statsReceiver: StatsReceiver
  ): Future[Option[Seq[TweetId]]] = earlybirdSearchClient
    .search(request)
    .map { response =>
      response.responseCode match {
        case EarlybirdResponseCode.Success =>
          val earlybirdSearchResult =
            response.searchResults
              .map {
                _.results
                  .map(searchResult => searchResult.id)
              }
          statsReceiver.scope("result").stat("size").add(earlybirdSearchResult.size)
          earlybirdSearchResult
        case e =>
          statsReceiver.scope("failures").counter(e.getClass.getSimpleName).incr()
          Some(Seq.empty)
      }
    }

}
package com.twitter.cr_mixer.module

import com.google.inject.Provides
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.inject.TwitterModule
import com.twitter.recos.user_ad_graph.thriftscala.ConsumersBasedRelatedAdRequest
import com.twitter.recos.user_ad_graph.thriftscala.RelatedAdResponse
import com.twitter.recos.user_ad_graph.thriftscala.UserAdGraph
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import javax.inject.Named
import javax.inject.Singleton

object ConsumersBasedUserAdGraphStoreModule extends TwitterModule {

  @Provides
  @Singleton
  @Named(ModuleNames.ConsumerBasedUserAdGraphStore)
  def providesConsumerBasedUserAdGraphStore(
    userAdGraphService: UserAdGraph.MethodPerEndpoint
  ): ReadableStore[ConsumersBasedRelatedAdRequest, RelatedAdResponse] = {
    new ReadableStore[ConsumersBasedRelatedAdRequest, RelatedAdResponse] {
      override def get(
        k: ConsumersBasedRelatedAdRequest
      ): Future[Option[RelatedAdResponse]] = {
        userAdGraphService.consumersBasedRelatedAds(k).map(Some(_))
      }
    }
  }
}
package com.twitter.cr_mixer.module.core

import com.twitter.inject.TwitterModule
import com.google.inject.Provides
import javax.inject.Singleton
import com.twitter.util.Duration
import com.twitter.app.Flag
import com.twitter.cr_mixer.config.TimeoutConfig

/**
 * All timeout settings in CrMixer.
 * Timeout numbers are defined in source/cr-mixer/server/config/deploy.aurora
 */
object TimeoutConfigModule extends TwitterModule {

  /**
   * Flag names for client timeout
   * These are used in modules extending ThriftMethodBuilderClientModule
   * which cannot accept injection of TimeoutConfig
   */
  val EarlybirdClientTimeoutFlagName = "earlybird.client.timeout"
  val FrsClientTimeoutFlagName = "frsSignalFetch.client.timeout"
  val QigRankerClientTimeoutFlagName = "qigRanker.client.timeout"
  val TweetypieClientTimeoutFlagName = "tweetypie.client.timeout"
  val UserTweetGraphClientTimeoutFlagName = "userTweetGraph.client.timeout"
  val UserTweetGraphPlusClientTimeoutFlagName = "userTweetGraphPlus.client.timeout"
  val UserAdGraphClientTimeoutFlagName = "userAdGraph.client.timeout"
  val UserVideoGraphClientTimeoutFlagName = "userVideoGraph.client.timeout"
  val UtegClientTimeoutFlagName = "uteg.client.timeout"
  val NaviRequestTimeoutFlagName = "navi.client.request.timeout"

  /**
   * Flags for timeouts
   * These are defined and initialized only in this file
   */
  // timeout for the service
  private val serviceTimeout: Flag[Duration] =
    flag("service.timeout", "service total timeout")

  // timeout for signal fetch
  private val signalFetchTimeout: Flag[Duration] =
    flag[Duration]("signalFetch.timeout", "signal fetch timeout")

  // timeout for similarity engine
  private val similarityEngineTimeout: Flag[Duration] =
    flag[Duration]("similarityEngine.timeout", "similarity engine timeout")
  private val annServiceClientTimeout: Flag[Duration] =
    flag[Duration]("annService.client.timeout", "annQueryService client timeout")

  // timeout for user affinities fetcher
  private val userStateUnderlyingStoreTimeout: Flag[Duration] =
    flag[Duration]("userStateUnderlyingStore.timeout", "user state underlying store timeout")

  private val userStateStoreTimeout: Flag[Duration] =
    flag[Duration]("userStateStore.timeout", "user state store timeout")

  private val utegSimilarityEngineTimeout: Flag[Duration] =
    flag[Duration]("uteg.similarityEngine.timeout", "uteg similarity engine timeout")

  private val earlybirdServerTimeout: Flag[Duration] =
    flag[Duration]("earlybird.server.timeout", "earlybird server timeout")

  private val earlybirdSimilarityEngineTimeout: Flag[Duration] =
    flag[Duration]("earlybird.similarityEngine.timeout", "Earlybird similarity engine timeout")

  private val frsBasedTweetEndpointTimeout: Flag[Duration] =
    flag[Duration](
      "frsBasedTweet.endpoint.timeout",
      "frsBasedTweet endpoint timeout"
    )

  private val topicTweetEndpointTimeout: Flag[Duration] =
    flag[Duration](
      "topicTweet.endpoint.timeout",
      "topicTweet endpoint timeout"
    )

  // timeout for Navi client
  private val naviRequestTimeout: Flag[Duration] =
    flag[Duration](
      NaviRequestTimeoutFlagName,
      Duration.fromMilliseconds(2000),
      "Request timeout for a single RPC Call",
    )

  @Provides
  @Singleton
  def provideTimeoutBudget(): TimeoutConfig =
    TimeoutConfig(
      serviceTimeout = serviceTimeout(),
      signalFetchTimeout = signalFetchTimeout(),
      similarityEngineTimeout = similarityEngineTimeout(),
      annServiceClientTimeout = annServiceClientTimeout(),
      utegSimilarityEngineTimeout = utegSimilarityEngineTimeout(),
      userStateUnderlyingStoreTimeout = userStateUnderlyingStoreTimeout(),
      userStateStoreTimeout = userStateStoreTimeout(),
      earlybirdServerTimeout = earlybirdServerTimeout(),
      earlybirdSimilarityEngineTimeout = earlybirdSimilarityEngineTimeout(),
      frsBasedTweetEndpointTimeout = frsBasedTweetEndpointTimeout(),
      topicTweetEndpointTimeout = topicTweetEndpointTimeout(),
      naviRequestTimeout = naviRequestTimeout()
    )

}
package com.twitter.cr_mixer.module.core

import com.google.inject.Provides
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.scribe.ScribeCategories
import com.twitter.cr_mixer.scribe.ScribeCategory
import com.twitter.finagle.mtls.authentication.ServiceIdentifier
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.inject.TwitterModule
import com.twitter.logging.BareFormatter
import com.twitter.logging.Level
import com.twitter.logging.Logger
import com.twitter.logging.NullHandler
import com.twitter.logging.QueueingHandler
import com.twitter.logging.ScribeHandler
import com.twitter.logging.{LoggerFactory => TwitterLoggerFactory}
import javax.inject.Named
import javax.inject.Singleton

object LoggerFactoryModule extends TwitterModule {

  private val DefaultQueueSize = 10000

  @Provides
  @Singleton
  @Named(ModuleNames.AbDeciderLogger)
  def provideAbDeciderLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.AbDecider,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.TopLevelApiDdgMetricsLogger)
  def provideTopLevelApiDdgMetricsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.TopLevelApiDdgMetrics,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.TweetRecsLogger)
  def provideTweetRecsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.TweetsRecs,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.BlueVerifiedTweetRecsLogger)
  def provideVITTweetRecsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.VITTweetsRecs,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.RelatedTweetsLogger)
  def provideRelatedTweetsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.RelatedTweets,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.UtegTweetsLogger)
  def provideUtegTweetsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.UtegTweets,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  @Provides
  @Singleton
  @Named(ModuleNames.AdsRecommendationsLogger)
  def provideAdsRecommendationsLogger(
    serviceIdentifier: ServiceIdentifier,
    statsReceiver: StatsReceiver
  ): Logger = {
    buildLoggerFactory(
      ScribeCategories.AdsRecommendations,
      serviceIdentifier.environment,
      statsReceiver.scope("ScribeLogger"))
      .apply()
  }

  private def buildLoggerFactory(
    category: ScribeCategory,
    environment: String,
    statsReceiver: StatsReceiver
  ): TwitterLoggerFactory = {
    environment match {
      case "prod" =>
        TwitterLoggerFactory(
          node = category.getProdLoggerFactoryNode,
          level = Some(Level.INFO),
          useParents = false,
          handlers = List(
            QueueingHandler(
              maxQueueSize = DefaultQueueSize,
              handler = ScribeHandler(
                category = category.scribeCategory,
                formatter = BareFormatter,
                statsReceiver = statsReceiver.scope(category.getProdLoggerFactoryNode)
              )
            )
          )
        )
      case _ =>
        TwitterLoggerFactory(
          node = category.getStagingLoggerFactoryNode,
          level = Some(Level.DEBUG),
          useParents = false,
          handlers = List(
            { () => NullHandler }
          )
        )
    }
  }
}
package com.twitter.cr_mixer.module.core

import com.twitter.inject.TwitterModule

object CrMixerFlagName {
  val SERVICE_FLAG = "cr_mixer.flag"
  val DarkTrafficFilterDeciderKey = "thrift.dark.traffic.filter.decider_key"
}

object CrMixerFlagModule extends TwitterModule {
  import CrMixerFlagName._

  flag[Boolean](name = SERVICE_FLAG, default = false, help = "This is a CR Mixer flag")

  flag[String](
    name = DarkTrafficFilterDeciderKey,
    default = "dark_traffic_filter",
    help = "Dark traffic filter decider key"
  )
}
