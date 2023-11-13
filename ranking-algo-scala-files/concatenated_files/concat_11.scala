package com.twitter.cr_mixer.config

import com.twitter.util.Duration

case class TimeoutConfig(
  /* Default timeouts for candidate generator */
  serviceTimeout: Duration,
  signalFetchTimeout: Duration,
  similarityEngineTimeout: Duration,
  annServiceClientTimeout: Duration,
  /* For Uteg Candidate Generator */
  utegSimilarityEngineTimeout: Duration,
  /* For User State Store */
  userStateUnderlyingStoreTimeout: Duration,
  userStateStoreTimeout: Duration,
  /* For FRS based tweets */
  // Timeout passed to EarlyBird server
  earlybirdServerTimeout: Duration,
  // Timeout set on CrMixer side
  earlybirdSimilarityEngineTimeout: Duration,
  frsBasedTweetEndpointTimeout: Duration,
  topicTweetEndpointTimeout: Duration,
  // Timeout Settings for Navi gRPC Client
  naviRequestTimeout: Duration)
package com.twitter.cr_mixer.config

import com.twitter.conversions.DurationOps._
import com.twitter.cr_mixer.exception.InvalidSANNConfigException
import com.twitter.simclusters_v2.thriftscala.EmbeddingType
import com.twitter.simclustersann.thriftscala.ScoringAlgorithm
import com.twitter.simclustersann.thriftscala.{SimClustersANNConfig => ThriftSimClustersANNConfig}
import com.twitter.util.Duration

case class SimClustersANNConfig(
  maxNumResults: Int,
  minScore: Double,
  candidateEmbeddingType: EmbeddingType,
  maxTopTweetsPerCluster: Int,
  maxScanClusters: Int,
  maxTweetCandidateAge: Duration,
  minTweetCandidateAge: Duration,
  annAlgorithm: ScoringAlgorithm) {
  val toSANNConfigThrift: ThriftSimClustersANNConfig = ThriftSimClustersANNConfig(
    maxNumResults = maxNumResults,
    minScore = minScore,
    candidateEmbeddingType = candidateEmbeddingType,
    maxTopTweetsPerCluster = maxTopTweetsPerCluster,
    maxScanClusters = maxScanClusters,
    maxTweetCandidateAgeHours = maxTweetCandidateAge.inHours,
    minTweetCandidateAgeHours = minTweetCandidateAge.inHours,
    annAlgorithm = annAlgorithm,
  )
}

object SimClustersANNConfig {

  final val DefaultConfig = SimClustersANNConfig(
    maxNumResults = 200,
    minScore = 0.0,
    candidateEmbeddingType = EmbeddingType.LogFavBasedTweet,
    maxTopTweetsPerCluster = 800,
    maxScanClusters = 50,
    maxTweetCandidateAge = 24.hours,
    minTweetCandidateAge = 0.hours,
    annAlgorithm = ScoringAlgorithm.CosineSimilarity,
  )

  /*
  SimClustersANNConfigId: String
  Format: Prod - “EmbeddingType_ModelVersion_Default”
  Format: Experiment - “EmbeddingType_ModelVersion_Date_Two-Digit-Serial-Number”. Date : YYYYMMDD
   */

  private val FavBasedProducer_Model20m145k2020_Default = DefaultConfig.copy()

  // Chunnan's exp on maxTweetCandidateAgeDays 2
  private val FavBasedProducer_Model20m145k2020_20220617_06 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      maxTweetCandidateAge = 48.hours,
    )

  // Experimental SANN config
  private val FavBasedProducer_Model20m145k2020_20220801 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.VideoPlayBack50LogFavBasedTweet,
    )

  // SANN-1 config
  private val FavBasedProducer_Model20m145k2020_20220810 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-2 config
  private val FavBasedProducer_Model20m145k2020_20220818 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavClickBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-3 config
  private val FavBasedProducer_Model20m145k2020_20220819 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.PushOpenLogFavBasedTweet,
    )

  // SANN-5 config
  private val FavBasedProducer_Model20m145k2020_20221221 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedRealTimeTweet,
      maxTweetCandidateAge = 1.hours
    )

  // SANN-4 config
  private val FavBasedProducer_Model20m145k2020_20221220 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedEvergreenTweet,
      maxTweetCandidateAge = 48.hours
    )
  private val LogFavLongestL2EmbeddingTweet_Model20m145k2020_Default = DefaultConfig.copy()

  // Chunnan's exp on maxTweetCandidateAgeDays 2
  private val LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220617_06 =
    LogFavLongestL2EmbeddingTweet_Model20m145k2020_Default.copy(
      maxTweetCandidateAge = 48.hours,
    )

  // Experimental SANN config
  private val LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220801 =
    LogFavLongestL2EmbeddingTweet_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.VideoPlayBack50LogFavBasedTweet,
    )

  // SANN-1 config
  private val LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220810 =
    LogFavLongestL2EmbeddingTweet_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-2 config
  private val LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220818 =
    LogFavLongestL2EmbeddingTweet_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavClickBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-3 config
  private val LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220819 =
    LogFavLongestL2EmbeddingTweet_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.PushOpenLogFavBasedTweet,
    )

  // SANN-5 config
  private val LogFavLongestL2EmbeddingTweet_Model20m145k2020_20221221 =
    LogFavLongestL2EmbeddingTweet_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedRealTimeTweet,
      maxTweetCandidateAge = 1.hours
    )
  // SANN-4 config
  private val LogFavLongestL2EmbeddingTweet_Model20m145k2020_20221220 =
    LogFavLongestL2EmbeddingTweet_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedEvergreenTweet,
      maxTweetCandidateAge = 48.hours
    )
  private val UnfilteredUserInterestedIn_Model20m145k2020_Default = DefaultConfig.copy()

  // Chunnan's exp on maxTweetCandidateAgeDays 2
  private val UnfilteredUserInterestedIn_Model20m145k2020_20220617_06 =
    UnfilteredUserInterestedIn_Model20m145k2020_Default.copy(
      maxTweetCandidateAge = 48.hours,
    )

  // Experimental SANN config
  private val UnfilteredUserInterestedIn_Model20m145k2020_20220801 =
    UnfilteredUserInterestedIn_Model20m145k2020_20220617_06.copy(
      candidateEmbeddingType = EmbeddingType.VideoPlayBack50LogFavBasedTweet,
    )

  // SANN-1 config
  private val UnfilteredUserInterestedIn_Model20m145k2020_20220810 =
    UnfilteredUserInterestedIn_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-2 config
  private val UnfilteredUserInterestedIn_Model20m145k2020_20220818 =
    UnfilteredUserInterestedIn_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavClickBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-3 config
  private val UnfilteredUserInterestedIn_Model20m145k2020_20220819 =
    UnfilteredUserInterestedIn_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.PushOpenLogFavBasedTweet,
    )

  // SANN-5 config
  private val UnfilteredUserInterestedIn_Model20m145k2020_20221221 =
    UnfilteredUserInterestedIn_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedRealTimeTweet,
      maxTweetCandidateAge = 1.hours
    )

  // SANN-4 config
  private val UnfilteredUserInterestedIn_Model20m145k2020_20221220 =
    UnfilteredUserInterestedIn_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedEvergreenTweet,
      maxTweetCandidateAge = 48.hours
    )
  private val LogFavBasedUserInterestedInFromAPE_Model20m145k2020_Default = DefaultConfig.copy()

  // Chunnan's exp on maxTweetCandidateAgeDays 2
  private val LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220617_06 =
    LogFavBasedUserInterestedInFromAPE_Model20m145k2020_Default.copy(
      maxTweetCandidateAge = 48.hours,
    )

  // Experimental SANN config
  private val LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220801 =
    LogFavBasedUserInterestedInFromAPE_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.VideoPlayBack50LogFavBasedTweet,
    )

  // SANN-1 config
  private val LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220810 =
    LogFavBasedUserInterestedInFromAPE_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-2 config
  private val LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220818 =
    LogFavBasedUserInterestedInFromAPE_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavClickBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-3 config
  private val LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220819 =
    LogFavBasedUserInterestedInFromAPE_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.PushOpenLogFavBasedTweet,
    )

  // SANN-5 config
  private val LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20221221 =
    LogFavBasedUserInterestedInFromAPE_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedRealTimeTweet,
      maxTweetCandidateAge = 1.hours
    )

  // SANN-4 config
  private val LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20221220 =
    LogFavBasedUserInterestedInFromAPE_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedEvergreenTweet,
      maxTweetCandidateAge = 48.hours
    )
  private val LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_Default =
    DefaultConfig.copy()

  // Chunnan's exp on maxTweetCandidateAgeDays 2
  private val LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220617_06 =
    LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_Default.copy(
      maxTweetCandidateAge = 48.hours,
    )

  // Experimental SANN config
  private val LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220801 =
    LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.VideoPlayBack50LogFavBasedTweet,
    )

  // SANN-1 config
  private val LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220810 =
    LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-2 config
  private val LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220818 =
    LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavClickBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-3 config
  private val LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220819 =
    LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.PushOpenLogFavBasedTweet,
    )

  // SANN-5 config
  private val LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20221221 =
    LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedRealTimeTweet,
      maxTweetCandidateAge = 1.hours
    )

  // SANN-4 config
  private val LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20221220 =
    LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedEvergreenTweet,
      maxTweetCandidateAge = 48.hours
    )
  private val UserNextInterestedIn_Model20m145k2020_Default = DefaultConfig.copy()

  // Chunnan's exp on maxTweetCandidateAgeDays 2
  private val UserNextInterestedIn_Model20m145k2020_20220617_06 =
    UserNextInterestedIn_Model20m145k2020_Default.copy(
      maxTweetCandidateAge = 48.hours,
    )

  // Experimental SANN config
  private val UserNextInterestedIn_Model20m145k2020_20220801 =
    UserNextInterestedIn_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.VideoPlayBack50LogFavBasedTweet,
    )

  // SANN-1 config
  private val UserNextInterestedIn_Model20m145k2020_20220810 =
    UserNextInterestedIn_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-2 config
  private val UserNextInterestedIn_Model20m145k2020_20220818 =
    UserNextInterestedIn_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavClickBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-3 config
  private val UserNextInterestedIn_Model20m145k2020_20220819 =
    UserNextInterestedIn_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.PushOpenLogFavBasedTweet,
    )

  // SANN-5 config
  private val UserNextInterestedIn_Model20m145k2020_20221221 =
    UserNextInterestedIn_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedRealTimeTweet,
      maxTweetCandidateAge = 1.hours
    )

  // SANN-4 config
  private val UserNextInterestedIn_Model20m145k2020_20221220 =
    UserNextInterestedIn_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedEvergreenTweet,
      maxTweetCandidateAge = 48.hours
    )
  // Vincent's experiment on using FollowBasedProducer as query embedding type for UserFollow
  private val FollowBasedProducer_Model20m145k2020_Default =
    FavBasedProducer_Model20m145k2020_Default.copy()

  // Experimental SANN config
  private val FollowBasedProducer_Model20m145k2020_20220801 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.VideoPlayBack50LogFavBasedTweet,
    )

  // SANN-1 config
  private val FollowBasedProducer_Model20m145k2020_20220810 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-2 config
  private val FollowBasedProducer_Model20m145k2020_20220818 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      maxNumResults = 100,
      candidateEmbeddingType = EmbeddingType.LogFavClickBasedAdsTweet,
      maxTweetCandidateAge = 175200.hours,
      maxTopTweetsPerCluster = 1600
    )

  // SANN-3 config
  private val FollowBasedProducer_Model20m145k2020_20220819 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.PushOpenLogFavBasedTweet,
    )

  // SANN-5 config
  private val FollowBasedProducer_Model20m145k2020_20221221 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedRealTimeTweet,
      maxTweetCandidateAge = 1.hours
    )

  // SANN-4 config
  private val FollowBasedProducer_Model20m145k2020_20221220 =
    FavBasedProducer_Model20m145k2020_Default.copy(
      candidateEmbeddingType = EmbeddingType.LogFavBasedEvergreenTweet,
      maxTweetCandidateAge = 48.hours
    )
  val DefaultConfigMappings: Map[String, SimClustersANNConfig] = Map(
    "FavBasedProducer_Model20m145k2020_Default" -> FavBasedProducer_Model20m145k2020_Default,
    "FavBasedProducer_Model20m145k2020_20220617_06" -> FavBasedProducer_Model20m145k2020_20220617_06,
    "FavBasedProducer_Model20m145k2020_20220801" -> FavBasedProducer_Model20m145k2020_20220801,
    "FavBasedProducer_Model20m145k2020_20220810" -> FavBasedProducer_Model20m145k2020_20220810,
    "FavBasedProducer_Model20m145k2020_20220818" -> FavBasedProducer_Model20m145k2020_20220818,
    "FavBasedProducer_Model20m145k2020_20220819" -> FavBasedProducer_Model20m145k2020_20220819,
    "FavBasedProducer_Model20m145k2020_20221221" -> FavBasedProducer_Model20m145k2020_20221221,
    "FavBasedProducer_Model20m145k2020_20221220" -> FavBasedProducer_Model20m145k2020_20221220,
    "FollowBasedProducer_Model20m145k2020_Default" -> FollowBasedProducer_Model20m145k2020_Default,
    "FollowBasedProducer_Model20m145k2020_20220801" -> FollowBasedProducer_Model20m145k2020_20220801,
    "FollowBasedProducer_Model20m145k2020_20220810" -> FollowBasedProducer_Model20m145k2020_20220810,
    "FollowBasedProducer_Model20m145k2020_20220818" -> FollowBasedProducer_Model20m145k2020_20220818,
    "FollowBasedProducer_Model20m145k2020_20220819" -> FollowBasedProducer_Model20m145k2020_20220819,
    "FollowBasedProducer_Model20m145k2020_20221221" -> FollowBasedProducer_Model20m145k2020_20221221,
    "FollowBasedProducer_Model20m145k2020_20221220" -> FollowBasedProducer_Model20m145k2020_20221220,
    "LogFavLongestL2EmbeddingTweet_Model20m145k2020_Default" -> LogFavLongestL2EmbeddingTweet_Model20m145k2020_Default,
    "LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220617_06" -> LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220617_06,
    "LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220801" -> LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220801,
    "LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220810" -> LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220810,
    "LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220818" -> LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220818,
    "LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220819" -> LogFavLongestL2EmbeddingTweet_Model20m145k2020_20220819,
    "LogFavLongestL2EmbeddingTweet_Model20m145k2020_20221221" -> LogFavLongestL2EmbeddingTweet_Model20m145k2020_20221221,
    "LogFavLongestL2EmbeddingTweet_Model20m145k2020_20221220" -> LogFavLongestL2EmbeddingTweet_Model20m145k2020_20221220,
    "UnfilteredUserInterestedIn_Model20m145k2020_Default" -> UnfilteredUserInterestedIn_Model20m145k2020_Default,
    "UnfilteredUserInterestedIn_Model20m145k2020_20220617_06" -> UnfilteredUserInterestedIn_Model20m145k2020_20220617_06,
    "UnfilteredUserInterestedIn_Model20m145k2020_20220801" -> UnfilteredUserInterestedIn_Model20m145k2020_20220801,
    "UnfilteredUserInterestedIn_Model20m145k2020_20220810" -> UnfilteredUserInterestedIn_Model20m145k2020_20220810,
    "UnfilteredUserInterestedIn_Model20m145k2020_20220818" -> UnfilteredUserInterestedIn_Model20m145k2020_20220818,
    "UnfilteredUserInterestedIn_Model20m145k2020_20220819" -> UnfilteredUserInterestedIn_Model20m145k2020_20220819,
    "UnfilteredUserInterestedIn_Model20m145k2020_20221221" -> UnfilteredUserInterestedIn_Model20m145k2020_20221221,
    "UnfilteredUserInterestedIn_Model20m145k2020_20221220" -> UnfilteredUserInterestedIn_Model20m145k2020_20221220,
    "LogFavBasedUserInterestedInFromAPE_Model20m145k2020_Default" -> LogFavBasedUserInterestedInFromAPE_Model20m145k2020_Default,
    "LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220617_06" -> LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220617_06,
    "LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220801" -> LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220801,
    "LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220810" -> LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220810,
    "LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220818" -> LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220818,
    "LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220819" -> LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20220819,
    "LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20221221" -> LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20221221,
    "LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20221220" -> LogFavBasedUserInterestedInFromAPE_Model20m145k2020_20221220,
    "LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_Default" -> LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_Default,
    "LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220617_06" -> LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220617_06,
    "LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220801" -> LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220801,
    "LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220810" -> LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220810,
    "LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220818" -> LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220818,
    "LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220819" -> LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20220819,
    "LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20221221" -> LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20221221,
    "LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20221220" -> LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE_Model20m145k2020_20221220,
    "UserNextInterestedIn_Model20m145k2020_Default" -> UserNextInterestedIn_Model20m145k2020_Default,
    "UserNextInterestedIn_Model20m145k2020_20220617_06" -> UserNextInterestedIn_Model20m145k2020_20220617_06,
    "UserNextInterestedIn_Model20m145k2020_20220801" -> UserNextInterestedIn_Model20m145k2020_20220801,
    "UserNextInterestedIn_Model20m145k2020_20220810" -> UserNextInterestedIn_Model20m145k2020_20220810,
    "UserNextInterestedIn_Model20m145k2020_20220818" -> UserNextInterestedIn_Model20m145k2020_20220818,
    "UserNextInterestedIn_Model20m145k2020_20220819" -> UserNextInterestedIn_Model20m145k2020_20220819,
    "UserNextInterestedIn_Model20m145k2020_20221221" -> UserNextInterestedIn_Model20m145k2020_20221221,
    "UserNextInterestedIn_Model20m145k2020_20221220" -> UserNextInterestedIn_Model20m145k2020_20221220,
  )

  def getConfig(
    embeddingType: String,
    modelVersion: String,
    id: String
  ): SimClustersANNConfig = {
    val configName = embeddingType + "_" + modelVersion + "_" + id
    DefaultConfigMappings.get(configName) match {
      case Some(config) => config
      case None =>
        throw InvalidSANNConfigException(s"Incorrect config id passed in for SANN $configName")
    }
  }
}
package com.twitter.cr_mixer.logging

import com.twitter.cr_mixer.model.RelatedTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.logging.ScribeLoggerUtils._
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.thriftscala.FetchCandidatesResult
import com.twitter.cr_mixer.thriftscala.GetRelatedTweetsScribe
import com.twitter.cr_mixer.thriftscala.PerformanceMetrics
import com.twitter.cr_mixer.thriftscala.PreRankFilterResult
import com.twitter.cr_mixer.thriftscala.RelatedTweetRequest
import com.twitter.cr_mixer.thriftscala.RelatedTweetResponse
import com.twitter.cr_mixer.thriftscala.RelatedTweetResult
import com.twitter.cr_mixer.thriftscala.RelatedTweetTopLevelApiResult
import com.twitter.cr_mixer.thriftscala.TweetCandidateWithMetadata
import com.twitter.cr_mixer.util.CandidateGenerationKeyUtil
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Trace
import com.twitter.logging.Logger
import com.twitter.simclusters_v2.common.UserId
import com.twitter.util.Future
import com.twitter.util.Stopwatch
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

@Singleton
case class RelatedTweetScribeLogger @Inject() (
  decider: CrMixerDecider,
  statsReceiver: StatsReceiver,
  @Named(ModuleNames.RelatedTweetsLogger) relatedTweetsScribeLogger: Logger) {

  private val scopedStats = statsReceiver.scope("RelatedTweetsScribeLogger")
  private val topLevelApiStats = scopedStats.scope("TopLevelApi")
  private val topLevelApiNoUserIdStats = scopedStats.scope("TopLevelApiNoUserId")
  private val upperFunnelsStats = scopedStats.scope("UpperFunnels")
  private val upperFunnelsNoUserIdStats = scopedStats.scope("UpperFunnelsNoUserId")

  def scribeInitialCandidates(
    query: RelatedTweetCandidateGeneratorQuery,
    getResultFn: => Future[Seq[Seq[InitialCandidate]]]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    scribeResultsAndPerformanceMetrics(
      RelatedTweetScribeMetadata.from(query),
      getResultFn,
      convertToResultFn = convertFetchCandidatesResult
    )
  }

  def scribePreRankFilterCandidates(
    query: RelatedTweetCandidateGeneratorQuery,
    getResultFn: => Future[Seq[Seq[InitialCandidate]]]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    scribeResultsAndPerformanceMetrics(
      RelatedTweetScribeMetadata.from(query),
      getResultFn,
      convertToResultFn = convertPreRankFilterResult
    )
  }

  /**
   * Scribe Top Level API Request / Response and performance metrics
   * for the getRelatedTweets endpoint.
   */
  def scribeGetRelatedTweets(
    request: RelatedTweetRequest,
    startTime: Long,
    relatedTweetScribeMetadata: RelatedTweetScribeMetadata,
    getResultFn: => Future[RelatedTweetResponse]
  ): Future[RelatedTweetResponse] = {
    val timer = Stopwatch.start()
    getResultFn.onSuccess { response =>
      relatedTweetScribeMetadata.clientContext.userId match {
        case Some(userId) =>
          if (decider.isAvailableForId(userId, DeciderConstants.upperFunnelPerStepScribeRate)) {
            topLevelApiStats.counter(relatedTweetScribeMetadata.product.originalName).incr()
            val latencyMs = timer().inMilliseconds
            val result = convertTopLevelAPIResult(request, response, startTime)
            val traceId = Trace.id.traceId.toLong
            val scribeMsg =
              buildScribeMessage(result, relatedTweetScribeMetadata, latencyMs, traceId)

            scribeResult(scribeMsg)
          }
        case _ =>
          topLevelApiNoUserIdStats.counter(relatedTweetScribeMetadata.product.originalName).incr()
      }
    }
  }

  /**
   * Scribe Per-step intermediate results and performance metrics
   * for each step: fetch candidates, filters.
   */
  private def scribeResultsAndPerformanceMetrics[T](
    relatedTweetScribeMetadata: RelatedTweetScribeMetadata,
    getResultFn: => Future[T],
    convertToResultFn: (T, UserId) => RelatedTweetResult
  ): Future[T] = {
    val timer = Stopwatch.start()
    getResultFn.onSuccess { input =>
      relatedTweetScribeMetadata.clientContext.userId match {
        case Some(userId) =>
          if (decider.isAvailableForId(userId, DeciderConstants.upperFunnelPerStepScribeRate)) {
            upperFunnelsStats.counter(relatedTweetScribeMetadata.product.originalName).incr()
            val latencyMs = timer().inMilliseconds
            val result = convertToResultFn(input, userId)
            val traceId = Trace.id.traceId.toLong
            val scribeMsg =
              buildScribeMessage(result, relatedTweetScribeMetadata, latencyMs, traceId)
            scribeResult(scribeMsg)
          }
        case _ =>
          upperFunnelsNoUserIdStats.counter(relatedTweetScribeMetadata.product.originalName).incr()
      }
    }
  }

  private def convertTopLevelAPIResult(
    request: RelatedTweetRequest,
    response: RelatedTweetResponse,
    startTime: Long
  ): RelatedTweetResult = {
    RelatedTweetResult.RelatedTweetTopLevelApiResult(
      RelatedTweetTopLevelApiResult(
        timestamp = startTime,
        request = request,
        response = response
      ))
  }

  private def convertFetchCandidatesResult(
    candidatesSeq: Seq[Seq[InitialCandidate]],
    requestUserId: UserId
  ): RelatedTweetResult = {
    val tweetCandidatesWithMetadata = candidatesSeq.flatMap { candidates =>
      candidates.map { candidate =>
        TweetCandidateWithMetadata(
          tweetId = candidate.tweetId,
          candidateGenerationKey = None
        ) // do not hydrate candidateGenerationKey to save cost
      }
    }
    RelatedTweetResult.FetchCandidatesResult(
      FetchCandidatesResult(Some(tweetCandidatesWithMetadata)))
  }

  private def convertPreRankFilterResult(
    candidatesSeq: Seq[Seq[InitialCandidate]],
    requestUserId: UserId
  ): RelatedTweetResult = {
    val tweetCandidatesWithMetadata = candidatesSeq.flatMap { candidates =>
      candidates.map { candidate =>
        val candidateGenerationKey =
          CandidateGenerationKeyUtil.toThrift(candidate.candidateGenerationInfo, requestUserId)
        TweetCandidateWithMetadata(
          tweetId = candidate.tweetId,
          candidateGenerationKey = Some(candidateGenerationKey),
          authorId = Some(candidate.tweetInfo.authorId),
          score = Some(candidate.getSimilarityScore),
          numCandidateGenerationKeys = None
        )
      }
    }
    RelatedTweetResult.PreRankFilterResult(PreRankFilterResult(Some(tweetCandidatesWithMetadata)))
  }

  private def buildScribeMessage(
    relatedTweetResult: RelatedTweetResult,
    relatedTweetScribeMetadata: RelatedTweetScribeMetadata,
    latencyMs: Long,
    traceId: Long
  ): GetRelatedTweetsScribe = {
    GetRelatedTweetsScribe(
      uuid = relatedTweetScribeMetadata.requestUUID,
      internalId = relatedTweetScribeMetadata.internalId,
      relatedTweetResult = relatedTweetResult,
      requesterId = relatedTweetScribeMetadata.clientContext.userId,
      guestId = relatedTweetScribeMetadata.clientContext.guestId,
      traceId = Some(traceId),
      performanceMetrics = Some(PerformanceMetrics(Some(latencyMs))),
      impressedBuckets = getImpressedBuckets(scopedStats)
    )
  }

  private def scribeResult(
    scribeMsg: GetRelatedTweetsScribe
  ): Unit = {
    publish(logger = relatedTweetsScribeLogger, codec = GetRelatedTweetsScribe, message = scribeMsg)
  }
}
package com.twitter.cr_mixer.source_signal

import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.GraphSourceInfo
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.param.RealGraphInParams
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
 * This store fetch user recommendations from In-Network RealGraph (go/realgraph) for a given userId
 */
@Singleton
case class RealGraphInSourceGraphFetcher @Inject() (
  @Named(ModuleNames.RealGraphInStore) realGraphStoreMh: ReadableStore[UserId, CandidateSeq],
  override val timeoutConfig: TimeoutConfig,
  globalStats: StatsReceiver)
    extends SourceGraphFetcher {

  override protected val stats: StatsReceiver = globalStats.scope(identifier)
  override protected val graphSourceType: SourceType = SourceType.RealGraphIn

  override def isEnabled(query: FetcherQuery): Boolean = {
    query.params(RealGraphInParams.EnableSourceGraphParam)
  }

  override def fetchAndProcess(
    query: FetcherQuery,
  ): Future[Option[GraphSourceInfo]] = {
    val rawSignals = trackPerItemStats(query)(
      realGraphStoreMh.get(query.userId).map {
        _.map { candidateSeq =>
          candidateSeq.candidates
            .map { candidate =>
              // Bundle the userId with its score
              (candidate.userId, candidate.score)
            }
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
package com.twitter.cr_mixer.logging

import com.twitter.cr_mixer.logging.ScribeLoggerUtils._
import com.twitter.cr_mixer.model.UtegTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TweetWithScoreAndSocialProof
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.thriftscala.UtegTweetRequest
import com.twitter.cr_mixer.thriftscala.UtegTweetResponse
import com.twitter.cr_mixer.thriftscala.FetchCandidatesResult
import com.twitter.cr_mixer.thriftscala.GetUtegTweetsScribe
import com.twitter.cr_mixer.thriftscala.PerformanceMetrics
import com.twitter.cr_mixer.thriftscala.UtegTweetResult
import com.twitter.cr_mixer.thriftscala.UtegTweetTopLevelApiResult
import com.twitter.cr_mixer.thriftscala.TweetCandidateWithMetadata
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Trace
import com.twitter.logging.Logger
import com.twitter.simclusters_v2.common.UserId
import com.twitter.util.Future
import com.twitter.util.Stopwatch
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

@Singleton
case class UtegTweetScribeLogger @Inject() (
  decider: CrMixerDecider,
  statsReceiver: StatsReceiver,
  @Named(ModuleNames.UtegTweetsLogger) utegTweetScribeLogger: Logger) {

  private val scopedStats = statsReceiver.scope("UtegTweetScribeLogger")
  private val topLevelApiStats = scopedStats.scope("TopLevelApi")
  private val upperFunnelsStats = scopedStats.scope("UpperFunnels")

  def scribeInitialCandidates(
    query: UtegTweetCandidateGeneratorQuery,
    getResultFn: => Future[Seq[TweetWithScoreAndSocialProof]]
  ): Future[Seq[TweetWithScoreAndSocialProof]] = {
    scribeResultsAndPerformanceMetrics(
      ScribeMetadata.from(query),
      getResultFn,
      convertToResultFn = convertFetchCandidatesResult
    )
  }

  /**
   * Scribe Top Level API Request / Response and performance metrics
   * for the GetUtegTweetRecommendations() endpoint.
   */
  def scribeGetUtegTweetRecommendations(
    request: UtegTweetRequest,
    startTime: Long,
    scribeMetadata: ScribeMetadata,
    getResultFn: => Future[UtegTweetResponse]
  ): Future[UtegTweetResponse] = {
    val timer = Stopwatch.start()
    getResultFn.onSuccess { response =>
      if (decider.isAvailableForId(
          scribeMetadata.userId,
          DeciderConstants.upperFunnelPerStepScribeRate)) {
        topLevelApiStats.counter(scribeMetadata.product.originalName).incr()
        val latencyMs = timer().inMilliseconds
        val result = convertTopLevelAPIResult(request, response, startTime)
        val traceId = Trace.id.traceId.toLong
        val scribeMsg =
          buildScribeMessage(result, scribeMetadata, latencyMs, traceId)

        scribeResult(scribeMsg)
      }
    }
  }

  private def convertTopLevelAPIResult(
    request: UtegTweetRequest,
    response: UtegTweetResponse,
    startTime: Long
  ): UtegTweetResult = {
    UtegTweetResult.UtegTweetTopLevelApiResult(
      UtegTweetTopLevelApiResult(
        timestamp = startTime,
        request = request,
        response = response
      ))
  }

  private def buildScribeMessage(
    utegTweetResult: UtegTweetResult,
    scribeMetadata: ScribeMetadata,
    latencyMs: Long,
    traceId: Long
  ): GetUtegTweetsScribe = {
    GetUtegTweetsScribe(
      uuid = scribeMetadata.requestUUID,
      userId = scribeMetadata.userId,
      utegTweetResult = utegTweetResult,
      traceId = Some(traceId),
      performanceMetrics = Some(PerformanceMetrics(Some(latencyMs))),
      impressedBuckets = getImpressedBuckets(scopedStats)
    )
  }

  private def scribeResult(
    scribeMsg: GetUtegTweetsScribe
  ): Unit = {
    publish(logger = utegTweetScribeLogger, codec = GetUtegTweetsScribe, message = scribeMsg)
  }

  private def convertFetchCandidatesResult(
    candidates: Seq[TweetWithScoreAndSocialProof],
    requestUserId: UserId
  ): UtegTweetResult = {
    val tweetCandidatesWithMetadata = candidates.map { candidate =>
      TweetCandidateWithMetadata(
        tweetId = candidate.tweetId,
        candidateGenerationKey = None
      ) // do not hydrate candidateGenerationKey to save cost
    }
    UtegTweetResult.FetchCandidatesResult(FetchCandidatesResult(Some(tweetCandidatesWithMetadata)))
  }

  /**
   * Scribe Per-step intermediate results and performance metrics
   * for each step: fetch candidates, filters.
   */
  private def scribeResultsAndPerformanceMetrics[T](
    scribeMetadata: ScribeMetadata,
    getResultFn: => Future[T],
    convertToResultFn: (T, UserId) => UtegTweetResult
  ): Future[T] = {
    val timer = Stopwatch.start()
    getResultFn.onSuccess { input =>
      if (decider.isAvailableForId(
          scribeMetadata.userId,
          DeciderConstants.upperFunnelPerStepScribeRate)) {
        upperFunnelsStats.counter(scribeMetadata.product.originalName).incr()
        val latencyMs = timer().inMilliseconds
        val result = convertToResultFn(input, scribeMetadata.userId)
        val traceId = Trace.id.traceId.toLong
        val scribeMsg =
          buildScribeMessage(result, scribeMetadata, latencyMs, traceId)
        scribeResult(scribeMsg)
      }
    }
  }
}
package com.twitter.cr_mixer.logging

import com.twitter.cr_mixer.featureswitch.CrMixerImpressedBuckets
import com.twitter.cr_mixer.thriftscala.ImpressesedBucketInfo
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.logging.Logger
import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.scrooge.ThriftStruct
import com.twitter.scrooge.ThriftStructCodec

object ScribeLoggerUtils {

  /**
   * Handles base64-encoding, serialization, and publish.
   */
  private[logging] def publish[T <: ThriftStruct](
    logger: Logger,
    codec: ThriftStructCodec[T],
    message: T
  ): Unit = {
    logger.info(BinaryThriftStructSerializer(codec).toString(message))
  }

  private[logging] def getImpressedBuckets(
    scopedStats: StatsReceiver
  ): Option[List[ImpressesedBucketInfo]] = {
    StatsUtil.trackNonFutureBlockStats(scopedStats.scope("getImpressedBuckets")) {
      CrMixerImpressedBuckets.getAllImpressedBuckets.map { listBuckets =>
        val listBucketsSet = listBuckets.toSet
        scopedStats.stat("impressed_buckets").add(listBucketsSet.size)
        listBucketsSet.map { bucket =>
          ImpressesedBucketInfo(
            experimentId = bucket.experiment.settings.experimentId.getOrElse(-1L),
            bucketName = bucket.name,
            version = bucket.experiment.settings.version,
          )
        }.toList
      }
    }
  }

}
package com.twitter.cr_mixer
package logging

import com.twitter.cr_mixer.thriftscala.CrMixerTweetRequest
import com.twitter.cr_mixer.thriftscala.Product

case class TopLevelDdgMetricsMetadata(
  userId: Option[Long],
  product: Product,
  clientApplicationId: Option[Long],
  countryCode: Option[String])

object TopLevelDdgMetricsMetadata {
  def from(request: CrMixerTweetRequest): TopLevelDdgMetricsMetadata = {
    TopLevelDdgMetricsMetadata(
      userId = request.clientContext.userId,
      product = request.product,
      clientApplicationId = request.clientContext.appId,
      countryCode = request.clientContext.countryCode
    )
  }
}
package com.twitter.cr_mixer.logging

import com.google.common.base.CaseFormat
import com.twitter.abdecider.ScribingABDeciderUtil
import com.twitter.scribelib.marshallers.ClientDataProvider
import com.twitter.scribelib.marshallers.ScribeSerialization
import com.twitter.timelines.clientevent.MinimalClientDataProvider
import com.twitter.cr_mixer.model.BlendedCandidate
import com.twitter.cr_mixer.model.CrCandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialCandidate
import com.twitter.cr_mixer.model.RankedCandidate
import com.twitter.cr_mixer.logging.ScribeLoggerUtils._
import com.twitter.cr_mixer.model.GraphSourceInfo
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.scribe.ScribeCategories
import com.twitter.cr_mixer.thriftscala.CrMixerTweetRequest
import com.twitter.cr_mixer.thriftscala.CrMixerTweetResponse
import com.twitter.cr_mixer.thriftscala.FetchCandidatesResult
import com.twitter.cr_mixer.thriftscala.FetchSignalSourcesResult
import com.twitter.cr_mixer.thriftscala.GetTweetsRecommendationsScribe
import com.twitter.cr_mixer.thriftscala.InterleaveResult
import com.twitter.cr_mixer.thriftscala.PerformanceMetrics
import com.twitter.cr_mixer.thriftscala.PreRankFilterResult
import com.twitter.cr_mixer.thriftscala.Product
import com.twitter.cr_mixer.thriftscala.RankResult
import com.twitter.cr_mixer.thriftscala.Result
import com.twitter.cr_mixer.thriftscala.SourceSignal
import com.twitter.cr_mixer.thriftscala.TopLevelApiResult
import com.twitter.cr_mixer.thriftscala.TweetCandidateWithMetadata
import com.twitter.cr_mixer.thriftscala.VITTweetCandidateScribe
import com.twitter.cr_mixer.thriftscala.VITTweetCandidatesScribe
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.util.CandidateGenerationKeyUtil
import com.twitter.cr_mixer.util.MetricTagUtil
import com.twitter.decider.SimpleRecipient
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Trace
import com.twitter.finatra.kafka.producers.KafkaProducerBase
import com.twitter.logging.Logger
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.util.Future
import com.twitter.util.Stopwatch
import com.twitter.util.Time

import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton
import scala.util.Random

@Singleton
case class CrMixerScribeLogger @Inject() (
  decider: CrMixerDecider,
  statsReceiver: StatsReceiver,
  @Named(ModuleNames.TweetRecsLogger) tweetRecsScribeLogger: Logger,
  @Named(ModuleNames.BlueVerifiedTweetRecsLogger) blueVerifiedTweetRecsScribeLogger: Logger,
  @Named(ModuleNames.TopLevelApiDdgMetricsLogger) ddgMetricsLogger: Logger,
  kafkaProducer: KafkaProducerBase[String, GetTweetsRecommendationsScribe]) {

  import CrMixerScribeLogger._

  private val scopedStats = statsReceiver.scope("CrMixerScribeLogger")
  private val topLevelApiStats = scopedStats.scope("TopLevelApi")
  private val upperFunnelsStats = scopedStats.scope("UpperFunnels")
  private val kafkaMessagesStats = scopedStats.scope("KafkaMessages")
  private val topLevelApiDdgMetricsStats = scopedStats.scope("TopLevelApiDdgMetrics")
  private val blueVerifiedTweetCandidatesStats = scopedStats.scope("BlueVerifiedTweetCandidates")

  private val serialization = new ScribeSerialization {}

  def scribeSignalSources(
    query: CrCandidateGeneratorQuery,
    getResultFn: => Future[(Set[SourceInfo], Map[String, Option[GraphSourceInfo]])]
  ): Future[(Set[SourceInfo], Map[String, Option[GraphSourceInfo]])] = {
    scribeResultsAndPerformanceMetrics(
      ScribeMetadata.from(query),
      getResultFn,
      convertToResultFn = convertFetchSignalSourcesResult
    )
  }

  def scribeInitialCandidates(
    query: CrCandidateGeneratorQuery,
    getResultFn: => Future[Seq[Seq[InitialCandidate]]]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    scribeResultsAndPerformanceMetrics(
      ScribeMetadata.from(query),
      getResultFn,
      convertToResultFn = convertFetchCandidatesResult
    )
  }

  def scribePreRankFilterCandidates(
    query: CrCandidateGeneratorQuery,
    getResultFn: => Future[Seq[Seq[InitialCandidate]]]
  ): Future[Seq[Seq[InitialCandidate]]] = {
    scribeResultsAndPerformanceMetrics(
      ScribeMetadata.from(query),
      getResultFn,
      convertToResultFn = convertPreRankFilterResult
    )
  }

  def scribeInterleaveCandidates(
    query: CrCandidateGeneratorQuery,
    getResultFn: => Future[Seq[BlendedCandidate]]
  ): Future[Seq[BlendedCandidate]] = {
    scribeResultsAndPerformanceMetrics(
      ScribeMetadata.from(query),
      getResultFn,
      convertToResultFn = convertInterleaveResult,
      enableKafkaScribe = true
    )
  }

  def scribeRankedCandidates(
    query: CrCandidateGeneratorQuery,
    getResultFn: => Future[Seq[RankedCandidate]]
  ): Future[Seq[RankedCandidate]] = {
    scribeResultsAndPerformanceMetrics(
      ScribeMetadata.from(query),
      getResultFn,
      convertToResultFn = convertRankResult
    )
  }

  /**
   * Scribe Top Level API Request / Response and performance metrics
   * for the getTweetRecommendations() endpoint.
   */
  def scribeGetTweetRecommendations(
    request: CrMixerTweetRequest,
    startTime: Long,
    scribeMetadata: ScribeMetadata,
    getResultFn: => Future[CrMixerTweetResponse]
  ): Future[CrMixerTweetResponse] = {
    val timer = Stopwatch.start()
    getResultFn.onSuccess { response =>
      val latencyMs = timer().inMilliseconds
      val result = convertTopLevelAPIResult(request, response, startTime)
      val traceId = Trace.id.traceId.toLong
      val scribeMsg = buildScribeMessage(result, scribeMetadata, latencyMs, traceId)

      // We use upperFunnelPerStepScribeRate to cover TopLevelApi scribe logs
      if (decider.isAvailableForId(
          scribeMetadata.userId,
          DeciderConstants.upperFunnelPerStepScribeRate)) {
        topLevelApiStats.counter(scribeMetadata.product.originalName).incr()
        scribeResult(scribeMsg)
      }
      if (decider.isAvailableForId(
          scribeMetadata.userId,
          DeciderConstants.topLevelApiDdgMetricsScribeRate)) {
        topLevelApiDdgMetricsStats.counter(scribeMetadata.product.originalName).incr()
        val topLevelDdgMetricsMetadata = TopLevelDdgMetricsMetadata.from(request)
        publishTopLevelDdgMetrics(
          logger = ddgMetricsLogger,
          topLevelDdgMetricsMetadata = topLevelDdgMetricsMetadata,
          latencyMs = latencyMs,
          candidateSize = response.tweets.length)
      }
    }
  }

  /**
   * Scribe all of the Blue Verified tweets that are candidates from cr-mixer
   * from the getTweetRecommendations() endpoint for stats tracking/debugging purposes.
   */
  def scribeGetTweetRecommendationsForBlueVerified(
    scribeMetadata: ScribeMetadata,
    getResultFn: => Future[Seq[RankedCandidate]]
  ): Future[Seq[RankedCandidate]] = {
    getResultFn.onSuccess { rankedCandidates =>
      if (decider.isAvailable(DeciderConstants.enableScribeForBlueVerifiedTweetCandidates)) {
        blueVerifiedTweetCandidatesStats.counter("process_request").incr()

        val blueVerifiedTweetCandidates = rankedCandidates.filter { tweet =>
          tweet.tweetInfo.hasBlueVerifiedAnnotation.contains(true)
        }

        val impressedBuckets = getImpressedBuckets(blueVerifiedTweetCandidatesStats).getOrElse(Nil)

        val blueVerifiedCandidateScribes = blueVerifiedTweetCandidates.map { candidate =>
          blueVerifiedTweetCandidatesStats
            .scope(scribeMetadata.product.name).counter(
              candidate.tweetInfo.authorId.toString).incr()
          VITTweetCandidateScribe(
            tweetId = candidate.tweetId,
            authorId = candidate.tweetInfo.authorId,
            score = candidate.predictionScore,
            metricTags = MetricTagUtil.buildMetricTags(candidate)
          )
        }

        val blueVerifiedScribe =
          VITTweetCandidatesScribe(
            uuid = scribeMetadata.requestUUID,
            userId = scribeMetadata.userId,
            candidates = blueVerifiedCandidateScribes,
            product = scribeMetadata.product,
            impressedBuckets = impressedBuckets
          )

        publish(
          logger = blueVerifiedTweetRecsScribeLogger,
          codec = VITTweetCandidatesScribe,
          message = blueVerifiedScribe)
      }
    }
  }

  /**
   * Scribe Per-step intermediate results and performance metrics
   * for each step: fetch signals, fetch candidates, filters, ranker, etc
   */
  private[logging] def scribeResultsAndPerformanceMetrics[T](
    scribeMetadata: ScribeMetadata,
    getResultFn: => Future[T],
    convertToResultFn: (T, UserId) => Result,
    enableKafkaScribe: Boolean = false
  ): Future[T] = {
    val timer = Stopwatch.start()
    getResultFn.onSuccess { input =>
      val latencyMs = timer().inMilliseconds
      val result = convertToResultFn(input, scribeMetadata.userId)
      val traceId = Trace.id.traceId.toLong
      val scribeMsg = buildScribeMessage(result, scribeMetadata, latencyMs, traceId)

      if (decider.isAvailableForId(
          scribeMetadata.userId,
          DeciderConstants.upperFunnelPerStepScribeRate)) {
        upperFunnelsStats.counter(scribeMetadata.product.originalName).incr()
        scribeResult(scribeMsg)
      }

      // forks the scribe as a Kafka message for async feature hydration
      if (enableKafkaScribe && shouldScribeKafkaMessage(
          scribeMetadata.userId,
          scribeMetadata.product)) {
        kafkaMessagesStats.counter(scribeMetadata.product.originalName).incr()

        val batchedKafkaMessages = downsampleKafkaMessage(scribeMsg)
        batchedKafkaMessages.foreach { kafkaMessage =>
          kafkaProducer.send(
            topic = ScribeCategories.TweetsRecs.scribeCategory,
            key = traceId.toString,
            value = kafkaMessage,
            timestamp = Time.now.inMilliseconds
          )
        }
      }
    }
  }

  private def convertTopLevelAPIResult(
    request: CrMixerTweetRequest,
    response: CrMixerTweetResponse,
    startTime: Long
  ): Result = {
    Result.TopLevelApiResult(
      TopLevelApiResult(
        timestamp = startTime,
        request = request,
        response = response
      ))
  }

  private def convertFetchSignalSourcesResult(
    sourceInfoSetTuple: (Set[SourceInfo], Map[String, Option[GraphSourceInfo]]),
    requestUserId: UserId
  ): Result = {
    val sourceSignals = sourceInfoSetTuple._1.map { sourceInfo =>
      SourceSignal(id = Some(sourceInfo.internalId))
    }
    // For source graphs, we pass in requestUserId as a placeholder
    val sourceGraphs = sourceInfoSetTuple._2.map {
      case (_, _) =>
        SourceSignal(id = Some(InternalId.UserId(requestUserId)))
    }
    Result.FetchSignalSourcesResult(
      FetchSignalSourcesResult(
        signals = Some(sourceSignals ++ sourceGraphs)
      ))
  }

  private def convertFetchCandidatesResult(
    candidatesSeq: Seq[Seq[InitialCandidate]],
    requestUserId: UserId
  ): Result = {
    val tweetCandidatesWithMetadata = candidatesSeq.flatMap { candidates =>
      candidates.map { candidate =>
        TweetCandidateWithMetadata(
          tweetId = candidate.tweetId,
          candidateGenerationKey = Some(
            CandidateGenerationKeyUtil.toThrift(candidate.candidateGenerationInfo, requestUserId)),
          score = Some(candidate.getSimilarityScore),
          numCandidateGenerationKeys = None // not populated yet
        )
      }
    }
    Result.FetchCandidatesResult(FetchCandidatesResult(Some(tweetCandidatesWithMetadata)))
  }

  private def convertPreRankFilterResult(
    candidatesSeq: Seq[Seq[InitialCandidate]],
    requestUserId: UserId
  ): Result = {
    val tweetCandidatesWithMetadata = candidatesSeq.flatMap { candidates =>
      candidates.map { candidate =>
        TweetCandidateWithMetadata(
          tweetId = candidate.tweetId,
          candidateGenerationKey = Some(
            CandidateGenerationKeyUtil.toThrift(candidate.candidateGenerationInfo, requestUserId)),
          score = Some(candidate.getSimilarityScore),
          numCandidateGenerationKeys = None // not populated yet
        )
      }
    }
    Result.PreRankFilterResult(PreRankFilterResult(Some(tweetCandidatesWithMetadata)))
  }

  // We take InterleaveResult for Unconstrained dataset ML ranker training
  private def convertInterleaveResult(
    blendedCandidates: Seq[BlendedCandidate],
    requestUserId: UserId
  ): Result = {
    val tweetCandidatesWithMetadata = blendedCandidates.map { blendedCandidate =>
      val candidateGenerationKey =
        CandidateGenerationKeyUtil.toThrift(blendedCandidate.reasonChosen, requestUserId)
      TweetCandidateWithMetadata(
        tweetId = blendedCandidate.tweetId,
        candidateGenerationKey = Some(candidateGenerationKey),
        authorId = Some(blendedCandidate.tweetInfo.authorId), // for ML pipeline training
        score = Some(blendedCandidate.getSimilarityScore),
        numCandidateGenerationKeys = Some(blendedCandidate.potentialReasons.size)
      ) // hydrate fields for light ranking training data
    }
    Result.InterleaveResult(InterleaveResult(Some(tweetCandidatesWithMetadata)))
  }

  private def convertRankResult(
    rankedCandidates: Seq[RankedCandidate],
    requestUserId: UserId
  ): Result = {
    val tweetCandidatesWithMetadata = rankedCandidates.map { rankedCandidate =>
      val candidateGenerationKey =
        CandidateGenerationKeyUtil.toThrift(rankedCandidate.reasonChosen, requestUserId)
      TweetCandidateWithMetadata(
        tweetId = rankedCandidate.tweetId,
        candidateGenerationKey = Some(candidateGenerationKey),
        score = Some(rankedCandidate.getSimilarityScore),
        numCandidateGenerationKeys = Some(rankedCandidate.potentialReasons.size)
      )
    }
    Result.RankResult(RankResult(Some(tweetCandidatesWithMetadata)))
  }

  private def buildScribeMessage(
    result: Result,
    scribeMetadata: ScribeMetadata,
    latencyMs: Long,
    traceId: Long
  ): GetTweetsRecommendationsScribe = {
    GetTweetsRecommendationsScribe(
      uuid = scribeMetadata.requestUUID,
      userId = scribeMetadata.userId,
      result = result,
      traceId = Some(traceId),
      performanceMetrics = Some(PerformanceMetrics(Some(latencyMs))),
      impressedBuckets = getImpressedBuckets(scopedStats)
    )
  }

  private def scribeResult(
    scribeMsg: GetTweetsRecommendationsScribe
  ): Unit = {
    publish(
      logger = tweetRecsScribeLogger,
      codec = GetTweetsRecommendationsScribe,
      message = scribeMsg)
  }

  /**
   * Gate for producing messages to Kafka for async feature hydration
   */
  private def shouldScribeKafkaMessage(
    userId: UserId,
    product: Product
  ): Boolean = {
    val isEligibleUser = decider.isAvailable(
      DeciderConstants.kafkaMessageScribeSampleRate,
      Some(SimpleRecipient(userId)))
    val isHomeProduct = (product == Product.Home)
    isEligibleUser && isHomeProduct
  }

  /**
   * Due to size limits of Strato (see SD-19028), each Kafka message must be downsampled
   */
  private[logging] def downsampleKafkaMessage(
    scribeMsg: GetTweetsRecommendationsScribe
  ): Seq[GetTweetsRecommendationsScribe] = {
    val sampledResultSeq: Seq[Result] = scribeMsg.result match {
      case Result.InterleaveResult(interleaveResult) =>
        val sampledTweetsSeq = interleaveResult.tweets
          .map { tweets =>
            Random
              .shuffle(tweets).take(KafkaMaxTweetsPerMessage)
              .grouped(BatchSize).toSeq
          }.getOrElse(Seq.empty)

        sampledTweetsSeq.map { sampledTweets =>
          Result.InterleaveResult(InterleaveResult(Some(sampledTweets)))
        }

      // if it's an unrecognized type, err on the side of sending no candidates
      case _ =>
        kafkaMessagesStats.counter("InvalidKafkaMessageResultType").incr()
        Seq(Result.InterleaveResult(InterleaveResult(None)))
    }

    sampledResultSeq.map { sampledResult =>
      GetTweetsRecommendationsScribe(
        uuid = scribeMsg.uuid,
        userId = scribeMsg.userId,
        result = sampledResult,
        traceId = scribeMsg.traceId,
        performanceMetrics = None,
        impressedBuckets = None
      )
    }
  }

  /**
   * Handles client_event serialization to log data into DDG metrics
   */
  private[logging] def publishTopLevelDdgMetrics(
    logger: Logger,
    topLevelDdgMetricsMetadata: TopLevelDdgMetricsMetadata,
    candidateSize: Long,
    latencyMs: Long,
  ): Unit = {
    val data = Map[Any, Any](
      "latency_ms" -> latencyMs,
      "event_value" -> candidateSize
    )
    val label: (String, String) = ("tweetrec", "")
    val namespace = getNamespace(topLevelDdgMetricsMetadata, label) + ("action" -> "candidates")
    val message =
      serialization
        .serializeClientEvent(namespace, getClientData(topLevelDdgMetricsMetadata), data)
    logger.info(message)
  }

  private def getClientData(
    topLevelDdgMetricsMetadata: TopLevelDdgMetricsMetadata
  ): ClientDataProvider =
    MinimalClientDataProvider(
      userId = topLevelDdgMetricsMetadata.userId,
      guestId = None,
      clientApplicationId = topLevelDdgMetricsMetadata.clientApplicationId,
      countryCode = topLevelDdgMetricsMetadata.countryCode
    )

  private def getNamespace(
    topLevelDdgMetricsMetadata: TopLevelDdgMetricsMetadata,
    label: (String, String)
  ): Map[String, String] = {
    val productName =
      CaseFormat.UPPER_CAMEL
        .to(CaseFormat.LOWER_UNDERSCORE, topLevelDdgMetricsMetadata.product.originalName)

    Map(
      "client" -> ScribingABDeciderUtil.clientForAppId(
        topLevelDdgMetricsMetadata.clientApplicationId),
      "page" -> "cr-mixer",
      "section" -> productName,
      "component" -> label._1,
      "element" -> label._2
    )
  }
}

object CrMixerScribeLogger {
  val KafkaMaxTweetsPerMessage: Int = 200
  val BatchSize: Int = 20
}
package com.twitter.cr_mixer.logging

import com.twitter.cr_mixer.model.AdsCandidateGeneratorQuery
import com.twitter.cr_mixer.model.InitialAdsCandidate
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.logging.ScribeLoggerUtils._
import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.param.decider.DeciderConstants
import com.twitter.cr_mixer.thriftscala.AdsRecommendationTopLevelApiResult
import com.twitter.cr_mixer.thriftscala.AdsRecommendationsResult
import com.twitter.cr_mixer.thriftscala.AdsRequest
import com.twitter.cr_mixer.thriftscala.AdsResponse
import com.twitter.cr_mixer.thriftscala.FetchCandidatesResult
import com.twitter.cr_mixer.thriftscala.GetAdsRecommendationsScribe
import com.twitter.cr_mixer.thriftscala.PerformanceMetrics
import com.twitter.cr_mixer.thriftscala.TweetCandidateWithMetadata
import com.twitter.cr_mixer.util.CandidateGenerationKeyUtil
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Trace
import com.twitter.logging.Logger
import com.twitter.simclusters_v2.common.UserId
import com.twitter.util.Future
import com.twitter.util.Stopwatch

import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

@Singleton
case class AdsRecommendationsScribeLogger @Inject() (
  @Named(ModuleNames.AdsRecommendationsLogger) adsRecommendationsScribeLogger: Logger,
  decider: CrMixerDecider,
  statsReceiver: StatsReceiver) {

  private val scopedStats = statsReceiver.scope(this.getClass.getCanonicalName)
  private val upperFunnelsStats = scopedStats.scope("UpperFunnels")
  private val topLevelApiStats = scopedStats.scope("TopLevelApi")

  /*
   * Scribe first step results after fetching initial ads candidate
   * */
  def scribeInitialAdsCandidates(
    query: AdsCandidateGeneratorQuery,
    getResultFn: => Future[Seq[Seq[InitialAdsCandidate]]],
    enableScribe: Boolean // controlled by feature switch so that we can scribe for certain DDG
  ): Future[Seq[Seq[InitialAdsCandidate]]] = {
    val scribeMetadata = ScribeMetadata.from(query)
    val timer = Stopwatch.start()
    getResultFn.onSuccess { input =>
      val latencyMs = timer().inMilliseconds
      val result = convertFetchCandidatesResult(input, scribeMetadata.userId)
      val traceId = Trace.id.traceId.toLong
      val scribeMsg = buildScribeMessage(result, scribeMetadata, latencyMs, traceId)

      if (enableScribe && decider.isAvailableForId(
          scribeMetadata.userId,
          DeciderConstants.adsRecommendationsPerExperimentScribeRate)) {
        upperFunnelsStats.counter(scribeMetadata.product.originalName).incr()
        scribeResult(scribeMsg)
      }
    }
  }

  /*
   * Scribe top level API results
   * */
  def scribeGetAdsRecommendations(
    request: AdsRequest,
    startTime: Long,
    scribeMetadata: ScribeMetadata,
    getResultFn: => Future[AdsResponse],
    enableScribe: Boolean
  ): Future[AdsResponse] = {
    val timer = Stopwatch.start()
    getResultFn.onSuccess { response =>
      val latencyMs = timer().inMilliseconds
      val result = AdsRecommendationsResult.AdsRecommendationTopLevelApiResult(
        AdsRecommendationTopLevelApiResult(
          timestamp = startTime,
          request = request,
          response = response
        ))
      val traceId = Trace.id.traceId.toLong
      val scribeMsg = buildScribeMessage(result, scribeMetadata, latencyMs, traceId)

      if (enableScribe && decider.isAvailableForId(
          scribeMetadata.userId,
          DeciderConstants.adsRecommendationsPerExperimentScribeRate)) {
        topLevelApiStats.counter(scribeMetadata.product.originalName).incr()
        scribeResult(scribeMsg)
      }
    }
  }

  private def convertFetchCandidatesResult(
    candidatesSeq: Seq[Seq[InitialAdsCandidate]],
    requestUserId: UserId
  ): AdsRecommendationsResult = {
    val tweetCandidatesWithMetadata = candidatesSeq.flatMap { candidates =>
      candidates.map { candidate =>
        TweetCandidateWithMetadata(
          tweetId = candidate.tweetId,
          candidateGenerationKey = Some(
            CandidateGenerationKeyUtil.toThrift(candidate.candidateGenerationInfo, requestUserId)),
          score = Some(candidate.getSimilarityScore),
          numCandidateGenerationKeys = None // not populated yet
        )
      }
    }
    AdsRecommendationsResult.FetchCandidatesResult(
      FetchCandidatesResult(Some(tweetCandidatesWithMetadata)))
  }

  private def buildScribeMessage(
    result: AdsRecommendationsResult,
    scribeMetadata: ScribeMetadata,
    latencyMs: Long,
    traceId: Long
  ): GetAdsRecommendationsScribe = {
    GetAdsRecommendationsScribe(
      uuid = scribeMetadata.requestUUID,
      userId = scribeMetadata.userId,
      result = result,
      traceId = Some(traceId),
      performanceMetrics = Some(PerformanceMetrics(Some(latencyMs))),
      impressedBuckets = getImpressedBuckets(scopedStats)
    )
  }

  private def scribeResult(
    scribeMsg: GetAdsRecommendationsScribe
  ): Unit = {
    publish(
      logger = adsRecommendationsScribeLogger,
      codec = GetAdsRecommendationsScribe,
      message = scribeMsg)
  }

}
