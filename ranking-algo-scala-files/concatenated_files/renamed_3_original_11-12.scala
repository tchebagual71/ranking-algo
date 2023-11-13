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
package com.twitter.cr_mixer.logging

import com.twitter.cr_mixer.model.AdsCandidateGeneratorQuery
import com.twitter.cr_mixer.model.CrCandidateGeneratorQuery
import com.twitter.cr_mixer.model.RelatedTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.model.UtegTweetCandidateGeneratorQuery
import com.twitter.cr_mixer.thriftscala.Product
import com.twitter.product_mixer.core.thriftscala.ClientContext
import com.twitter.simclusters_v2.common.UserId
import com.twitter.simclusters_v2.thriftscala.InternalId

case class ScribeMetadata(
  requestUUID: Long,
  userId: UserId,
  product: Product)

object ScribeMetadata {
  def from(query: CrCandidateGeneratorQuery): ScribeMetadata = {
    ScribeMetadata(query.requestUUID, query.userId, query.product)
  }

  def from(query: UtegTweetCandidateGeneratorQuery): ScribeMetadata = {
    ScribeMetadata(query.requestUUID, query.userId, query.product)
  }

  def from(query: AdsCandidateGeneratorQuery): ScribeMetadata = {
    ScribeMetadata(query.requestUUID, query.userId, query.product)
  }
}

case class RelatedTweetScribeMetadata(
  requestUUID: Long,
  internalId: InternalId,
  clientContext: ClientContext,
  product: Product)

object RelatedTweetScribeMetadata {
  def from(query: RelatedTweetCandidateGeneratorQuery): RelatedTweetScribeMetadata = {
    RelatedTweetScribeMetadata(
      query.requestUUID,
      query.internalId,
      query.clientContext,
      query.product)
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.param.decider.CrMixerDecider
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.GlobalRequestTimeoutException
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.mux.ServerApplicationError
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.hashing.KeyHasher
import com.twitter.hermit.store.common.ObservedMemcachedReadableStore
import com.twitter.relevance_platform.common.injection.LZ4Injection
import com.twitter.relevance_platform.common.injection.SeqObjectInjection
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.Params
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.TimeoutException
import com.twitter.util.logging.Logging
import org.apache.thrift.TApplicationException

/**
 * A SimilarityEngine is a wrapper which, given a [[Query]], returns a list of [[Candidate]]
 * The main purposes of a SimilarityEngine is to provide a consistent interface for candidate
 * generation logic, and provides default functions, including:
 * - Identification
 * - Observability
 * - Timeout settings
 * - Exception Handling
 * - Gating by Deciders & FeatureSwitch settings
 * - (coming soon): Dark traffic
 *
 * Note:
 * A SimilarityEngine by itself is NOT meant to be cacheable.
 * Caching should be implemented in the underlying ReadableStore that provides the [[Candidate]]s
 *
 * Please keep extension of this class local this directory only
 *
 */
trait SimilarityEngine[Query, Candidate] {

  /**
   * Uniquely identifies a similarity engine.
   * Avoid using the same engine type for more than one engine, it will cause stats to double count
   */
  private[similarity_engine] def identifier: SimilarityEngineType

  def getCandidates(query: Query): Future[Option[Seq[Candidate]]]

}

object SimilarityEngine extends Logging {
  case class SimilarityEngineConfig(
    timeout: Duration,
    gatingConfig: GatingConfig)

  /**
   * Controls for whether or not this Engine is enabled.
   * In our previous design, we were expecting a Sim Engine will only take one set of Params,
   * and that’s why we decided to have GatingConfig and the EnableFeatureSwitch in the trait.
   * However, we now have two candidate generation pipelines: Tweet Rec, Related Tweets
   * and they are now having their own set of Params, but EnableFeatureSwitch can only put in 1 fixed value.
   * We need some further refactor work to make it more flexible.
   *
   * @param deciderConfig Gate the Engine by a decider. If specified,
   * @param enableFeatureSwitch. DO NOT USE IT FOR NOW. It needs some refactorting. Please set it to None (SD-20268)
   */
  case class GatingConfig(
    deciderConfig: Option[DeciderConfig],
    enableFeatureSwitch: Option[
      FSParam[Boolean]
    ]) // Do NOT use the enableFeatureSwitch. It needs some refactoring.

  case class DeciderConfig(
    decider: CrMixerDecider,
    deciderString: String)

  case class MemCacheConfig[K](
    cacheClient: Client,
    ttl: Duration,
    asyncUpdate: Boolean = false,
    keyToString: K => String)

  private[similarity_engine] def isEnabled(
    params: Params,
    gatingConfig: GatingConfig
  ): Boolean = {
    val enabledByDecider =
      gatingConfig.deciderConfig.forall { config =>
        config.decider.isAvailable(config.deciderString)
      }

    val enabledByFS = gatingConfig.enableFeatureSwitch.forall(params.apply)

    enabledByDecider && enabledByFS
  }

  // Default key hasher for memcache keys
  val keyHasher: KeyHasher = KeyHasher.FNV1A_64

  /**
   * Add a MemCache wrapper to a ReadableStore with a preset key and value injection functions
   * Note: The [[Query]] object needs to be cacheable,
   * i.e. it cannot be a runtime objects or complex objects, for example, configapi.Params
   *
   * @param underlyingStore un-cached store implementation
   * @param keyPrefix       a prefix differentiates 2 stores if they share the same key space.
   *                        e.x. 2 implementations of ReadableStore[UserId, Seq[Candidiate] ]
   *                        can use prefix "store_v1", "store_v2"
   * @return                A ReadableStore with a MemCache wrapper
   */
  private[similarity_engine] def addMemCache[Query, Candidate <: Serializable](
    underlyingStore: ReadableStore[Query, Seq[Candidate]],
    memCacheConfig: MemCacheConfig[Query],
    keyPrefix: Option[String] = None,
    statsReceiver: StatsReceiver
  ): ReadableStore[Query, Seq[Candidate]] = {
    val prefix = keyPrefix.getOrElse("")

    ObservedMemcachedReadableStore.fromCacheClient[Query, Seq[Candidate]](
      backingStore = underlyingStore,
      cacheClient = memCacheConfig.cacheClient,
      ttl = memCacheConfig.ttl,
      asyncUpdate = memCacheConfig.asyncUpdate,
    )(
      valueInjection = LZ4Injection.compose(SeqObjectInjection[Candidate]()),
      keyToString = { k: Query => s"CRMixer:$prefix${memCacheConfig.keyToString(k)}" },
      statsReceiver = statsReceiver
    )
  }

  private val timer = com.twitter.finagle.util.DefaultTimer

  /**
   * Applies runtime configs, like stats, timeouts, exception handling, onto fn
   */
  private[similarity_engine] def getFromFn[Query, Candidate](
    fn: Query => Future[Option[Seq[Candidate]]],
    storeQuery: Query,
    engineConfig: SimilarityEngineConfig,
    params: Params,
    scopedStats: StatsReceiver
  ): Future[Option[Seq[Candidate]]] = {
    if (isEnabled(params, engineConfig.gatingConfig)) {
      scopedStats.counter("gate_enabled").incr()

      StatsUtil
        .trackOptionItemsStats(scopedStats) {
          fn.apply(storeQuery).raiseWithin(engineConfig.timeout)(timer)
        }
        .rescue {
          case _: TimeoutException | _: GlobalRequestTimeoutException | _: TApplicationException |
              _: ClientDiscardedRequestException |
              _: ServerApplicationError // TApplicationException inside
              =>
            debug("Failed to fetch. request aborted or timed out")
            Future.None
          case e =>
            error("Failed to fetch. request aborted or timed out", e)
            Future.None
        }
    } else {
      scopedStats.counter("gate_disabled").incr()
      Future.None
    }
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.SimilarityEngineInfo
import com.twitter.cr_mixer.model.TweetWithScore
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.base.Stats
import com.twitter.product_mixer.core.thriftscala.ClientContext
import com.twitter.qig_ranker.thriftscala.Product
import com.twitter.qig_ranker.thriftscala.ProductContext
import com.twitter.qig_ranker.thriftscala.QigRanker
import com.twitter.qig_ranker.thriftscala.QigRankerProductResponse
import com.twitter.qig_ranker.thriftscala.QigRankerRequest
import com.twitter.qig_ranker.thriftscala.QigRankerResponse
import com.twitter.qig_ranker.thriftscala.TwistlySimilarTweetsProductContext
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Future
import javax.inject.Singleton

/**
 * This store looks for similar tweets from QueryInteractionGraph (QIG) for a source tweet id.
 * For a given query tweet, QIG returns us the similar tweets that have an overlap of engagements
 * (with the query tweet) on different search queries
 */
@Singleton
case class TweetBasedQigSimilarityEngine(
  qigRanker: QigRanker.MethodPerEndpoint,
  statsReceiver: StatsReceiver)
    extends ReadableStore[
      TweetBasedQigSimilarityEngine.Query,
      Seq[TweetWithScore]
    ] {

  private val stats = statsReceiver.scope(this.getClass.getSimpleName)
  private val fetchCandidatesStat = stats.scope("fetchCandidates")

  override def get(
    query: TweetBasedQigSimilarityEngine.Query
  ): Future[Option[Seq[TweetWithScore]]] = {
    query.sourceId match {
      case InternalId.TweetId(tweetId) =>
        val qigSimilarTweetsRequest = getQigSimilarTweetsRequest(tweetId)

        Stats.trackOption(fetchCandidatesStat) {
          qigRanker
            .getSimilarCandidates(qigSimilarTweetsRequest)
            .map { qigSimilarTweetsResponse =>
              getCandidatesFromQigResponse(qigSimilarTweetsResponse)
            }
        }
      case _ =>
        Future.value(None)
    }
  }

  private def getQigSimilarTweetsRequest(
    tweetId: Long
  ): QigRankerRequest = {
    // Note: QigRanker needs a non-empty userId to be passed to return results.
    // We are passing in a dummy userId until we fix this on QigRanker side
    val clientContext = ClientContext(userId = Some(0L))
    val productContext = ProductContext.TwistlySimilarTweetsProductContext(
      TwistlySimilarTweetsProductContext(tweetId = tweetId))

    QigRankerRequest(
      clientContext = clientContext,
      product = Product.TwistlySimilarTweets,
      productContext = Some(productContext),
    )
  }

  private def getCandidatesFromQigResponse(
    qigSimilarTweetsResponse: QigRankerResponse
  ): Option[Seq[TweetWithScore]] = {
    qigSimilarTweetsResponse.productResponse match {
      case QigRankerProductResponse
            .TwistlySimilarTweetCandidatesResponse(response) =>
        val tweetsWithScore = response.similarTweets
          .map { similarTweetResult =>
            TweetWithScore(
              similarTweetResult.tweetResult.tweetId,
              similarTweetResult.tweetResult.score.getOrElse(0L))
          }
        Some(tweetsWithScore)

      case _ => None
    }
  }
}

object TweetBasedQigSimilarityEngine {

  def toSimilarityEngineInfo(score: Double): SimilarityEngineInfo = {
    SimilarityEngineInfo(
      similarityEngineType = SimilarityEngineType.Qig,
      modelId = None,
      score = Some(score))
  }

  case class Query(sourceId: InternalId)

  def fromParams(
    sourceId: InternalId,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    EngineQuery(
      Query(sourceId = sourceId),
      params
    )
  }

}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.model.TweetWithCandidateGenerationInfo
import com.twitter.simclusters_v2.common.TweetId
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SimilaritySourceOrderingUtil {
  /**
   * This function flatten and dedup input candidates according to the order in the input Seq
   * [[candidate10, candidate11], [candidate20, candidate21]] => [candidate10, candidate11, candidate20, candidate21]
   */
  def keepGivenOrder(
    candidates: Seq[Seq[TweetWithCandidateGenerationInfo]],
  ): Seq[TweetWithCandidateGenerationInfo] = {

    val seen = mutable.Set[TweetId]()
    val combinedCandidates = candidates.flatten
    val result = ArrayBuffer[TweetWithCandidateGenerationInfo]()

    combinedCandidates.foreach { candidate =>
      val candidateTweetId = candidate.tweetId
      val seenCandidate = seen.contains(candidateTweetId) // de-dup
      if (!seenCandidate) {
        result += candidate
        seen.add(candidate.tweetId)
      }
    }
    //convert result to immutable seq
    result.toList
  }
}
package com.twitter.cr_mixer.model

import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId

case class TweetWithAuthor(tweetId: TweetId, authorId: UserId)
package com.twitter.cr_mixer.source_signal

import com.twitter.cr_mixer.model.SourceInfo
import com.twitter.cr_mixer.source_signal.SourceFetcher.FetcherQuery
import com.twitter.cr_mixer.thriftscala.SourceType
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.util.Future

/***
 * A SourceSignalFetcher is a trait that extends from `SourceFetcher`
 * and is specialized in tackling Signals (eg., USS, FRS) fetch.
 * Currently, we define Signals as (but not limited to) a set of past engagements that
 * the user makes, such as RecentFav, RecentFollow, etc.
 *
 * The [[ResultType]] of a SourceSignalFetcher is `Seq[SourceInfo]`. When we pass in userId,
 * the underlying store returns a list of signals.
 */
trait SourceSignalFetcher extends SourceFetcher[Seq[SourceInfo]] {

  protected type SignalConvertType

  def trackStats(
    query: FetcherQuery
  )(
    func: => Future[Option[Seq[SourceInfo]]]
  ): Future[Option[Seq[SourceInfo]]] = {
    val productScopedStats = stats.scope(query.product.originalName)
    val productUserStateScopedStats = productScopedStats.scope(query.userState.toString)
    StatsUtil
      .trackOptionItemsStats(productScopedStats) {
        StatsUtil
          .trackOptionItemsStats(productUserStateScopedStats) {
            func
          }
      }
  }

  /***
   * Convert a list of Signals of type [[SignalConvertType]] into SourceInfo
   */
  def convertSourceInfo(
    sourceType: SourceType,
    signals: Seq[SignalConvertType]
  ): Seq[SourceInfo]
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.config.TimeoutConfig
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType_ModelBased
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType_RecencyBased
import com.twitter.cr_mixer.model.EarlybirdSimilarityEngineType_TensorflowBased
import com.twitter.cr_mixer.model.TweetWithAuthor
import com.twitter.cr_mixer.param.EarlybirdFrsBasedCandidateGenerationParams
import com.twitter.cr_mixer.param.EarlybirdFrsBasedCandidateGenerationParams.FrsBasedCandidateGenerationEarlybirdSimilarityEngineTypeParam
import com.twitter.cr_mixer.param.FrsParams.FrsBasedCandidateGenerationMaxCandidatesNumParam
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.simclusters_v2.common.TweetId
import com.twitter.simclusters_v2.common.UserId
import com.twitter.snowflake.id.SnowflakeId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
case class EarlybirdSimilarityEngineRouter @Inject() (
  earlybirdRecencyBasedSimilarityEngine: EarlybirdSimilarityEngine[
    EarlybirdRecencyBasedSimilarityEngine.EarlybirdRecencyBasedSearchQuery,
    EarlybirdRecencyBasedSimilarityEngine
  ],
  earlybirdModelBasedSimilarityEngine: EarlybirdSimilarityEngine[
    EarlybirdModelBasedSimilarityEngine.EarlybirdModelBasedSearchQuery,
    EarlybirdModelBasedSimilarityEngine
  ],
  earlybirdTensorflowBasedSimilarityEngine: EarlybirdSimilarityEngine[
    EarlybirdTensorflowBasedSimilarityEngine.EarlybirdTensorflowBasedSearchQuery,
    EarlybirdTensorflowBasedSimilarityEngine
  ],
  timeoutConfig: TimeoutConfig,
  statsReceiver: StatsReceiver)
    extends ReadableStore[EarlybirdSimilarityEngineRouter.Query, Seq[TweetWithAuthor]] {
  import EarlybirdSimilarityEngineRouter._

  override def get(
    k: EarlybirdSimilarityEngineRouter.Query
  ): Future[Option[Seq[TweetWithAuthor]]] = {
    k.rankingMode match {
      case EarlybirdSimilarityEngineType_RecencyBased =>
        earlybirdRecencyBasedSimilarityEngine.getCandidates(recencyBasedQueryFromParams(k))
      case EarlybirdSimilarityEngineType_ModelBased =>
        earlybirdModelBasedSimilarityEngine.getCandidates(modelBasedQueryFromParams(k))
      case EarlybirdSimilarityEngineType_TensorflowBased =>
        earlybirdTensorflowBasedSimilarityEngine.getCandidates(tensorflowBasedQueryFromParams(k))
    }
  }
}

object EarlybirdSimilarityEngineRouter {
  case class Query(
    searcherUserId: Option[UserId],
    seedUserIds: Seq[UserId],
    maxNumTweets: Int,
    excludedTweetIds: Set[TweetId],
    rankingMode: EarlybirdSimilarityEngineType,
    frsUserToScoresForScoreAdjustment: Option[Map[UserId, Double]],
    maxTweetAge: Duration,
    filterOutRetweetsAndReplies: Boolean,
    params: configapi.Params)

  def queryFromParams(
    searcherUserId: Option[UserId],
    seedUserIds: Seq[UserId],
    excludedTweetIds: Set[TweetId],
    frsUserToScoresForScoreAdjustment: Option[Map[UserId, Double]],
    params: configapi.Params
  ): Query =
    Query(
      searcherUserId,
      seedUserIds,
      maxNumTweets = params(FrsBasedCandidateGenerationMaxCandidatesNumParam),
      excludedTweetIds,
      rankingMode =
        params(FrsBasedCandidateGenerationEarlybirdSimilarityEngineTypeParam).rankingMode,
      frsUserToScoresForScoreAdjustment,
      maxTweetAge = params(
        EarlybirdFrsBasedCandidateGenerationParams.FrsBasedCandidateGenerationEarlybirdMaxTweetAge),
      filterOutRetweetsAndReplies = params(
        EarlybirdFrsBasedCandidateGenerationParams.FrsBasedCandidateGenerationEarlybirdFilterOutRetweetsAndReplies),
      params
    )

  private def recencyBasedQueryFromParams(
    query: Query
  ): EngineQuery[EarlybirdRecencyBasedSimilarityEngine.EarlybirdRecencyBasedSearchQuery] =
    EngineQuery(
      EarlybirdRecencyBasedSimilarityEngine.EarlybirdRecencyBasedSearchQuery(
        seedUserIds = query.seedUserIds,
        maxNumTweets = query.maxNumTweets,
        excludedTweetIds = query.excludedTweetIds,
        maxTweetAge = query.maxTweetAge,
        filterOutRetweetsAndReplies = query.filterOutRetweetsAndReplies
      ),
      query.params
    )

  private def tensorflowBasedQueryFromParams(
    query: Query,
  ): EngineQuery[EarlybirdTensorflowBasedSimilarityEngine.EarlybirdTensorflowBasedSearchQuery] =
    EngineQuery(
      EarlybirdTensorflowBasedSimilarityEngine.EarlybirdTensorflowBasedSearchQuery(
        searcherUserId = query.searcherUserId,
        seedUserIds = query.seedUserIds,
        maxNumTweets = query.maxNumTweets,
        // hard code the params below for now. Will move to FS after shipping the ddg
        beforeTweetIdExclusive = None,
        afterTweetIdExclusive =
          Some(SnowflakeId.firstIdFor((Time.now - query.maxTweetAge).inMilliseconds)),
        filterOutRetweetsAndReplies = query.filterOutRetweetsAndReplies,
        useTensorflowRanking = true,
        excludedTweetIds = query.excludedTweetIds,
        maxNumHitsPerShard = 1000
      ),
      query.params
    )
  private def modelBasedQueryFromParams(
    query: Query,
  ): EngineQuery[EarlybirdModelBasedSimilarityEngine.EarlybirdModelBasedSearchQuery] =
    EngineQuery(
      EarlybirdModelBasedSimilarityEngine.EarlybirdModelBasedSearchQuery(
        seedUserIds = query.seedUserIds,
        maxNumTweets = query.maxNumTweets,
        oldestTweetTimestampInSec = Some(query.maxTweetAge.ago.inSeconds),
        frsUserToScoresForScoreAdjustment = query.frsUserToScoresForScoreAdjustment
      ),
      query.params
    )
}
package com.twitter.cr_mixer.similarity_engine

import com.google.inject.Inject
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.twitter.contentrecommender.thriftscala.AlgorithmType
import com.twitter.cr_mixer.model.ModuleNames
import com.twitter.cr_mixer.model.TopicTweetWithScore
import com.twitter.cr_mixer.param.TopicTweetParams
import com.twitter.cr_mixer.similarity_engine.SkitTopicTweetSimilarityEngine._
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.frigate.common.util.StatsUtil
import com.twitter.simclusters_v2.thriftscala.EmbeddingType
import com.twitter.simclusters_v2.thriftscala.TopicId
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi
import com.twitter.topic_recos.thriftscala.TopicTweet
import com.twitter.topic_recos.thriftscala.TopicTweetPartitionFlatKey
import com.twitter.util.Future

@Singleton
case class SkitHighPrecisionTopicTweetSimilarityEngine @Inject() (
  @Named(ModuleNames.SkitStratoStoreName) skitStratoStore: ReadableStore[
    TopicTweetPartitionFlatKey,
    Seq[TopicTweet]
  ],
  statsReceiver: StatsReceiver)
    extends ReadableStore[EngineQuery[Query], Seq[TopicTweetWithScore]] {

  private val name: String = this.getClass.getSimpleName
  private val stats = statsReceiver.scope(name)

  override def get(query: EngineQuery[Query]): Future[Option[Seq[TopicTweetWithScore]]] = {
    StatsUtil.trackOptionItemsStats(stats) {
      fetch(query).map { tweets =>
        val topTweets =
          tweets
            .sortBy(-_.favCount)
            .take(query.storeQuery.maxCandidates)
            .map { tweet =>
              TopicTweetWithScore(
                tweetId = tweet.tweetId,
                score = tweet.favCount,
                similarityEngineType = SimilarityEngineType.SkitHighPrecisionTopicTweet
              )
            }
        Some(topTweets)
      }
    }
  }

  private def fetch(query: EngineQuery[Query]): Future[Seq[SkitTopicTweet]] = {
    val latestTweetTimeInHour = System.currentTimeMillis() / 1000 / 60 / 60

    val earliestTweetTimeInHour = latestTweetTimeInHour -
      math.min(MaxTweetAgeInHours, query.storeQuery.maxTweetAge.inHours)
    val timedKeys = for (timePartition <- earliestTweetTimeInHour to latestTweetTimeInHour) yield {

      TopicTweetPartitionFlatKey(
        entityId = query.storeQuery.topicId.entityId,
        timePartition = timePartition,
        algorithmType = Some(AlgorithmType.SemanticCoreTweet),
        tweetEmbeddingType = Some(EmbeddingType.LogFavBasedTweet),
        language = query.storeQuery.topicId.language.getOrElse("").toLowerCase,
        country = None, // Disable country. It is not used.
        semanticCoreAnnotationVersionId = Some(query.storeQuery.semanticCoreVersionId)
      )
    }

    getTweetsForKeys(
      timedKeys,
      query.storeQuery.topicId
    )
  }

  /**
   * Given a set of keys, multiget the underlying Strato store, combine and flatten the results.
   */
  private def getTweetsForKeys(
    keys: Seq[TopicTweetPartitionFlatKey],
    sourceTopic: TopicId
  ): Future[Seq[SkitTopicTweet]] = {
    Future
      .collect { skitStratoStore.multiGet(keys.toSet).values.toSeq }
      .map { combinedResults =>
        val topTweets = combinedResults.flatten.flatten
        topTweets.map { tweet =>
          SkitTopicTweet(
            tweetId = tweet.tweetId,
            favCount = tweet.scores.favCount.getOrElse(0L),
            cosineSimilarityScore = tweet.scores.cosineSimilarity.getOrElse(0.0),
            sourceTopic = sourceTopic
          )
        }
      }
  }
}

object SkitHighPrecisionTopicTweetSimilarityEngine {

  def fromParams(
    topicId: TopicId,
    isVideoOnly: Boolean,
    params: configapi.Params,
  ): EngineQuery[Query] = {
    val maxCandidates = if (isVideoOnly) {
      params(TopicTweetParams.MaxSkitHighPrecisionCandidatesParam) * 2
    } else {
      params(TopicTweetParams.MaxSkitHighPrecisionCandidatesParam)
    }

    EngineQuery(
      Query(
        topicId = topicId,
        maxCandidates = maxCandidates,
        maxTweetAge = params(TopicTweetParams.MaxTweetAge),
        semanticCoreVersionId = params(TopicTweetParams.SemanticCoreVersionIdParam)
      ),
      params
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.MemCacheConfig
import com.twitter.cr_mixer.similarity_engine.SimilarityEngine.SimilarityEngineConfig
import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.storehaus.ReadableStore
import com.twitter.timelines.configapi.Params
import com.twitter.util.Future

case class LookupEngineQuery[Query](
  storeQuery: Query, // the actual Query type of the underlying store
  lookupKey: String,
  params: Params,
)

/**
 * This Engine provides a map interface for looking up different model implementations.
 * It provides modelId level monitoring for free.
 *
 * Example use cases include OfflineSimClusters lookup
 *
 *
 * @param versionedStoreMap   A mapping from a modelId to a corresponding implementation
 * @param memCacheConfigOpt   If specified, it will wrap the underlying store with a MemCache layer
 *                            You should only enable this for cacheable queries, e.x. TweetIds.
 *                            consumer based UserIds are generally not possible to cache.
 */
class LookupSimilarityEngine[Query, Candidate <: Serializable](
  versionedStoreMap: Map[String, ReadableStore[Query, Seq[Candidate]]], // key = modelId
  override val identifier: SimilarityEngineType,
  globalStats: StatsReceiver,
  engineConfig: SimilarityEngineConfig,
  memCacheConfigOpt: Option[MemCacheConfig[Query]] = None)
    extends SimilarityEngine[LookupEngineQuery[Query], Candidate] {

  private val scopedStats = globalStats.scope("similarityEngine", identifier.toString)

  private val underlyingLookupMap = {
    memCacheConfigOpt match {
      case Some(config) =>
        versionedStoreMap.map {
          case (modelId, store) =>
            (
              modelId,
              SimilarityEngine.addMemCache(
                underlyingStore = store,
                memCacheConfig = config,
                keyPrefix = Some(modelId),
                statsReceiver = scopedStats
              )
            )
        }
      case _ => versionedStoreMap
    }
  }

  override def getCandidates(
    engineQuery: LookupEngineQuery[Query]
  ): Future[Option[Seq[Candidate]]] = {
    val versionedStore =
      underlyingLookupMap
        .getOrElse(
          engineQuery.lookupKey,
          throw new IllegalArgumentException(
            s"${this.getClass.getSimpleName} ${identifier.toString}: ModelId ${engineQuery.lookupKey} does not exist"
          )
        )

    SimilarityEngine.getFromFn(
      fn = versionedStore.get,
      storeQuery = engineQuery.storeQuery,
      engineConfig = engineConfig,
      params = engineQuery.params,
      scopedStats = scopedStats.scope(engineQuery.lookupKey)
    )
  }
}
package com.twitter.cr_mixer.similarity_engine

import com.twitter.cr_mixer.param.ConsumerEmbeddingBasedTwoTowerParams
import com.twitter.simclusters_v2.thriftscala.InternalId
import com.twitter.timelines.configapi

object ConsumerEmbeddingBasedTwoTowerSimilarityEngine {
  def fromParams(
    sourceId: InternalId,
    params: configapi.Params,
  ): HnswANNEngineQuery = {
    HnswANNEngineQuery(
      sourceId = sourceId,
      modelId = params(ConsumerEmbeddingBasedTwoTowerParams.ModelIdParam),
      params = params
    )
  }
}
