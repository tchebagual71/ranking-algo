package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object SimClustersANNParams {

  // Different SimClusters ANN cluster has its own config id (model slot)
  object SimClustersANNConfigId
      extends FSParam[String](
        name = "similarity_simclusters_ann_simclusters_ann_config_id",
        default = "Default"
      )

  object SimClustersANN1ConfigId
      extends FSParam[String](
        name = "similarity_simclusters_ann_simclusters_ann_1_config_id",
        default = "20220810"
      )

  object SimClustersANN2ConfigId
      extends FSParam[String](
        name = "similarity_simclusters_ann_simclusters_ann_2_config_id",
        default = "20220818"
      )

  object SimClustersANN3ConfigId
      extends FSParam[String](
        name = "similarity_simclusters_ann_simclusters_ann_3_config_id",
        default = "20220819"
      )

  object SimClustersANN5ConfigId
      extends FSParam[String](
        name = "similarity_simclusters_ann_simclusters_ann_5_config_id",
        default = "20221221"
      )
  object SimClustersANN4ConfigId
      extends FSParam[String](
        name = "similarity_simclusters_ann_simclusters_ann_4_config_id",
        default = "20221220"
      )
  object ExperimentalSimClustersANNConfigId
      extends FSParam[String](
        name = "similarity_simclusters_ann_experimental_simclusters_ann_config_id",
        default = "20220801"
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    SimClustersANNConfigId,
    SimClustersANN1ConfigId,
    SimClustersANN2ConfigId,
    SimClustersANN3ConfigId,
    SimClustersANN5ConfigId,
    ExperimentalSimClustersANNConfigId
  )

  lazy val config: BaseConfig = {
    val stringOverrides = FeatureSwitchOverrideUtil.getStringFSOverrides(
      SimClustersANNConfigId,
      SimClustersANN1ConfigId,
      SimClustersANN2ConfigId,
      SimClustersANN3ConfigId,
      SimClustersANN5ConfigId,
      ExperimentalSimClustersANNConfigId
    )

    BaseConfigBuilder()
      .set(stringOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object RankerParams {

  object MaxCandidatesToRank
      extends FSBoundedParam[Int](
        name = "twistly_core_max_candidates_to_rank",
        default = 2000,
        min = 0,
        max = 9999
      )

  object EnableBlueVerifiedTopK
      extends FSParam[Boolean](
        name = "twistly_core_blue_verified_top_k",
        default = true
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    MaxCandidatesToRank,
    EnableBlueVerifiedTopK
  )

  lazy val config: BaseConfig = {

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(EnableBlueVerifiedTopK)

    val boundedDurationFSOverrides =
      FeatureSwitchOverrideUtil.getBoundedDurationFSOverrides()

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MaxCandidatesToRank
    )

    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
    )
    val stringFSOverrides = FeatureSwitchOverrideUtil.getStringFSOverrides()

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(boundedDurationFSOverrides: _*)
      .set(intOverrides: _*)
      .set(enumOverrides: _*)
      .set(stringFSOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object RelatedTweetTweetBasedParams {

  // UTG params
  object EnableUTGParam
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_utg",
        default = false
      )

  // UVG params
  object EnableUVGParam
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_uvg",
        default = false
      )

  // UAG params
  object EnableUAGParam
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_uag",
        default = false
      )

  // SimClusters params
  object EnableSimClustersANNParam
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_simclusters",
        default = true
      )

  // Experimental SimClusters ANN params
  object EnableExperimentalSimClustersANNParam
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_experimental_simclusters_ann",
        default = false
      )

  // SimClusters ANN cluster 1 params
  object EnableSimClustersANN1Param
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_simclusters_ann_1",
        default = false
      )

  // SimClusters ANN cluster 2 params
  object EnableSimClustersANN2Param
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_simclusters_ann_2",
        default = false
      )

  // SimClusters ANN cluster 3 params
  object EnableSimClustersANN3Param
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_simclusters_ann_3",
        default = false
      )

  // SimClusters ANN cluster 5 params
  object EnableSimClustersANN5Param
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_simclusters_ann_5",
        default = false
      )

  object EnableSimClustersANN4Param
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_simclusters_ann_4",
        default = false
      )
  // TwHIN params
  object EnableTwHINParam
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_twhin",
        default = false
      )

  // QIG params
  object EnableQigSimilarTweetsParam
      extends FSParam[Boolean](
        name = "related_tweet_tweet_based_enable_qig_similar_tweets",
        default = false
      )

  // Filter params
  object SimClustersMinScoreParam
      extends FSBoundedParam[Double](
        name = "related_tweet_tweet_based_filter_simclusters_min_score",
        default = 0.3,
        min = 0.0,
        max = 1.0
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableTwHINParam,
    EnableQigSimilarTweetsParam,
    EnableUTGParam,
    EnableUVGParam,
    EnableSimClustersANNParam,
    EnableSimClustersANN2Param,
    EnableSimClustersANN3Param,
    EnableSimClustersANN5Param,
    EnableSimClustersANN4Param,
    EnableExperimentalSimClustersANNParam,
    SimClustersMinScoreParam
  )

  lazy val config: BaseConfig = {

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableTwHINParam,
      EnableQigSimilarTweetsParam,
      EnableUTGParam,
      EnableUVGParam,
      EnableSimClustersANNParam,
      EnableSimClustersANN2Param,
      EnableSimClustersANN3Param,
      EnableSimClustersANN5Param,
      EnableSimClustersANN4Param,
      EnableExperimentalSimClustersANNParam
    )

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(SimClustersMinScoreParam)

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object RelatedVideoTweetTweetBasedParams {

  // UTG params
  object EnableUTGParam
      extends FSParam[Boolean](
        name = "related_video_tweet_tweet_based_enable_utg",
        default = false
      )

  // SimClusters params
  object EnableSimClustersANNParam
      extends FSParam[Boolean](
        name = "related_video_tweet_tweet_based_enable_simclusters",
        default = true
      )

  // Experimental SimClusters ANN params
  object EnableExperimentalSimClustersANNParam
      extends FSParam[Boolean](
        name = "related_video_tweet_tweet_based_enable_experimental_simclusters_ann",
        default = false
      )

  // SimClusters ANN cluster 1 params
  object EnableSimClustersANN1Param
      extends FSParam[Boolean](
        name = "related_video_tweet_tweet_based_enable_simclusters_ann_1",
        default = false
      )

  // SimClusters ANN cluster 2 params
  object EnableSimClustersANN2Param
      extends FSParam[Boolean](
        name = "related_video_tweet_tweet_based_enable_simclusters_ann_2",
        default = false
      )

  // SimClusters ANN cluster 3 params
  object EnableSimClustersANN3Param
      extends FSParam[Boolean](
        name = "related_video_tweet_tweet_based_enable_simclusters_ann_3",
        default = false
      )

  // SimClusters ANN cluster 5 params
  object EnableSimClustersANN5Param
      extends FSParam[Boolean](
        name = "related_video_tweet_tweet_based_enable_simclusters_ann_5",
        default = false
      )

  // SimClusters ANN cluster 4 params
  object EnableSimClustersANN4Param
      extends FSParam[Boolean](
        name = "related_video_tweet_tweet_based_enable_simclusters_ann_4",
        default = false
      )
  // TwHIN params
  object EnableTwHINParam
      extends FSParam[Boolean](
        name = "related_video_tweet_tweet_based_enable_twhin",
        default = false
      )

  // QIG params
  object EnableQigSimilarTweetsParam
      extends FSParam[Boolean](
        name = "related_video_tweet_tweet_based_enable_qig_similar_tweets",
        default = false
      )

  // Filter params
  object SimClustersMinScoreParam
      extends FSBoundedParam[Double](
        name = "related_video_tweet_tweet_based_filter_simclusters_min_score",
        default = 0.3,
        min = 0.0,
        max = 1.0
      )

  object EnableUVGParam
      extends FSParam[Boolean](
        name = "related_video_tweet_tweet_based_enable_uvg",
        default = false
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableTwHINParam,
    EnableQigSimilarTweetsParam,
    EnableUTGParam,
    EnableUVGParam,
    EnableSimClustersANNParam,
    EnableSimClustersANN2Param,
    EnableSimClustersANN3Param,
    EnableSimClustersANN5Param,
    EnableSimClustersANN4Param,
    EnableExperimentalSimClustersANNParam,
    SimClustersMinScoreParam
  )

  lazy val config: BaseConfig = {

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableTwHINParam,
      EnableQigSimilarTweetsParam,
      EnableUTGParam,
      EnableUVGParam,
      EnableSimClustersANNParam,
      EnableSimClustersANN2Param,
      EnableSimClustersANN3Param,
      EnableSimClustersANN5Param,
      EnableSimClustersANN4Param,
      EnableExperimentalSimClustersANNParam
    )

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(SimClustersMinScoreParam)

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object RecentOriginalTweetsParams {

  // Source params
  object EnableSourceParam
      extends FSParam[Boolean](
        name = "twistly_recentoriginaltweets_enable_source",
        default = false
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(EnableSourceParam)

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(EnableSourceParam)

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .build()
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

object CustomizedRetrievalBasedCandidateGenerationParams {

  // Offline SimClusters InterestedIn params
  object EnableOfflineInterestedInParam
      extends FSParam[Boolean](
        name = "customized_retrieval_based_candidate_generation_enable_offline_interestedin",
        default = false
      )

  // Offline SimClusters FTR-based InterestedIn
  object EnableOfflineFTRInterestedInParam
      extends FSParam[Boolean](
        name = "customized_retrieval_based_candidate_generation_enable_ftr_offline_interestedin",
        default = false
      )

  // TwHin Collab Filter Cluster params
  object EnableTwhinCollabFilterClusterParam
      extends FSParam[Boolean](
        name = "customized_retrieval_based_candidate_generation_enable_twhin_collab_filter_cluster",
        default = false
      )

  // TwHin Multi Cluster params
  object EnableTwhinMultiClusterParam
      extends FSParam[Boolean](
        name = "customized_retrieval_based_candidate_generation_enable_twhin_multi_cluster",
        default = false
      )

  object EnableRetweetBasedDiffusionParam
      extends FSParam[Boolean](
        name = "customized_retrieval_based_candidate_generation_enable_retweet_based_diffusion",
        default = false
      )
  object CustomizedRetrievalBasedRetweetDiffusionSource
      extends FSParam[String](
        name =
          "customized_retrieval_based_candidate_generation_offline_retweet_based_diffusion_model_id",
        default = ModelConfig.RetweetBasedDiffusion
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableOfflineInterestedInParam,
    EnableOfflineFTRInterestedInParam,
    EnableTwhinCollabFilterClusterParam,
    EnableTwhinMultiClusterParam,
    EnableRetweetBasedDiffusionParam,
    CustomizedRetrievalBasedRetweetDiffusionSource
  )

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableOfflineInterestedInParam,
      EnableOfflineFTRInterestedInParam,
      EnableTwhinCollabFilterClusterParam,
      EnableTwhinMultiClusterParam,
      EnableRetweetBasedDiffusionParam
    )

    val stringFSOverrides =
      FeatureSwitchOverrideUtil.getStringFSOverrides(
        CustomizedRetrievalBasedRetweetDiffusionSource
      )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(stringFSOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object TweetBasedUserVideoGraphParams {

  object MinCoOccurrenceParam
      extends FSBoundedParam[Int](
        name = "tweet_based_user_video_graph_min_co_occurrence",
        default = 5,
        min = 0,
        max = 500
      )

  object TweetBasedMinScoreParam
      extends FSBoundedParam[Double](
        name = "tweet_based_user_video_graph_tweet_based_min_score",
        default = 0.0,
        min = 0.0,
        max = 100.0
      )

  object ConsumersBasedMinScoreParam
      extends FSBoundedParam[Double](
        name = "tweet_based_user_video_graph_consumers_based_min_score",
        default = 4.0,
        min = 0.0,
        max = 10.0
      )

  object MaxConsumerSeedsNumParam
      extends FSBoundedParam[Int](
        name = "tweet_based_user_video_graph_max_user_seeds_num",
        default = 200,
        min = 0,
        max = 500
      )

  object EnableCoverageExpansionOldTweetParam
      extends FSParam[Boolean](
        name = "tweet_based_user_video_graph_enable_coverage_expansion_old_tweet",
        default = false
      )

  object EnableCoverageExpansionAllTweetParam
      extends FSParam[Boolean](
        name = "tweet_based_user_video_graph_enable_coverage_expansion_all_tweet",
        default = false
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    MinCoOccurrenceParam,
    MaxConsumerSeedsNumParam,
    TweetBasedMinScoreParam,
    EnableCoverageExpansionOldTweetParam,
    EnableCoverageExpansionAllTweetParam
  )

  lazy val config: BaseConfig = {

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MinCoOccurrenceParam,
      MaxConsumerSeedsNumParam
    )

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(TweetBasedMinScoreParam)

    BaseConfigBuilder()
      .set(intOverrides: _*)
      .set(doubleOverrides: _*)
      .build()
  }

}
package com.twitter.cr_mixer.param

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSEnumParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object BlenderParams {
  object BlendingAlgorithmEnum extends Enumeration {
    val RoundRobin: Value = Value
    val SourceTypeBackFill: Value = Value
    val SourceSignalSorting: Value = Value
  }
  object ContentBasedSortingAlgorithmEnum extends Enumeration {
    val FavoriteCount: Value = Value
    val SourceSignalRecency: Value = Value
    val RandomSorting: Value = Value
    val SimilarityToSignalSorting: Value = Value
    val CandidateRecency: Value = Value
  }

  object BlendingAlgorithmParam
      extends FSEnumParam[BlendingAlgorithmEnum.type](
        name = "blending_algorithm_id",
        default = BlendingAlgorithmEnum.RoundRobin,
        enum = BlendingAlgorithmEnum
      )

  object RankingInterleaveWeightShrinkageParam
      extends FSBoundedParam[Double](
        name = "blending_enable_ml_ranking_interleave_weights_shrinkage",
        default = 1.0,
        min = 0.0,
        max = 1.0
      )

  object RankingInterleaveMaxWeightAdjustments
      extends FSBoundedParam[Int](
        name = "blending_interleave_max_weighted_adjustments",
        default = 3000,
        min = 0,
        max = 9999
      )

  object SignalTypeSortingAlgorithmParam
      extends FSEnumParam[ContentBasedSortingAlgorithmEnum.type](
        name = "blending_algorithm_inner_signal_sorting_id",
        default = ContentBasedSortingAlgorithmEnum.SourceSignalRecency,
        enum = ContentBasedSortingAlgorithmEnum
      )

  object ContentBlenderTypeSortingAlgorithmParam
      extends FSEnumParam[ContentBasedSortingAlgorithmEnum.type](
        name = "blending_algorithm_content_blender_sorting_id",
        default = ContentBasedSortingAlgorithmEnum.FavoriteCount,
        enum = ContentBasedSortingAlgorithmEnum
      )

  //UserAffinities Algo Param: whether to distributed the source type weights
  object EnableDistributedSourceTypeWeightsParam
      extends FSParam[Boolean](
        name = "blending_algorithm_enable_distributed_source_type_weights",
        default = false
      )

  object BlendGroupingMethodEnum extends Enumeration {
    val SourceKeyDefault: Value = Value("SourceKey")
    val SourceTypeSimilarityEngine: Value = Value("SourceTypeSimilarityEngine")
    val AuthorId: Value = Value("AuthorId")
  }

  object BlendGroupingMethodParam
      extends FSEnumParam[BlendGroupingMethodEnum.type](
        name = "blending_grouping_method_id",
        default = BlendGroupingMethodEnum.SourceKeyDefault,
        enum = BlendGroupingMethodEnum
      )

  object RecencyBasedRandomSamplingHalfLifeInDays
      extends FSBoundedParam[Int](
        name = "blending_interleave_random_sampling_recency_based_half_life_in_days",
        default = 7,
        min = 1,
        max = 28
      )

  object RecencyBasedRandomSamplingDefaultWeight
      extends FSBoundedParam[Double](
        name = "blending_interleave_random_sampling_recency_based_default_weight",
        default = 1.0,
        min = 0.1,
        max = 2.0
      )

  object SourceTypeBackFillEnableVideoBackFill
      extends FSParam[Boolean](
        name = "blending_enable_video_backfill",
        default = false
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    BlendingAlgorithmParam,
    RankingInterleaveWeightShrinkageParam,
    RankingInterleaveMaxWeightAdjustments,
    EnableDistributedSourceTypeWeightsParam,
    BlendGroupingMethodParam,
    RecencyBasedRandomSamplingHalfLifeInDays,
    RecencyBasedRandomSamplingDefaultWeight,
    SourceTypeBackFillEnableVideoBackFill,
    SignalTypeSortingAlgorithmParam,
    ContentBlenderTypeSortingAlgorithmParam,
  )

  lazy val config: BaseConfig = {
    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
      BlendingAlgorithmParam,
      BlendGroupingMethodParam,
      SignalTypeSortingAlgorithmParam,
      ContentBlenderTypeSortingAlgorithmParam
    )

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableDistributedSourceTypeWeightsParam,
      SourceTypeBackFillEnableVideoBackFill
    )

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      RankingInterleaveMaxWeightAdjustments,
      RecencyBasedRandomSamplingHalfLifeInDays
    )

    val doubleOverrides = FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(
      RankingInterleaveWeightShrinkageParam,
      RecencyBasedRandomSamplingDefaultWeight
    )

    BaseConfigBuilder()
      .set(enumOverrides: _*)
      .set(booleanOverrides: _*)
      .set(intOverrides: _*)
      .set(doubleOverrides: _*)
      .build()
  }
}
package com.twitter.cr_mixer.param

import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object ConsumerEmbeddingBasedTripParams {
  object SourceIdParam
      extends FSParam[String](
        name = "consumer_embedding_based_trip_source_id",
        default = "EXPLR_TOPK_VID_48H_V3")

  object MaxNumCandidatesParam
      extends FSBoundedParam[Int](
        name = "consumer_embedding_based_trip_max_num_candidates",
        default = 80,
        min = 0,
        max = 200
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    SourceIdParam,
    MaxNumCandidatesParam
  )

  lazy val config: BaseConfig = {
    val stringFSOverrides =
      FeatureSwitchOverrideUtil.getStringFSOverrides(
        SourceIdParam
      )

    val intFSOverrides =
      FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
        MaxNumCandidatesParam
      )

    BaseConfigBuilder()
      .set(stringFSOverrides: _*)
      .set(intFSOverrides: _*)
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

object RelatedTweetGlobalParams {

  object MaxCandidatesPerRequestParam
      extends FSBoundedParam[Int](
        name = "related_tweet_core_max_candidates_per_request",
        default = 100,
        min = 0,
        max = 500
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(MaxCandidatesPerRequestParam)

  lazy val config: BaseConfig = {

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      MaxCandidatesPerRequestParam
    )

    BaseConfigBuilder()
      .set(intOverrides: _*)
      .build()
  }
}
