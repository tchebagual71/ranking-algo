package com.twitter.cr_mixer.model

import com.twitter.cr_mixer.thriftscala.SimilarityEngineType
import com.twitter.simclusters_v2.common.TweetId

/***
 * Bind a tweetId with a raw score generated from one single Similarity Engine
 * @param similarityEngineType, which underlying topic source the topic tweet is from
 */
case class TopicTweetWithScore(
  tweetId: TweetId,
  score: Double,
  similarityEngineType: SimilarityEngineType)
package com.twitter.cr_mixer.param

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
import com.twitter.simclusters_v2.thriftscala.{EmbeddingType => SimClustersEmbeddingType}
import com.twitter.timelines.configapi.BaseConfig
import com.twitter.timelines.configapi.BaseConfigBuilder
import com.twitter.timelines.configapi.FSBoundedParam
import com.twitter.timelines.configapi.FSEnumParam
import com.twitter.timelines.configapi.FSName
import com.twitter.timelines.configapi.FSParam
import com.twitter.timelines.configapi.FeatureSwitchOverrideUtil
import com.twitter.timelines.configapi.Param

object InterestedInParams {

  object SourceEmbedding extends Enumeration {
    protected case class EmbeddingType(embeddingType: SimClustersEmbeddingType) extends super.Val
    import scala.language.implicitConversions
    implicit def valueToEmbeddingtype(x: Value): EmbeddingType = x.asInstanceOf[EmbeddingType]

    val UserInterestedIn: Value = EmbeddingType(SimClustersEmbeddingType.FilteredUserInterestedIn)
    val UnfilteredUserInterestedIn: Value = EmbeddingType(
      SimClustersEmbeddingType.UnfilteredUserInterestedIn)
    val FromProducerEmbedding: Value = EmbeddingType(
      SimClustersEmbeddingType.FilteredUserInterestedInFromPE)
    val LogFavBasedUserInterestedInFromAPE: Value = EmbeddingType(
      SimClustersEmbeddingType.LogFavBasedUserInterestedInFromAPE)
    val FollowBasedUserInterestedInFromAPE: Value = EmbeddingType(
      SimClustersEmbeddingType.FollowBasedUserInterestedInFromAPE)
    val UserNextInterestedIn: Value = EmbeddingType(SimClustersEmbeddingType.UserNextInterestedIn)
    // AddressBook based InterestedIn
    val LogFavBasedUserInterestedAverageAddressBookFromIIAPE: Value = EmbeddingType(
      SimClustersEmbeddingType.LogFavBasedUserInterestedAverageAddressBookFromIIAPE)
    val LogFavBasedUserInterestedMaxpoolingAddressBookFromIIAPE: Value = EmbeddingType(
      SimClustersEmbeddingType.LogFavBasedUserInterestedMaxpoolingAddressBookFromIIAPE)
    val LogFavBasedUserInterestedBooktypeMaxpoolingAddressBookFromIIAPE: Value = EmbeddingType(
      SimClustersEmbeddingType.LogFavBasedUserInterestedBooktypeMaxpoolingAddressBookFromIIAPE)
    val LogFavBasedUserInterestedLargestDimMaxpoolingAddressBookFromIIAPE: Value = EmbeddingType(
      SimClustersEmbeddingType.LogFavBasedUserInterestedLargestDimMaxpoolingAddressBookFromIIAPE)
    val LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE: Value = EmbeddingType(
      SimClustersEmbeddingType.LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE)
    val LogFavBasedUserInterestedConnectedMaxpoolingAddressBookFromIIAPE: Value = EmbeddingType(
      SimClustersEmbeddingType.LogFavBasedUserInterestedConnectedMaxpoolingAddressBookFromIIAPE)
  }

  object EnableSourceParam
      extends FSParam[Boolean](
        name = "twistly_interestedin_enable_source",
        default = true
      )

  object InterestedInEmbeddingIdParam
      extends FSEnumParam[SourceEmbedding.type](
        name = "twistly_interestedin_embedding_id",
        default = SourceEmbedding.UnfilteredUserInterestedIn,
        enum = SourceEmbedding
      )

  object MinScoreParam
      extends FSBoundedParam[Double](
        name = "twistly_interestedin_min_score",
        default = 0.072,
        min = 0.0,
        max = 1.0
      )

  object EnableSourceSequentialModelParam
      extends FSParam[Boolean](
        name = "twistly_interestedin_sequential_model_enable_source",
        default = false
      )

  object NextInterestedInEmbeddingIdParam
      extends FSEnumParam[SourceEmbedding.type](
        name = "twistly_interestedin_sequential_model_embedding_id",
        default = SourceEmbedding.UserNextInterestedIn,
        enum = SourceEmbedding
      )

  object MinScoreSequentialModelParam
      extends FSBoundedParam[Double](
        name = "twistly_interestedin_sequential_model_min_score",
        default = 0.0,
        min = 0.0,
        max = 1.0
      )

  object EnableSourceAddressBookParam
      extends FSParam[Boolean](
        name = "twistly_interestedin_addressbook_enable_source",
        default = false
      )

  object AddressBookInterestedInEmbeddingIdParam
      extends FSEnumParam[SourceEmbedding.type](
        name = "twistly_interestedin_addressbook_embedding_id",
        default = SourceEmbedding.LogFavBasedUserInterestedLouvainMaxpoolingAddressBookFromIIAPE,
        enum = SourceEmbedding
      )

  object MinScoreAddressBookParam
      extends FSBoundedParam[Double](
        name = "twistly_interestedin_addressbook_min_score",
        default = 0.0,
        min = 0.0,
        max = 1.0
      )

  // Prod SimClusters ANN param
  // This is used to enable/disable querying of production SANN service. Useful when experimenting
  // with replacements to it.
  object EnableProdSimClustersANNParam
      extends FSParam[Boolean](
        name = "twistly_interestedin_enable_prod_simclusters_ann",
        default = true
      )

  // Experimental SimClusters ANN params
  object EnableExperimentalSimClustersANNParam
      extends FSParam[Boolean](
        name = "twistly_interestedin_enable_experimental_simclusters_ann",
        default = false
      )

  // SimClusters ANN 1 cluster params
  object EnableSimClustersANN1Param
      extends FSParam[Boolean](
        name = "twistly_interestedin_enable_simclusters_ann_1",
        default = false
      )

  // SimClusters ANN 2 cluster params
  object EnableSimClustersANN2Param
      extends FSParam[Boolean](
        name = "twistly_interestedin_enable_simclusters_ann_2",
        default = false
      )

  // SimClusters ANN 3 cluster params
  object EnableSimClustersANN3Param
      extends FSParam[Boolean](
        name = "twistly_interestedin_enable_simclusters_ann_3",
        default = false
      )

  // SimClusters ANN 5 cluster params
  object EnableSimClustersANN5Param
      extends FSParam[Boolean](
        name = "twistly_interestedin_enable_simclusters_ann_5",
        default = false
      )

  // SimClusters ANN 4 cluster params
  object EnableSimClustersANN4Param
      extends FSParam[Boolean](
        name = "twistly_interestedin_enable_simclusters_ann_4",
        default = false
      )
  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableSourceParam,
    EnableSourceSequentialModelParam,
    EnableSourceAddressBookParam,
    EnableProdSimClustersANNParam,
    EnableExperimentalSimClustersANNParam,
    EnableSimClustersANN1Param,
    EnableSimClustersANN2Param,
    EnableSimClustersANN3Param,
    EnableSimClustersANN5Param,
    EnableSimClustersANN4Param,
    MinScoreParam,
    MinScoreSequentialModelParam,
    MinScoreAddressBookParam,
    InterestedInEmbeddingIdParam,
    NextInterestedInEmbeddingIdParam,
    AddressBookInterestedInEmbeddingIdParam,
  )

  lazy val config: BaseConfig = {

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam,
      EnableSourceSequentialModelParam,
      EnableSourceAddressBookParam,
      EnableProdSimClustersANNParam,
      EnableExperimentalSimClustersANNParam,
      EnableSimClustersANN1Param,
      EnableSimClustersANN2Param,
      EnableSimClustersANN3Param,
      EnableSimClustersANN5Param,
      EnableSimClustersANN4Param
    )

    val doubleOverrides = FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(
      MinScoreParam,
      MinScoreSequentialModelParam,
      MinScoreAddressBookParam)

    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
      InterestedInEmbeddingIdParam,
      NextInterestedInEmbeddingIdParam,
      AddressBookInterestedInEmbeddingIdParam
    )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .set(enumOverrides: _*)
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

object CustomizedRetrievalBasedFTROfflineInterestedInParams {
  object CustomizedRetrievalBasedFTROfflineInterestedInSource
      extends FSParam[String](
        name = "customized_retrieval_based_ftr_offline_interestedin_model_id",
        default = ModelConfig.OfflineFavDecayedSum
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    CustomizedRetrievalBasedFTROfflineInterestedInSource)

  lazy val config: BaseConfig = {

    val stringFSOverrides =
      FeatureSwitchOverrideUtil.getStringFSOverrides(
        CustomizedRetrievalBasedFTROfflineInterestedInSource
      )

    BaseConfigBuilder()
      .set(stringFSOverrides: _*)
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

object RecentRetweetsParams {

  // Source params
  object EnableSourceParam
      extends FSParam[Boolean](
        name = "twistly_recentretweets_enable_source",
        default = false
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(EnableSourceParam)

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam
    )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
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
import com.twitter.usersignalservice.thriftscala.SignalType
import scala.language.implicitConversions

object UnifiedUSSSignalParams {

  object TweetAggregationTypeParam extends Enumeration {
    protected case class SignalTypeValue(signalType: SignalType) extends super.Val

    implicit def valueToSignalTypeValue(x: Value): SignalTypeValue =
      x.asInstanceOf[SignalTypeValue]

    val UniformAggregation = SignalTypeValue(SignalType.TweetBasedUnifiedUniformSignal)
    val EngagementAggregation = SignalTypeValue(
      SignalType.TweetBasedUnifiedEngagementWeightedSignal)
  }

  object ProducerAggregationTypeParam extends Enumeration {
    protected case class SignalTypeValue(signalType: SignalType) extends super.Val

    import scala.language.implicitConversions

    implicit def valueToSignalTypeValue(x: Value): SignalTypeValue =
      x.asInstanceOf[SignalTypeValue]

    val UniformAggregation = SignalTypeValue(SignalType.ProducerBasedUnifiedUniformSignal)
    val EngagementAggregation = SignalTypeValue(
      SignalType.ProducerBasedUnifiedEngagementWeightedSignal)

  }

  object ReplaceIndividualUSSSourcesParam
      extends FSParam[Boolean](
        name = "twistly_agg_replace_enable_source",
        default = false
      )

  object EnableTweetAggSourceParam
      extends FSParam[Boolean](
        name = "twistly_agg_tweet_agg_enable_source",
        default = false
      )

  object TweetAggTypeParam
      extends FSEnumParam[TweetAggregationTypeParam.type](
        name = "twistly_agg_tweet_agg_type_id",
        default = TweetAggregationTypeParam.EngagementAggregation,
        enum = TweetAggregationTypeParam
      )

  object UnifiedTweetSourceNumberParam
      extends FSBoundedParam[Int](
        name = "twistly_agg_tweet_agg_source_number",
        default = 0,
        min = 0,
        max = 100,
      )

  object EnableProducerAggSourceParam
      extends FSParam[Boolean](
        name = "twistly_agg_producer_agg_enable_source",
        default = false
      )

  object ProducerAggTypeParam
      extends FSEnumParam[ProducerAggregationTypeParam.type](
        name = "twistly_agg_producer_agg_type_id",
        default = ProducerAggregationTypeParam.EngagementAggregation,
        enum = ProducerAggregationTypeParam
      )

  object UnifiedProducerSourceNumberParam
      extends FSBoundedParam[Int](
        name = "twistly_agg_producer_agg_source_number",
        default = 0,
        min = 0,
        max = 100,
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableTweetAggSourceParam,
    EnableProducerAggSourceParam,
    TweetAggTypeParam,
    ProducerAggTypeParam,
    UnifiedTweetSourceNumberParam,
    UnifiedProducerSourceNumberParam,
    ReplaceIndividualUSSSourcesParam
  )
  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableTweetAggSourceParam,
      EnableProducerAggSourceParam,
      ReplaceIndividualUSSSourcesParam,
    )
    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(
      UnifiedProducerSourceNumberParam,
      UnifiedTweetSourceNumberParam)
    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
      TweetAggTypeParam,
      ProducerAggTypeParam
    )

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(intOverrides: _*)
      .set(enumOverrides: _*)
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

object ConsumersBasedUserAdGraphParams {

  object EnableSourceParam
      extends FSParam[Boolean](
        name = "consumers_based_user_ad_graph_enable_source",
        default = false
      )

  // UTG-Lookalike
  object MinCoOccurrenceParam
      extends FSBoundedParam[Int](
        name = "consumers_based_user_ad_graph_min_co_occurrence",
        default = 2,
        min = 0,
        max = 500
      )

  object MinScoreParam
      extends FSBoundedParam[Double](
        name = "consumers_based_user_ad_graph_min_score",
        default = 0.0,
        min = 0.0,
        max = 10.0
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableSourceParam,
    MinCoOccurrenceParam,
    MinScoreParam
  )

  lazy val config: BaseConfig = {

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(MinCoOccurrenceParam)
    val doubleOverrides = FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(MinScoreParam)
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(EnableSourceParam)

    BaseConfigBuilder()
      .set(intOverrides: _*)
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

object RelatedTweetProducerBasedParams {

  // UTG params
  object EnableUTGParam
      extends FSParam[Boolean](
        name = "related_tweet_producer_based_enable_utg",
        default = false
      )

  // SimClusters params
  object EnableSimClustersANNParam
      extends FSParam[Boolean](
        name = "related_tweet_producer_based_enable_simclusters",
        default = true
      )

  // Filter params
  object SimClustersMinScoreParam
      extends FSBoundedParam[Double](
        name = "related_tweet_producer_based_filter_simclusters_min_score",
        default = 0.0,
        min = 0.0,
        max = 1.0
      )

  // Experimental SimClusters ANN params
  object EnableExperimentalSimClustersANNParam
      extends FSParam[Boolean](
        name = "related_tweet_producer_based_enable_experimental_simclusters_ann",
        default = false
      )

  // SimClusters ANN cluster 1 params
  object EnableSimClustersANN1Param
      extends FSParam[Boolean](
        name = "related_tweet_producer_based_enable_simclusters_ann_1",
        default = false
      )

  // SimClusters ANN cluster 2 params
  object EnableSimClustersANN2Param
      extends FSParam[Boolean](
        name = "related_tweet_producer_based_enable_simclusters_ann_2",
        default = false
      )

  // SimClusters ANN cluster 3 params
  object EnableSimClustersANN3Param
      extends FSParam[Boolean](
        name = "related_tweet_producer_based_enable_simclusters_ann_3",
        default = false
      )

  // SimClusters ANN cluster 3 params
  object EnableSimClustersANN5Param
      extends FSParam[Boolean](
        name = "related_tweet_producer_based_enable_simclusters_ann_5",
        default = false
      )

  // SimClusters ANN cluster 4 params
  object EnableSimClustersANN4Param
      extends FSParam[Boolean](
        name = "related_tweet_producer_based_enable_simclusters_ann_4",
        default = false
      )
  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableUTGParam,
    EnableSimClustersANNParam,
    EnableSimClustersANN1Param,
    EnableSimClustersANN2Param,
    EnableSimClustersANN3Param,
    EnableSimClustersANN5Param,
    EnableSimClustersANN4Param,
    EnableExperimentalSimClustersANNParam,
    SimClustersMinScoreParam
  )

  lazy val config: BaseConfig = {

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableUTGParam,
      EnableSimClustersANNParam,
      EnableSimClustersANN1Param,
      EnableSimClustersANN2Param,
      EnableSimClustersANN3Param,
      EnableSimClustersANN5Param,
      EnableSimClustersANN4Param,
      EnableExperimentalSimClustersANNParam
    )

    val doubleOverrides = FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(
      SimClustersMinScoreParam
    )

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

object RealGraphOonParams {
  object EnableSourceParam
      extends FSParam[Boolean](
        name = "signal_realgraphoon_enable_source",
        default = false
      )

  object EnableSourceGraphParam
      extends FSParam[Boolean](
        name = "graph_realgraphoon_enable_source",
        default = false
      )

  object MaxConsumerSeedsNumParam
      extends FSBoundedParam[Int](
        name = "graph_realgraphoon_max_user_seeds_num",
        default = 200,
        min = 0,
        max = 1000
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableSourceParam,
    EnableSourceGraphParam,
    MaxConsumerSeedsNumParam
  )

  lazy val config: BaseConfig = {
    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam,
      EnableSourceGraphParam
    )

    val intOverrides = FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(MaxConsumerSeedsNumParam)

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(intOverrides: _*)
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

object ProducerBasedCandidateGenerationParams {
  // Source params. Not being used. It is always set to true in prod
  object EnableSourceParam
      extends FSParam[Boolean](
        name = "producer_based_candidate_generation_enable_source",
        default = false
      )

  object UtgCombinationMethodParam
      extends FSEnumParam[UnifiedSETweetCombinationMethod.type](
        name = "producer_based_candidate_generation_utg_combination_method_id",
        default = UnifiedSETweetCombinationMethod.Frontload,
        enum = UnifiedSETweetCombinationMethod
      )

  // UTG params
  object EnableUTGParam
      extends FSParam[Boolean](
        name = "producer_based_candidate_generation_enable_utg",
        default = false
      )

  object EnableUAGParam
      extends FSParam[Boolean](
        name = "producer_based_candidate_generation_enable_uag",
        default = false
      )

  // SimClusters params
  object EnableSimClustersANNParam
      extends FSParam[Boolean](
        name = "producer_based_candidate_generation_enable_simclusters",
        default = true
      )

  // Filter params
  object SimClustersMinScoreParam
      extends FSBoundedParam[Double](
        name = "producer_based_candidate_generation_filter_simclusters_min_score",
        default = 0.7,
        min = 0.0,
        max = 1.0
      )

  // Experimental SimClusters ANN params
  object EnableExperimentalSimClustersANNParam
      extends FSParam[Boolean](
        name = "producer_based_candidate_generation_enable_experimental_simclusters_ann",
        default = false
      )

  // SimClusters ANN cluster 1 params
  object EnableSimClustersANN1Param
      extends FSParam[Boolean](
        name = "producer_based_candidate_generation_enable_simclusters_ann_1",
        default = false
      )

  // SimClusters ANN cluster 2 params
  object EnableSimClustersANN2Param
      extends FSParam[Boolean](
        name = "producer_based_candidate_generation_enable_simclusters_ann_2",
        default = false
      )

  // SimClusters ANN cluster 3 params
  object EnableSimClustersANN3Param
      extends FSParam[Boolean](
        name = "producer_based_candidate_generation_enable_simclusters_ann_3",
        default = false
      )

  // SimClusters ANN cluster 5 params
  object EnableSimClustersANN5Param
      extends FSParam[Boolean](
        name = "producer_based_candidate_generation_enable_simclusters_ann_5",
        default = false
      )

  object EnableSimClustersANN4Param
      extends FSParam[Boolean](
        name = "producer_based_candidate_generation_enable_simclusters_ann_4",
        default = false
      )
  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableSourceParam,
    EnableUAGParam,
    EnableUTGParam,
    EnableSimClustersANNParam,
    EnableSimClustersANN1Param,
    EnableSimClustersANN2Param,
    EnableSimClustersANN3Param,
    EnableSimClustersANN5Param,
    EnableSimClustersANN4Param,
    EnableExperimentalSimClustersANNParam,
    SimClustersMinScoreParam,
    UtgCombinationMethodParam
  )

  lazy val config: BaseConfig = {

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam,
      EnableUAGParam,
      EnableUTGParam,
      EnableSimClustersANNParam,
      EnableSimClustersANN1Param,
      EnableSimClustersANN2Param,
      EnableSimClustersANN3Param,
      EnableSimClustersANN5Param,
      EnableSimClustersANN4Param,
      EnableExperimentalSimClustersANNParam
    )

    val enumOverrides = FeatureSwitchOverrideUtil.getEnumFSOverrides(
      NullStatsReceiver,
      Logger(getClass),
      UtgCombinationMethodParam,
    )

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(SimClustersMinScoreParam)

    BaseConfigBuilder()
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .set(enumOverrides: _*)
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

/**
 * ConsumersBasedUserVideoGraph Params: there are multiple ways (e.g. FRS, RealGraphIn) to generate consumersSeedSet for ConsumersBasedUserTweetGraph
 * for now we allow flexibility in tuning UVG params for different consumersSeedSet generation algo by giving the param name {consumerSeedSetAlgo}{ParamName}
 */

object ConsumersBasedUserVideoGraphParams {

  object EnableSourceParam
      extends FSParam[Boolean](
        name = "consumers_based_user_video_graph_enable_source",
        default = false
      )

  // UTG-RealGraphIN
  object RealGraphInMinCoOccurrenceParam
      extends FSBoundedParam[Int](
        name = "consumers_based_user_video_graph_real_graph_in_min_co_occurrence",
        default = 3,
        min = 0,
        max = 500
      )

  object RealGraphInMinScoreParam
      extends FSBoundedParam[Double](
        name = "consumers_based_user_video_graph_real_graph_in_min_score",
        default = 2.0,
        min = 0.0,
        max = 10.0
      )

  val AllParams: Seq[Param[_] with FSName] = Seq(
    EnableSourceParam,
    RealGraphInMinCoOccurrenceParam,
    RealGraphInMinScoreParam
  )

  lazy val config: BaseConfig = {

    val intOverrides =
      FeatureSwitchOverrideUtil.getBoundedIntFSOverrides(RealGraphInMinCoOccurrenceParam)

    val doubleOverrides =
      FeatureSwitchOverrideUtil.getBoundedDoubleFSOverrides(RealGraphInMinScoreParam)

    val booleanOverrides = FeatureSwitchOverrideUtil.getBooleanFSOverrides(
      EnableSourceParam
    )

    BaseConfigBuilder()
      .set(intOverrides: _*)
      .set(booleanOverrides: _*)
      .set(doubleOverrides: _*)
      .build()
  }
}
