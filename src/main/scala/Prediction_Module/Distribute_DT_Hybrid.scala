package Prediction_Module

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.collection.mutable


/**
 * The aim of DT_Hybrid algorithm is to predict the goodness of the spot1-spot2 relationship through a bipartite
 * network. Such network is composed of two kind of nodes: Documents, Spots (Terms).
 * To get the the spot1-spot2 relationship score, we build the following DataFrames at first:
 *   1. pmids_wids_df:   contains all the pmid-wid associations
 *   2. pmids_1ONdeg_df: contains the reciprocal values of the pmids' degree
 *   3. wids_1ONdeg_df:  contains the reciprocal values of the wikis' degree
 * As soon as the DataFrames have been built, the pipeline builds our W_DataFrame (contains the score of each rel-
 * ationship) through the following operations:
 *   1. pmids_wids DataFrame is joined to itself in order to get pmid-wid1-wid2 associations
 *   2. pmid-wid1-wid2 DataFrame is joined with pmid_1ONdeg one to associate to each pmid its own reciprocal degree
 *   3. a groupBy on wid1-wid2 is executed to calculate the contribute that ti-tj couple receives from the documents
 *   4. wid1-wid2-gamma(pmid) DataFrame is joined with wids_1ONdeg one to calculate the contribute that ti-tj couple
 *      receives from the term ti
 *   5. wid1-wid2-gamma(pmid)-gamma(ti) DataFrame is joined with wids_1ONdeg one to calculate the contribute that ti-tj
 *      couple receives from the term tj
 *   6. the last selecting operation builds a DataFrame contains the biotagme scores for each ti-tj association
 **/
object Distribute_DT_Hybrid {

    def dt_hybrid_manager(spark: SparkSession, conf_map: mutable.Map[String, Any]): Unit = {
        import spark.implicits._

        val w_matrix_paths   = conf_map("hdfs_paths").asInstanceOf[mutable.Map[String, String]]
        val dt_hybrid_params = conf_map("dt_hybrid_parameters").asInstanceOf[mutable.Map[String, String]]
        val alpha: Double    = dt_hybrid_params("alpha").toDouble

        //1.pmids_wids_df
        val pmids_wids   = spark.read.parquet(w_matrix_paths("DocumentsAnnotations_path") + "_filtered/*.parquet")
            .select($"pmid", $"wiki_id").persist()

        //2.pmids_1ONdeg_df
        val pmids_1ONdeg = pmids_wids.groupBy("pmid").agg(countDistinct($"wiki_id").as("deg"))
            .select($"pmid", (lit(1.0)/$"deg").as("1_on_degP"))

        //3. wids_1ONdeg_df
        val wid_1onDeg   = pmids_wids.groupBy("wiki_id").agg(countDistinct($"pmid").as("deg"))
            .select($"wiki_id", (lit(1.0)/$"deg").as("1_on_degW"))

        //W_DataFrame
        val norm_wind    = Window.orderBy("wid1").partitionBy("wid1")
        val w_DataFrame  = pmids_wids.withColumnRenamed("wiki_id", "wid1")
            .join(pmids_wids.withColumnRenamed("wiki_id", "wid2"), "pmid")
            .join(pmids_1ONdeg, "pmid")
            .groupBy("wid1","wid2").agg(sum($"1_on_degP").as("gamma(pmid)"))
            .join(broadcast(wid_1onDeg).select($"wiki_id".as("wid1"), pow($"1_on_degW",alpha).as("gamma(t1)")),"wid1")
            .join(broadcast(wid_1onDeg).select($"wiki_id".as("wid2"), pow($"1_on_degW",1-alpha).as("gamma(t2)")),"wid2")
            .select($"wid1", $"wid2", ($"gamma(pmid)" * $"gamma(t1)" * $"gamma(t2)").as("w"))
            .withColumn("w_max", max($"w").over(norm_wind))
            .select($"wid1", $"wid2", ($"w"/$"w_max").as("w_norm"))
            .where($"wid1" =!= $"wid2").repartitionByRange(col("wid1"))

        //Saving DataFrame
        w_DataFrame.write.mode("overwrite").parquet(w_matrix_paths("w_matrix_path"))
        pmids_wids.unpersist()
    }
}
