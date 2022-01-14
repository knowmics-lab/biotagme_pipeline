package Prediction_Module

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable



object Intermediate_dataframe_creation {
    /**
    * The updateDataFrame function is used to update the old wikiId_spot and wid1_wid2_freq DataFrame
    * when the updating procedure is triggered.
    * NOTE:
    *   If id_df opt is set to one, the directory name containing wid1_wid2_freq DataFrame has to be
    *   change in old_name + _tmp. Such operation is necessary because you can not overwrite a file
    *   when you are read it.
    **/
    def updateDataFrame(spark: SparkSession, root_pat:String, fs:FileSystem, id_df: Int):Unit = {
        if (id_df == 1) fs.rename(new Path(root_pat), new Path(root_pat + "_tmp"))

        /* DataFrames loading */
        val old_df = spark.read.parquet(root_pat + (if (id_df == 1) "_tmp" else "") + "/*.parquet")
        val new_df = spark.read.parquet(root_pat + "1/*.parquet")

        id_df match {
                 case 1 => // update the frequency value
                      new_df.union(old_df)
                            .groupBy("wid1", "wid2").agg(sum("freq").as("freq"))
                            .write.mode("overwrite").parquet(root_pat)
                      fs.delete(new Path(root_pat + "1"), true)
                      fs.delete(new Path(root_pat + "_tmp"), true)
                 case 2 =>
                      new_df.join(old_df, Seq("wiki_id", "spot"), "leftanti")
                            .write.mode("append").parquet(root_pat)
                      fs.delete(new Path(root_pat + "1"), true)
                 case 3 =>
                      new_df.join(old_df, Seq("wid1", "wid2", "pmid"), "leftanti")
                            .write.mode("append").parquet(root_pat)
                      fs.delete(new Path(root_pat + "1"), true)
        }
    }

    /**
     * The intermediate_df_creation function allows to generate the followings DataFrames:
     *    1. wikiId_spot: contains the wiki_id-spot name associations
     *    2. wid1_wid2_freq: contains all the wid1-wid2 relationships as well as the count of these in the documents.
     * The DataFrame doc_annotate contains either all documents annotations (if opt = 1) or the new documents ones
     * (if opt = 0) is loaded at first. Then, the doc_annotate is joined to itself in order to get all possible
     * pmid-wid1-wid2 relationship. At this point, a group by on wid1-wid2 is triggered so that the frequency of
     * this pairs are obtained. Finally the 1.,2. DataFrames are built through the appropriate selecting operation
     * and stored within the hadoop filesystem.
     **/
    def intermediate_df_creation(spark: SparkSession, conf_map: mutable.Map[String, Any], opt: Int):Unit = {
        import spark.implicits._

        val logger = Logger.getLogger("Intermediate DataFrames creation")
        val fs     = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val paths  = conf_map("hdfs_paths").asInstanceOf[mutable.Map[String, String]]

        logger.info("Reading of the DataFrame containing the documents annotations...")
        val path_docs_ann = paths("DocumentsAnnotations_path") + "_filtered" + (if(opt == 0) "1" else "")
        val doc_annotate  = spark.read.parquet(path_docs_ann + "/*.parquet")
            .select("pmid", "wiki_id", "spot").persist()

        if(doc_annotate.count() != 0) {
           logger.info("Beginning of the wid1_wid2_df generation...")
           var wid1_wid2_df = doc_annotate.selectExpr("pmid", "wiki_id as wid1").distinct
               .join(doc_annotate.selectExpr("pmid as pmid2", "wiki_id as wid2").distinct,
                    $"pmid" === $"pmid2" && $"wid1" < $"wid2")
               .select("pmid", "wid1", "wid2")

           //this row is new, so we need to test it
           val top_wind = Window.orderBy($"wid1", $"wid2", desc("pmid")).partitionBy("wid1", "wid2")
           wid1_wid2_df
              .withColumn("n_row", row_number().over(top_wind))
              .where($"n_row" <= 10).drop("n_row")
              .distinct.write.mode("overwrite").save(paths("wid1_wid2_pmid") + (if (opt == 0) "1" else ""))
           wid1_wid2_df = wid1_wid2_df.groupBy("wid1", "wid2").agg(countDistinct(col("pmid")).as("freq"))

           logger.info("Saving wikiId_spot and wid1_wid2_freq DataFrame...")
           // 1. Saving wid1_wid2_freq dataframe
           wid1_wid2_df.write.mode("overwrite").save(paths("wid1_wid2_path") + (if (opt == 0) "1" else ""))

           // 2. Saving wikiID_spot dataframe
           doc_annotate
              .select("wiki_id", "spot").distinct
              .write.mode("overwrite").save(paths("wiki_ids_spot_path") + (if (opt == 0) "1" else ""))

           if(opt == 0){
              updateDataFrame(spark, paths("wiki_ids_spot_path"), fs, 2)
              updateDataFrame(spark, paths("wid1_wid2_path"), fs, 1)
              updateDataFrame(spark, paths("wid1_wid2_pmid"), fs, 3)
              fs.delete(new Path(paths("DocumentsAnnotations_path") + "_filtered1"), true)
           }
        }
    }


    /**
      * prediction_matrix function compute the BioTG_Score as the product among the following contributes:
      *   - w_norm :             DT_Hybrid prediction
      *   - freq:                widx_widy couple frequency
      *   - tagme_relatedness:   Tagme prediction
      **/
    def prediction_matrix(spark: SparkSession, conf_map: mutable.Map[String, Any]):Unit = {
        import spark.implicits._

        val paths      = conf_map("hdfs_paths").asInstanceOf[mutable.Map[String, String]]
        val pred_param = conf_map("dt_hybrid_parameters").asInstanceOf[mutable.Map[String, String]]
        val w_mat      = spark.read.parquet(paths("w_matrix_path") + "/*")
        var rel_tagme  = spark.read.parquet(paths("tagme_relatedness") + "/*")
        var freq_assoc = spark.read.parquet(paths("wid1_wid2_path") + "/*")

        rel_tagme  =  rel_tagme.union(rel_tagme.selectExpr("wid2 as wid1", "wid1 as wid2", "tag_relatedness"))
        freq_assoc =  freq_assoc.union(freq_assoc.selectExpr("wid2 as wid1", "wid1 as wid2", "freq"))

        val top_wind = Window.orderBy($"wid1", desc("BioTG_Score")).partitionBy("wid1")
        w_mat.join(rel_tagme,  Seq("wid1", "wid2"))
             .join(freq_assoc, Seq("wid1", "wid2"))
             .select($"wid1", $"wid2", $"w_norm" * $"tag_relatedness" * $"freq" as "BioTG_Score")
             .withColumn("n_row", row_number().over(top_wind))
             .where($"n_row" <= pred_param("top").toInt).drop("n_row")
             .write.mode("overwrite").parquet(paths("prediction_matrix_path"))
    }
}
