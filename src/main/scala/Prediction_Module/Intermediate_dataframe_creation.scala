package Prediction_Module

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.xml.{Elem, Node}


object Intermediate_dataframe_creation {
  /* This function is used to create a paths_map row. Both the multiple space and \n will be replaced with "" */
  def addEelem(key:String, value: Node):(String,String) =
      key -> value.child(3).text.replaceAll("[\\s+|\n+]","")

  /* During this phase, the key-value association into the xml file will be converted to scala tuple*/
  def widi_widj_paths(xml_obj: Elem): Map[String, String] = {
      var paths_map: Map[String, String] = Map()
      xml_obj.child.foreach(elem_0 =>
          elem_0.label match {
             case "hdfs_paths" =>
                 elem_0.child.foreach(elem_1 => elem_1.label match {
                     case "DocumentsAnnotations_path" => paths_map += addEelem("DocumentsAnnotations_path", elem_1)
                     case "wid1_wid2_path"            => paths_map += addEelem("wid1_wid2_path", elem_1)
                     case "pmids_wiki_ids_path"       => paths_map += addEelem("pmids_wiki_ids_path", elem_1)
                     case "wiki_ids_spot_path"        => paths_map += addEelem("wiki_ids_spot_path", elem_1)
                     case _ => null
                 })
             case _ => null
          })
      paths_map
  }

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
    def intermediate_df_creation(spark: SparkSession, xml_obj: Elem, opt: Int):Unit = {
        import spark.implicits._
        val logger = Logger.getLogger("Intermediate DataFrames creation")
        val fs     = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val paths  = widi_widj_paths(xml_obj)

        logger.info("Reading of the DataFrame containing the documents annotations...")
        val path_docs_ann = paths("DocumentsAnnotations_path") + "_filtered" + (if(opt == 0) "1" else "")
        val doc_annotate  = spark.read.parquet(path_docs_ann + "/*.parquet")
            .select("pmid", "wiki_id", "spot").persist()

        if(doc_annotate.count() != 0) {
           logger.info("Beginning of the wid1_wid2_df generation...")
           val wid1_wid2_df = doc_annotate.selectExpr("pmid", "wiki_id as wid1").distinct
               .join(doc_annotate.selectExpr("pmid as pmid2", "wiki_id as wid2").distinct,
                    $"pmid" === $"pmid2" && $"wid1" < $"wid2")
               .select("pmid", "wid1", "wid2")
               .groupBy("wid1", "wid2").agg(countDistinct(col("pmid")).as("freq"))


           logger.info("Saving wikiId_spot and wid1_wid2_freq DataFrame...")
           wid1_wid2_df.write.mode("overwrite").save(paths("wid1_wid2_path") + (if (opt == 0) "1" else ""))
           doc_annotate
              .select("wiki_id", "spot").distinct
              .write.mode("overwrite").save(paths("wiki_ids_spot_path") + (if (opt == 0) "1" else ""))

           if(opt == 0){
              updateDataFrame(spark, paths("wiki_ids_spot_path"), fs, 2)
              updateDataFrame(spark, paths("wid1_wid2_path"), fs, 1)
              fs.delete(new Path(paths("DocumentsAnnotations_path") + "_filtered1"), true)
           }
        }
    }
}
