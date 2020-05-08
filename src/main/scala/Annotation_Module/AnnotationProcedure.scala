package Annotation_Module

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import scala.xml.Elem
import AnnotationUtils._
import org.apache.spark.sql.functions._


object AnnotationProcedure extends AnnotationTrait {

  /**
   * Main function to Annotation procedure:
   *   - The first "select" operation generate a DataFrame where the title and abstract columns have been
   *     joined in a single one. The delimiter element is \n
   *   - The second "select" operation invoke the sendAnnotationRequest user define function in order to
   *     annotate the title-abstract text. It will return a new DataFrame composed of two columns:
   *       - pmid column
   *       - annotations structure
   *   - The "filter" operation is executed to remove all the record having an empty annotation set
   *   - The last "select" will return a DataFrame where the annotation structure has been exploded in columns
   **/
  override def getDataFrame_annotations(spark: SparkSession, option: Int, conf_xml_obj: Elem): Unit = {
      import spark.implicits._

      // configuration parameters
      val parameters_map = getTagme_info(conf_xml_obj)
      val documents_path = if(option == 0) parameters_map("newDocuments_path") else parameters_map("allDocuments_path")

      // spark DataFrame and variables
      val new_abst   = spark.read.json(documents_path + "/*.json")

      // procedure
      new_abst.repartition(getNumPartition(new_abst, spark),$"PMID")
         .select($"PMID", concat($"title", lit("\n"), $"Abstract").as("Text"))
         .select($"PMID",
              explode(sendAnnotationRequest(typedLit(parameters_map), $"Text", $"PMID").getItem("annotations")).as("An")
         ).filter($"An.title".isNotNull)
         .select($"PMID",
              when_other($"An.spot",0).as("spot"),
              $"An.rho".as("rho"),
              when_other($"An.dbpedia_categories", 1).as("dbpedia_categories"),
              $"An.id".as("id"),
              when_other($"An.title", 0).as("title")
         ).write.mode("overwrite").parquet(parameters_map("DocumentsAnnotations_path"))
  }


  /**
   * Categories Filtering operation:
   *   The join between the new_abst DataFrame and categories one  is carried out in order to remove all of no
   *   biology record. There are only DBpedia biology categories within "categories DataFrame".
   *   NOTE:
   *     If the opt parameter is zero, then the new annotation will be append to the old one. At the same time,
   *     only the new ones will be stored in another directory so that the biotagme score will be compute only
   *     to these. In addition, the new document title-abstract will be append to the old.
   *
   **/
  override def makeCategoriesFiltering(spark: SparkSession, option: Int, conf_xml_obj: Elem): Unit ={
      import spark.implicits._

      // configuration parameters
      val fs:FileSystem  = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val parameters_map = getTagme_info(conf_xml_obj)
      val categories     = spark.read.parquet(parameters_map("categories_path") + "/*.parquet")

      var annotation_filtering = spark.read.parquet(parameters_map("DocumentsAnnotations_path") + "/*.parquet")
         .withColumn("dbpedia_categories", explode($"dbpedia_categories"))
         .withColumn("dbpedia_categories", arrayBytes2String($"dbpedia_categories"))
         .join(broadcast(categories), "dbpedia_categories")
         .selectExpr("PMID as pmid", "id as wiki_id","spot","rho", "title").distinct()

      if(option == 0) {
          // Update annotations DataFrame
          val annotation_filtering_prev = spark.read.parquet(parameters_map("DocumentsAnnotations_path") + "_filtered/*.parquet")
          annotation_filtering = annotation_filtering.join(annotation_filtering_prev, Seq("pmid","wiki_id"), "leftanti")
          annotation_filtering.write.mode("overwrite").parquet(parameters_map("DocumentsAnnotations_path") + "_filtered1")
          annotation_filtering.write.mode("append").parquet(parameters_map("DocumentsAnnotations_path") + "_filtered")

          // Update articles table adding the new pmid with title and abstract
          val all_documents = spark.read.json(parameters_map("allDocuments_path") + "/*.json")
          val new_documents = spark.read.json(parameters_map("newDocuments_path") + "/*.json")
          all_documents.union(new_documents).distinct
             .write.mode("overwrite").json(parameters_map("allDocuments_path"))

          fs.delete(new Path(parameters_map("newDocuments_path")), true)
      }
      else
         annotation_filtering.write.mode("overwrite").parquet(parameters_map("DocumentsAnnotations_path") + "_filtered")

      // The directory containing the unfiltered annotations will be deleted
      fs.delete(new Path(parameters_map("DocumentsAnnotations_path")), true)
  }

}
