import scala.xml.XML.loadFile
import org.apache.spark.sql.{Column, SparkSession}
import Annotation_Module.AnnotationProcedure._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.matching.Regex
import Databases_Module.DataBaseMain.create_indexing_relationships
import Networks.BioTagMEMain.create_biologicalElems_inBioTAGME
import Databases_Module.Gene.GeneMain._
import Databases_Module.Protein.ProteinMain.get_Proteins_dataframes
import Prediction_Module.Distribute_DT_Hybrid.dt_hybrid_manager
import Networks.BioTagMEMain.create_biotagme_network
import Networks.NetworkMain._
import Databases_Module.mRNA.mRNAMain.get_mRNA_dataframes
import Prediction_Module.Intermediate_dataframe_creation.intermediate_df_creation

import scala.xml.Elem

object MainBioTAGMEr {
   def main(args: Array[String]): Unit = {

      val conf_xml = loadFile("src/main/configuration/biotagme.xml")
      //val spark: SparkSession = SparkSession.builder().appName("BioTagmeScala")
      //    .master("spark://localhost:7077")
      //    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/spark-warehouse")
      //    .getOrCreate()

      //mainUpdater(conf_xml, spark)
      //getDataFrame_annotations(spark,1,conf_xml)
      //makeCategoriesFiltering(spark,1,conf_xml)

      //create_indexing_relationships(spark)
      //dt_hybrid_manager(spark,conf_xml)
      //intermediate_df_creation(spark,conf_xml)


      //create_biotg_and_string_network(spark)
      //create_csv_for_NEO4J(spark)
      //create_csv_for_mysql(spark)

      println(get_element_path(conf_xml, "hdfs_paths", "gene"))

   }
}
