import scala.xml.XML.loadFile
import org.apache.spark.sql.SparkSession
import Databases_Module.DataBaseMain._
import Prediction_Module.Distribute_DT_Hybrid.dt_hybrid_manager
import Networks.NetworkMain._
import SQL_JSON_Parser.SQL_JSON_Parser.create_title_abstract_df
import scala.collection.mutable.Map
import Databases_Module.Enzyme.EnzymeMain._
import Prediction_Module.Intermediate_dataframe_creation._
import Prediction_Module.TagmeRelatedness.main_tagme_relatedness
import scala.collection.mutable
import Update_Module.UpdateProcedure.mainUpdater
import Annotation_Module.AnnotationProcedure._



object MainBioTAGMEr extends Databases_Module.DatabasesUtilsTrait with Networks.NetworksUtils with MainBioTAGMErTrait {
   def main(args: Array[String]): Unit = {

      val conf_xml   = loadFile("src/main/configuration/biotagme.xml")
      xml_parser_main(xml2scalaSeq_parser(conf_xml))

      //print(conf_params("tagme_info").asInstanceOf[mutable.Map[String, String]])

      val spark: SparkSession = SparkSession.builder().appName("BioTagmeScala")
          .master("spark://localhost:7077")
          .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/spark-warehouse")
          .getOrCreate()


      //create_title_abstract_df(spark, conf_params("hdfs_paths").asInstanceOf[mutable.Map[String, String]])

      //mainUpdater(conf_xml, spark)
      //getDataFrame_annotations(spark,0,conf_xml)
      //makeCategoriesFiltering(spark,0, conf_xml)

      create_indexing_relationships(spark, conf_params)
      //dt_hybrid_manager(spark, conf_params)
      //intermediate_df_creation(spark, conf_params, 0)
      //main_tagme_relatedness(spark, conf_params, 0)
      //prediction_matrix(spark, conf_params)

      //create_biotg_and_string_network(spark,conf_xml, conf_params)
      //create_csv_for_NEO4J(spark, conf_params)
      //create_csv_for_mysql(spark, conf_params)

   }

}
