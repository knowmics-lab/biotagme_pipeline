import scala.xml.XML.loadFile
import org.apache.spark.sql.SparkSession
import Update_Module.UpdateProcedure._
import Annotation_Module.AnnotationProcedure._
import org.apache.spark.broadcast.Broadcast


object MainBioTAGMEr {
   def main(args: Array[String]): Unit = {

      val conf_xml = loadFile("src/main/configuration/biotagme.xml")
      val spark: SparkSession = SparkSession.builder().appName("BioTagmeScala")
          .master("spark://localhost:7077")
          .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/spark-warehouse")
          .getOrCreate()

      //mainUpdater(conf_xml, spark)
      getDataFrame_annotations(spark,1,conf_xml)
      makeCategoriesFiltering(spark,1,conf_xml)

      //System.err.println(spark.conf.get("spark.executor.memory"))


   }
}
