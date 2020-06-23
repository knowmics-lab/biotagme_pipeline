package Databases_Module.miRNA

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object mirTarBaseUtils {
     var header = Seq(
         "`miRTarBase ID`                as mirTarBase_ID",
         "lower(miRNA)                   as miRNA_name",
         "`Species (miRNA)`              as miRNA_species",
         "`Target Gene`                  as gene_name",
         "`Species (Target Gene)`        as target_species"
     )

     def read_csv(path:String, spark:SparkSession, req_drop:Boolean, columns:Seq[String], header:String = "true"): DataFrame = {
         var DatFr = spark.read.format("csv")
             .option("header", header).option("sep", ",").option("inferSchema", header)
             .load(path)

         if(req_drop) DatFr.drop(columns:_*) else DatFr.selectExpr(columns:_*)
     }


     def getMirTarBase(path:String, spark:SparkSession,rd:Boolean,has_header:String, species:String): DataFrame = {
          import spark.implicits._
          read_csv(path,spark,req_drop=rd,header,has_header).distinct
              .where(lower($"miRNA_species") === species.toLowerCase && lower($"target_species") === species.toLowerCase)
              .drop("miRNA_species","target_species")
     }
}
