package Databases_Module.Disease

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.functions._

object DiseaseEnhancer {
    val header_disease_enhancer: Seq[String] = Seq(
        "_c4        as gene_name",
        "lower(_c5) as disease_name"
    )

    /**
     *   get_disease_enhancer returns a Spark DataFrame contains all the disease-gene associations obtained
     *   by the DiseaseEnhancer database.
     **/
     def get_disease_enhancer(root:String, spark:SparkSession):DataFrame = {
         read_tsv(
             root + "/DiseaseEnhancer/enh2disease-1.0.2.txt",
             spark, req_drop = false, header_disease_enhancer, "false"
         ).distinct
          .select(
            concat_ws(":", lit("DE"), monotonically_increasing_id).as("de_id"),
            col("disease_name"), col("gene_name")
          )

     }

     def get_disEnh2indexing(df: DataFrame): DataFrame = {
          df.selectExpr("de_id", "disease_name", "disease_name as other_name").distinct
     }
}
