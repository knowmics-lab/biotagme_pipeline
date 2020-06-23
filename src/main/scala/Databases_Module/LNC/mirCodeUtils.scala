package Databases_Module.LNC

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object mirCodeUtils {
       val header_LNC_mircode = Seq(
           "gene_id",
           "gene_symbol",
           "gene_class",
           "microrna",
           "tr_region"
       )

       def getmirCode(path:String, spark:SparkSession):DataFrame = {
           import spark.implicits._
           read_tsv(path, spark, req_drop=false, header_LNC_mircode)
              .where($"gene_class".contains("lnc"))
       }


       def get_mirCode4indexing(mrCode:DataFrame): DataFrame = {
           mrCode.selectExpr("gene_symbol as LNC_name", "gene_symbol as other_name").distinct
                 .select(
                     concat_ws(":", lit("mrCode"), monotonically_increasing_id).as("miRCode_id"),
                     col("LNC_name"), col("other_name")
                 )
       }

       def create_mirCode_rel(
           elem_oelem: (DataFrame,String,String,DataFrame,String,String,String) => DataFrame,
           lnc_indexing: DataFrame, miRcode4index:DataFrame, miRCode:DataFrame
       ):  DataFrame =
       {
           val miRCode_rel = miRCode.join(miRcode4index.select("mirCode_ID","LNC_name"), expr("gene_symbol = LNC_name"))
           elem_oelem(
               miRCode_rel,"mirCode_ID", "LNC_name", lnc_indexing,
               "LNC-gene", "gene_symbol", "gene_id"
           ).union(
               elem_oelem(
                   miRCode_rel.withColumn("miRNA_ID", explode(mirCol_elaboration(col("microrna")))),
                   "mirCode_ID", "LNC_name", lnc_indexing,
                   "LNC-miRNA", "", "miRNA_ID"
               )
           )

       }

       val mirCol_elaboration: UserDefinedFunction = udf((mirs:String)=> {
           val pref_plus_postfix = mirs.split("-")
           val prefix            = pref_plus_postfix(0)
           val postfix           = pref_plus_postfix(1).split("/")

           postfix.map(ptx => prefix + "-" + ptx)
       })
}
