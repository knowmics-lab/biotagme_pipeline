package Databases_Module.Drug

import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PharmGKBUtils {

   val selection:Seq[String] = Seq(
       "`PharmGKB Accession Id` as PGKB_ID",
       "`Name` as DRUG_NAME",
       "`Generic Names` as Generic_Name",
       "`Trade Names` as Trade_Names",
       "`Brand Mixtures` as Brand_Mixtures",
       "`Cross-references` as Cross_references",
       "`InChI`",
       "`Type`"
   )

   def pharm_loader(root:String, spark: SparkSession):DataFrame = {
       import spark.implicits._
       read_tsv(root + "/*", spark, req_drop = false, selection)
          .where($"Type" === "Drug").drop("Type")
   }


   def columns_group(DatFr: DataFrame,selectCol:String): DataFrame = {
       DatFr.select("PGKB_ID","DRUG_NAME", selectCol)
            .where(col(selectCol).isNotNull)
            .withColumn(selectCol, explode(split(col(selectCol),"\"\\s*,\\s*\"")))
            .withColumn(selectCol, regexp_replace(col(selectCol),"\"",""))
   }

}
