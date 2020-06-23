package Databases_Module.miRNA

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.functions._

object mirCancerUtils {
    val header_mirna = Seq(
        "lower(mirId)  as miRNA_name",
        "lower(Cancer) as disease_name"
    )

    /**
     * get_mirCancer returns all the miRNAs-Cancers relationship from mirCancer databases. We add a new column which
     * represent a row ID (called mirCancer_ID)
    **/
    def get_mirCancer(path: String, spark:SparkSession): DataFrame ={
        read_tsv(path,spark,req_drop=false,header_mirna).distinct
           .withColumn("mirCancerID", concat_ws(":", lit("mirCancer"), monotonically_increasing_id()))
    }

}
