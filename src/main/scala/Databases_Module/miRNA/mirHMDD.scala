package Databases_Module.miRNA

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.functions._

object mirHMDD {
    val header = Seq(
        "lower(mir)     as miRNA_name",
        "lower(disease) as disease_name"
    )

    /**
     * get_HMDD function is used to create a spark DataFrame from the HMDD text file. It contains many
     * miRNAs-drugs interaction.
    **/
    def get_HMDD(path: String, spark:SparkSession):DataFrame = {
        import spark.implicits._

        read_tsv(path, spark, req_drop=false,header).distinct
            .select(
                concat_ws(":", lit("HMDDB"), monotonically_increasing_id()).as("HMDD_ID"),
                $"miRNA_name", $"disease_name"
            )
    }
}
