package Databases_Module.LNC

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object LNCbookUtils {
    val header_lnc = Seq(
        "_c2 as type",
        "_c8 as info"
    )

    val header_disease_lnc = Seq(
        "_c0 as transcript_id",
        "_c1 as gene_id",
        "_c2 as gene_symbol",
        "_c3 as gene_alias",
        "_c4 as disease",
        "_c8 as source",
        "_c9 as mesh_ontology"
    )

    val header_lnc_miRNA = Seq(
        "`Transcript ID` as transcript_id",
        "`MiRNA ID` as miRNA_id"
    )


    /**
     * This user defined function has been implemented in order to convert the string containing all lnc information
     * in a map. Such map allows to create a DataFrame having the LNC information as columns
    **/
    val split_info_book: UserDefinedFunction = udf((info:String) => {
        val splitting = info.split("; ")
        var info_map: Map[String, String] = Map(
            "gene_id"            -> null,
            "gene_type"          -> null,
            "transcript_id"      -> null,
            "transcript_name"    -> null,
            "transcript_alias"   -> null,
            "type"               -> null
        )

        splitting.foreach(column => {
            val key_val = column.split(" ")
            var key     = key_val(0)
            var value   = key_val(1).replaceAll("\"","")

            if(key.contains("transcript_alias")){
               key       = "transcript_alias"
               if(info_map("transcript_alias") != null)
                  value = info_map("transcript_alias") + ";" + value
            }

            info_map   += (key -> value)
        })

        info_map
    })



    def getLNCbook(path:String, spark:SparkSession, sel_file:Int, txt_header:String = "false"): DataFrame = {
        import spark.implicits._
        val header = sel_file match {
          case 0 => header_lnc
          case 1 => header_disease_lnc
          case 2 => header_lnc_miRNA
        }

        val LNCdb = read_tsv(path, spark, req_drop=false, header, txt_header)

        if(sel_file == 0)
           LNCdb
              .where($"type" === "gene" || $"type" === "transcript")
              .withColumn("info", split_info_book($"info"))
              .select(
                  $"info.gene_id".as("lncb_gene_id"),
                  regexp_replace($"info.gene_type", ";", "").as("lncb_gene_type"),
                  $"info.transcript_id".as("lncb_transcript_id"),
                  $"info.transcript_type".as("lncb_transcript_type"),
                  $"info.transcript_alias".as("lncb_transcript_aliases")
              )
              .distinct()
        else
            LNCdb.distinct()
    }

}
