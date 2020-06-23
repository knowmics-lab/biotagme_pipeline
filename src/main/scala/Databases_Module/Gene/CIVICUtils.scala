package Databases_Module.Gene

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.functions._

object CIVICUtils {
    val civic_gene_header = Seq(
        "gene_id as civic_gene_id",
        "name    as gene_name"
    )

    val civic_gene_otherInfo_header = Seq(
        "gene_id        as civic_id",
        "gene           as gene_name",
        "lower(disease) as disease_name",
        "lower(drugs)   as drugs_name"
    )


    def getCIVIC(path:String, spark: SparkSession, sel_file:Int): DataFrame = {
        val header = sel_file match {
            case 0 => civic_gene_header
            case 1 => civic_gene_otherInfo_header
        }

        read_tsv(path,spark,req_drop=false,header)
    }

    def getCIVIC4Indexing(civic:DataFrame): DataFrame = {
        civic.selectExpr("civic_id", "gene_name", "gene_name as other_name")
    }


    def getCivic_relationships(
        elem_oelem: (DataFrame,String,String,DataFrame,String,String,String) => DataFrame,
        civic_db:DataFrame, gene_indexing:DataFrame
    ):  DataFrame = {
        elem_oelem(civic_db,"civic_id","gene_name",gene_indexing,"gene-disease", "disease_name", "")
           .union(elem_oelem(
              civic_db.select(col("civic_id"), explode(split(col("drugs_name"),",")).as("drugs_name")),
                 "civic_id", "gene_name", gene_indexing, "gene-drug", "drugs_name", "")
           )
    }
}
