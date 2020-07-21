package Databases_Module.miRNA

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.functions._

object mirBaseUtils {
    val header_mirna = Seq(
        "_c1         as miRNA_id",
        "lower(_c2)  as miRNA_name",
        "lower(_c3 ) as miRNA_old_name",
        "_c8         as is_dead"
    )

    val header_mature = Seq(
        "_c3        as miRNA_id",
        "lower(_c1) as miRNA_name",
        "lower(_c2) as miRNA_old_name",
        "_c7        as is_dead"
    )

    val header_species = Seq(
        "_c0 as species_id",
        "_c1 as species_symbol",
        "_c3 as species_name"
    )

    /**
     * get_mirna function reads the either mirna.txt or mature.txt file and return a spark DataFrame containing only
     * selected species undead miRNA.
    **/
    def get_mirBase(paths: Map[String, String], spark:SparkSession, sel_species: String, sel_file:Int):DataFrame = {
        import spark.implicits._
        val root_path  = paths("root_path")

        val file_nm    = if(sel_file == 0) paths("mirna_file") else paths("mature_file")
        val sel_header = if(sel_file == 0) header_mirna else header_mature

        val mirna      = read_tsv(root_path + "/" + file_nm,              spark, req_drop = false, sel_header,"false")
        val species    = read_tsv(root_path + "/" + paths("specie_file"), spark, req_drop = false, header_species, "false")
            .where(lower($"species_name") === sel_species.toLowerCase).select("species_symbol")

        mirna.withColumn(colName="species_symbol", split($"miRNA_name","-").getItem(0))
            .join(species, usingColumn = "species_symbol")
            .where(condition=$"is_dead" === 0)
            .drop(colNames="species_symbol", "is_dead")
    }

    def select_union(DatFr: DataFrame, spark:SparkSession): DataFrame = {
        import spark.implicits._
        DatFr.selectExpr(DatFr.columns.filter(c => c != "miRNA_old_name") :+ "miRNA_name as other_name":_*)
             .union(DatFr.where(condition=$"miRNA_old_name" =!= "null"))
    }

    def miRBase2Indexing(spark: SparkSession, df_sel: DataFrame*): DataFrame = {
        var miRBase2indexing: DataFrame = null
        df_sel.foreach(df => {
           val tmp = select_union(df, spark)
           miRBase2indexing = if(miRBase2indexing == null) tmp else miRBase2indexing.union(tmp)
        })
        miRBase2indexing.distinct
    }
}
