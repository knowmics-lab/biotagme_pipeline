package Databases_Module.Gene

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.functions._

object HGNCUtils {
    val header_hgnc: Seq[String] = Seq(
        "hgnc_id",
        "symbol             as gene_symbol",
        "name               as gene_name",
        "locus_group",
        "locus_type",
        "status",
        "alias_symbol",
        "alias_name",
        "prev_symbol",
        "prev_name",
        "gene_family",
        "gene_family_id",
        "entrez_id",
        "ensembl_gene_id",
        "refseq_accession   as RefSeq_mRNA_ID",
        "uniprot_ids        as UniProtKB_Gene_Name_ID",
        "pubmed_id",
        "cosmic",
        "mirbase",
        "lncrnadb",
        "enzyme_id",
        "lncipedia"
    )


    val column_for_indexing: Seq[String] = Seq(
        "gene_symbol",
        "gene_name",
        "alias_symbol",
        "alias_name"
        //"prev_symbol",
        //"prev_name"
    )


    /**
     * getHGNC function is used to create a DataFrame from the HGNC.txt file. This DataFrame contains both gene
     * information and relationships with other databases such as Essembl, LNCpedia and so on
     **/
    def getHGNC(path:String, spark:SparkSession): DataFrame = {
        import spark.implicits._
        read_tsv(path,spark,req_drop=false,header_hgnc)
           .where($"status" === "Approved")
           .drop("status")
    }


    /**
     * The get_HGNC_df2indexing function allows to create a DataFrame composed of the following three columns:
     *   - hgnc_id
     *   - gene name
     *   - other name: It can be gene symbol, gene alias and so on
     **/
    def get_HGNC_df2indexing(hgnc_df: DataFrame): DataFrame = {
        var hgnc_indexing: DataFrame = null

        column_for_indexing.foreach(line => {
            val hgnc_df_portion: DataFrame =  hgnc_df
                .selectExpr("hgnc_id", "gene_symbol as gene_name", line + " as other_name")
                .where(col("other_name").isNotNull)
                .withColumn("other_name", explode(split(col("other_name"), "\\|")))
                .distinct()
            hgnc_indexing = if(hgnc_indexing == null) hgnc_df_portion else hgnc_indexing.union(hgnc_df_portion)
        })
        hgnc_indexing.distinct
    }


    def get_HGNC_relationships(
        elem_oelem: (DataFrame,String,String,DataFrame,String,String,String) => DataFrame,
        hgnc_db: DataFrame, gene_indexing: DataFrame
    ):  DataFrame =
    {
        var hgnc_relationships: DataFrame = null
        val map = Map(
            0 -> ("RefSeq_mRNA_ID",         "gene-mRNA"),
            1 -> ("UniProtKB_Gene_Name_ID", "gene-protein"),
            2 -> ("mirbase",                "gene-miRNA"),
            3 -> ("lncipedia",              "gene-lnc")
        )
        map.keys.foreach(k => {
            val info = map(k)
            val tmp  =
                if(info._1.toLowerCase().contains("id"))
                   elem_oelem(hgnc_db, "hgnc_id", "gene_name", gene_indexing, info._2, "", info._1)
                else
                   elem_oelem(hgnc_db, "hgnc_id", "gene_name", gene_indexing, info._2, info._1, "")

            hgnc_relationships = if(hgnc_relationships == null) tmp.distinct else hgnc_relationships.union(tmp.distinct)
        })
        hgnc_relationships
    }

}
