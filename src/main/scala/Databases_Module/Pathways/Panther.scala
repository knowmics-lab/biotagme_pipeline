package Databases_Module.Pathways

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.functions._

object Panther {
    val header = Seq(
        "_c0        as Panther_ID",
        "lower(_c1) as Pathway_name",
        "_c3        as gene_name",
        "_c4        as gen_prot_ID",
        "_c5        as protein_name"
    )

    def getPanther(path: String, spark:SparkSession, species: String = "Human"): DataFrame = {
        import spark.implicits._

        read_tsv(path, spark, req_drop=false, header, "false")
            .withColumn("gen_prot_ID", split($"gen_prot_ID", "\\|"))
            .where(lower($"gen_prot_ID".getItem(0)) === species.toLowerCase)
            .withColumn("gen_prot_ID", array_remove($"gen_prot_ID", $"gen_prot_ID".getItem(0)))
            .select(
                col("Panther_ID"), col("Pathway_name"), col("gene_name"),
                col("gen_prot_ID")(0).as("Gene_ID"),
                split(col("gen_prot_ID")(1), "=")(1).as("UniProt_ID")
            )
    }


    def create_panther_relationships (
        elem_oelem: (DataFrame,String,String,DataFrame,String,String,String) => DataFrame,
        pant:DataFrame, pathway_indexing: DataFrame
    ):  DataFrame = {
        elem_oelem(pant, "Panther_ID", "Pathway_name", pathway_indexing, "pathway-gene", "gene_name", "Gene_ID")
          .union(elem_oelem(pant, "Panther_ID", "Pathway_name", pathway_indexing, "pathway-protein", "", "UniProt_ID"))
    }
}
