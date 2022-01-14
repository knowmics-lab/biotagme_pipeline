package Databases_Module.Protein

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object UniprotUtils {
    val header_uniprot: Seq[String] = Seq(
        "`Entry`                          as UniProt_ID",
        "`Entry name`                     as uniprot_entry_name",
        "`Status`",
        "`Protein names`                  as protein_names",
        "`Organism`",
        "`Gene names  (synonym )`         as gene_synonyms",
        "`Gene names  (primary )`         as gene_name",
        //"`ChEBI IDs`                      as chEBI_ids",
        "`Cross-reference (RefSeq)`       as RefSeq_id",
        "`Cross-reference (BioGrid)`      as BioGrid_id",
        "`Cross-reference (STRING)`       as STRING_id",
        "`Cross-reference (DrugBank)`     as DrugBank_id",
        "`Ensembl transcript`             as Ensembl_id",
        "`Cross-reference (KEGG)`         as KEGG_id",
        "`Cross-reference (DisGeNET)`     as DisGenNet_id",
        "`Cross-reference (HGNC)`         as HGNC_id",
        "`Cross-reference (BRENDA)`       as BRENDA_id",
        "`Cross-reference (UniPathway)`   as UniPathway_id"
    )



    /**
     * getUniprot function is used to create a spark DataFrame containing all uniprot proteins data
     **/
    def getUniprot(path:String, spark: SparkSession): DataFrame = {
        import spark.implicits._
        read_tsv(path: String, spark, req_drop=false, header_uniprot)
           .where($"Organism".contains("Homo sapiens") && !$"Status".contains("unreviewed"))
    }



    /**
     * The following function return a DataFrame composed of three columns:
     *   - uniprot_entry
     *   - protein_name
     *   - other_name
     **/
    val get_aliases: UserDefinedFunction = udf((txt: String) => {
        val name_pat = "\\([^(|)]+\\)"
        val get_name = txt.split("\\s+" + name_pat + "\\s+(" + name_pat + ")+")(0)
        val pattern  = " \\([^(|)]+\\)".r
        get_name +: pattern.findAllIn(txt).toList.map(word => word.substring(3, word.length()-1))
    })

    def getUniprotdb2indexing(unip_df:DataFrame, spark:SparkSession): DataFrame = {
        import spark.implicits._
        val unip_elab = unip_df
              .select(
                  $"UniProt_ID", get_aliases($"protein_names").as("protein_names"),
                  concat_ws(" ", $"gene_name", $"gene_synonyms").as("gene_names")
              ).withColumn("protein_name", $"protein_names"(0))

        unip_elab.select($"UniProt_ID", $"protein_name", explode($"protein_names").as("other_name"))
              .union(unip_elab.select($"UniProt_ID", $"protein_name", explode(split($"gene_names"," "))
              .as("other_name")))
              .withColumn("other_name", lower($"other_name")).distinct
    }



    /**
     * create_uniprot_relationship function returns a Spark DataFrame contains all the relationships among UniProt DB
     * and the other databases used in BioTagME.
     **/
    def create_uniprot_relationship(
        elem_oelem: (DataFrame,String,String,DataFrame,String,String,String) => DataFrame,
        UniDB: DataFrame, protein_indexing:DataFrame
    ):  DataFrame =
    {
        val maps = Map(
            "RefSeq_id"    -> "protein-mRNA",
            "STRING_id"    -> "protein-string",
            "DrugBank_id"  -> "protein-drug",
            "Ensembl_id"   -> "protein-gene",
            "DisGenNet_id" -> "protein-disease",
            "HGNC_id"      -> "protein-gene",
            "BRENDA_id"    -> "protein-enzyme"
        )

        var uniprot_relationships: DataFrame = null
        maps.keys.foreach(ext_id => {
            val tmp = elem_oelem(
                UniDB, "UniProt_ID", "protein_name", protein_indexing, maps(ext_id),
                if(maps(ext_id).contains("gene")) "gene_name" else "", ext_id
            )
            uniprot_relationships = if(uniprot_relationships == null) tmp else uniprot_relationships.union(tmp)
        })

        uniprot_relationships.withColumn("REFERENCE", explode(reference_elaboration(col("REFERENCE"))))
    }



    /**
      * Since the disease reference id is not prefixed with C0..., the following user defined function will be
      * used to add such prefix. In addition, the semicolumn-separated external ids are split in a vector of ids
      **/
    val reference_elaboration: UserDefinedFunction = udf((id:String) => {
        val id_components = id.split(":")
        var new_id = id_components(1).split(";")

        if(id_components.length > 1 && id_components(0).toLowerCase.contains("disge"))
           new_id = new_id.filter(id_dge => id_dge != "")
              .map(id_dge => id_components(0) + ":C" + Array.fill(7 - id_dge.length)(0).mkString("") + id_dge)
        else
           new_id = new_id.filter(id_dge => id_dge != "")
              .map(id_dge => id_components(0) + ":" + id_dge.split(" \\[")(0))


        new_id
    })
}
