package Databases_Module.Pathways

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.miRNA.mirTarBaseUtils.read_csv
import org.apache.spark.sql.functions._

object PathBankUtils {
    val header_pathways = Seq(
        "`SMPDB ID`  as SMPDB_ID",
        "lower(name) as Pathway_name",
        "subject"
    )

    val header_pathway_otherElem = Seq(
        "`PathBank ID`          as SMPDB_ID",
        "lower(`Pathway Name`)  as Pathway_name",
        "`Pathway Subject`      as suject",
        "species",
        "`Protein Name`         as protein_name",
        "`Uniprot ID`           as UniProt_ID",
        "`HMDBP ID`             as HMDBP_ID",
        "`DrugBank ID`          as DrugBank_ID",
        "`GenBank ID`           as GenBank_ID",
        "`Gene Name`            as gene_name"
    )

    /**
     * this function is invoked to get all information about pathways and logical relations among pathways,
     * drugs, disease and gene.
     **/
    def getPathBank(path: String, spark:SparkSession, flag:Int, species:String = "Homo sapiens"): DataFrame = {
        import spark.implicits._
        val header = flag match {
          case 0 => header_pathways
          case 1 => header_pathway_otherElem
          case _ => null
        }

        var DatFr = read_csv(path, spark, req_drop=false, header).where($"Pathway_name" =!= "null")
        if(flag != 0) DatFr = DatFr.where(lower($"species") === species.toLowerCase).drop($"species")
        DatFr
    }

    /**
     * getPathway2Indexing is used to create a DataFrame containing the Pathways name and aliases that will be
     * used to create the biotagme pathways ids.
     **/
    def getPathway2Indexing(df:DataFrame, id_col:String): DataFrame = {
        df.select(
            col(id_col),
            col("Pathway_name"),
            split(col("Pathway_name"), " \\(")(0).as("other_name")
        ).distinct
    }


    /**
     * creates a DataFrame that contains all pathways-other_elements relationships saved into the databases used
     **/
    def create_pathbank_relationships(
        df:DataFrame, pathway_index: DataFrame,
        elem_oelem: (DataFrame,String,String,DataFrame,String,String,String) => DataFrame
    ):  DataFrame =
    {

        val pathbank_relationships =
                  elem_oelem(df,"SMPDB_ID", "Pathway_name", pathway_index, "pathway-gene", "gene_name", "")
           .union(elem_oelem(df,"SMPDB_ID", "Pathway_name", pathway_index, "pathway-protein", "", "UniProt_ID"))
           .union(elem_oelem(df,"SMPDB_ID", "Pathway_name", pathway_index, "pathway-protein", "", "HMDBP_ID"))
           .union(elem_oelem(df,"SMPDB_ID", "Pathway_name", pathway_index, "pathway-drug",    "", "DrugBank_ID"))

        pathbank_relationships
    }
}
