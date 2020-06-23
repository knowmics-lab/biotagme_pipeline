package Databases_Module.Pathways

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Databases_Module.Drug.ChEBIUtils.read_tsv

object ReactomeUtils {
    val header_pathways = Seq(
        "_c0        as Reactome_ID",
        "lower(_c1) as Pathway_name",
        "_c2        as species"
    )

    val header_pathways_rel = Seq(
        "_c0 as Reactome_ID1",
        "_c1 as Reactome_ID2"
    )

    val header_pathways_elements_relat = Seq(
        "_c2        as Element_name",
        "_c3        as Reactome_ID",
        "lower(_c5) as Pathway_name",
        "_c7        as Species"
    )

    val paths = Seq(
        "Ensembl2Reactome_PE_All_Levels.txt",
        "miRBase2Reactome_PE_All_Levels.txt",
        "UniProt2Reactome_PE_All_Levels.txt"
    )


    def getReactome(path: String, spark:SparkSession, flag:Int, species: String = "Homo sapiens", source:String = ""): DataFrame = {
        import spark.implicits._
        val header = flag match {
            case 0 => header_pathways
            case 1 => header_pathways_rel
            case 2 => ("_c0 as " + source + "_ID") +: header_pathways_elements_relat
            case _ => null
        }

        var DatFr = read_tsv(path, spark, req_drop = false, header, "false")
        if(flag != 1) DatFr = DatFr.where(lower($"Species") === species.toLowerCase).drop("Species")

        DatFr
    }


    def create_reactom_relationship(
        root:String, spark:SparkSession, pathway_indexing:DataFrame,
        elem_oelem: (DataFrame,String,String,DataFrame,String,String,String) => DataFrame
    ):  DataFrame = {

        val type_relats = Map("Ensembl" -> "gene", "miRBase" -> "miRNA", "UniProt" -> "protein")
        var reactome_rel: DataFrame = null
        paths.foreach(path => {
            val source   = path.split("2")(0)
            var df       = getReactome(root + "/Reactome/" + path, spark, flag = 2, source=source)

            df = elem_oelem(
               df, "Reactome_ID", "Pathway_name", pathway_indexing,
               "pathway-"+type_relats(source), "Element_name", source+"_ID"
            )
            reactome_rel = if(reactome_rel == null) df else reactome_rel.union(df)
        })
        reactome_rel
    }

}
