package Databases_Module.Pathways

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Databases_Module.Drug.ChEBIUtils.read_tsv
import scala.collection.mutable


object ReactomeUtils {
    private[this] val header_pathways = Seq(
        "_c0        as Reactome_ID",
        "lower(_c1) as Pathway_name",
        "_c2        as species"
    )

    private[this] val header_pathways_rel = Seq(
        "_c0 as Reactome_ID1",
        "_c1 as Reactome_ID2"
    )

    private[this] val header_pathways_elements_relat = Seq(
        "_c2        as Element_name",
        "_c3        as Reactome_ID",
        "lower(_c5) as Pathway_name",
        "_c7        as Species"
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
        path_files: mutable.Map[String, String], spark:SparkSession, pathway_indexing:DataFrame,
        elem_oelem: (DataFrame,String,String,DataFrame,String,String,String) => DataFrame
    ):  DataFrame = {

        val path_files_root = path_files("root_path")
        val paths = Seq(
            path_files_root + "/" + path_files("ensembl_reactome_file"),
            path_files_root + "/" + path_files("mirBase_reactome_file"),
            path_files_root + "/" + path_files("uniprot_reactome_file")
        )
        val type_relats = Map(
            "Ensembl" -> "gene",
            "miRBase" -> "miRNA",
            "UniProt" -> "protein"
        )

        var reactome_rel: DataFrame = null
        paths.foreach(path => {
            val source   = type_relation(path)
            var df       = getReactome(path, spark, flag = 2, source=source)

            df = elem_oelem(
               df, "Reactome_ID", "Pathway_name", pathway_indexing,
               "pathway-" + type_relats(source), "Element_name", source+"_ID"
            )
            reactome_rel = if(reactome_rel == null) df else reactome_rel.union(df)
        })
        reactome_rel
    }


    private def type_relation(path: String):String  = {
        var type_el = ""
        if(path.toLowerCase().contains("mirbase"))
           type_el = "miRBase"
        else if(path.toLowerCase().contains("ensembl"))
           type_el = "Ensembl"
        else if(path.toLowerCase().contains("uniprot"))
           type_el = "UniProt"

        type_el
    }
}
