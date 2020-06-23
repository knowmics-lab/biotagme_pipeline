package Databases_Module.Gene

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.hadoop.fs.{FileSystem, Path}

object EssemblUtils {
    val header_gene = Seq(
        "`Gene stable ID`       as Ensembl_gene_id",
        "`Transcript stable ID` as Ensembl_transcript_id",
        "`Gene name`            as gene_name",
        "`Transcript name`      as transcript_name",
        "`Gene Synonym`         as gene_synonym"
    )

    val f_type = Map(
        "miRBase"     -> "gene-miRNA",
        "reactome"    -> "gene-pathway",
        "RefSeq"      -> "gene-RNA",
        "RNACentral"  -> "gene-RNA",
        "uniprot"     -> "gene-protein"
    )


    def getEnsembl(path:String, spark:SparkSession, sel_file: Int):DataFrame = {
        read_tsv(path, spark, req_drop=false, header_gene)
    }

    def getEnsembl2indexing(Ensembl:DataFrame): DataFrame = {
        Ensembl.selectExpr("Ensembl_gene_id", "gene_name", "gene_name as other_name")
           //.union(Ensembl
           //    .selectExpr("Ensembl_gene_id", "gene_name", "gene_synonym as other_name")
           //    .where(col("other_name").isNotNull)
           //)
           .distinct
    }

    def getEnsembl_relationships(
        elem_oelem: (DataFrame,String,String,DataFrame,String,String,String) => DataFrame,
        root:String, spark:SparkSession, gene_indexing:DataFrame
    ):  DataFrame =
    {
        val paths = FileSystem.get(spark.sparkContext.hadoopConfiguration).listFiles(new Path(root + "/Essembl"),true)
        var ens_rel:DataFrame = null

        while(paths.hasNext){
            val path = paths.next.getPath.toString
            if(!(path.contains("data") || path.toLowerCase().contains("hgnc"))){
                val type_rel = path.split("_")(1).split("\\.")(0)
                var tmp = spark.read.format("csv")
                    .option("header", "true").option("sep", "\t").option("inferSchema", "true")
                    .load(path).distinct
                    .withColumnRenamed("Gene stable ID", "Ensembl_gene_id")

                val columns = tmp.columns.map(name_col => name_col.replace(" ", "_"))
                (1 until columns.length).foreach(ext_id => {
                    var rel = tmp.selectExpr(columns(0), "`" + tmp.columns(ext_id) + "` as `" + columns(ext_id) + "`")
                    rel     = elem_oelem(rel,columns(0),"gene_name",gene_indexing,f_type(type_rel),"",columns(ext_id))
                    ens_rel = if(ens_rel == null) rel.distinct else ens_rel.union(rel.distinct)
                })
            }
        }

        ens_rel
    }
}
