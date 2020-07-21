package Databases_Module.Gene

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import EssemblUtils._
import HGNCUtils._
import CIVICUtils._
import Databases_Module.DatabasesUtilsTrait
import scala.xml.Elem

object GeneMain extends DatabasesUtilsTrait {
    var gene_indexing: DataFrame = _
    def get_Genes_dataframes(spark: SparkSession, conf_xml:Elem): Unit = {
        val paths          = get_element_path(conf_xml, "hdfs_paths", "gene")

        /** Essembl **/
        val ens_paths      = paths("ensembl_path")
        val ens_root       = ens_paths("root_path")
        val Ensembl_db     = getEnsembl(ens_root + "/" + ens_paths("gene_file"), spark, sel_file=0)
        val Ens4indexing   = getEnsembl2indexing(Ensembl_db).persist

        /** HGNC **/
        val hgnc_root      = paths("hgnc_path")("root_path")
        val HGNC_db        = getHGNC(hgnc_root + "/HGNC/*", spark)
        val HGNC4indexing  = get_HGNC_df2indexing(HGNC_db).persist

        /** CIVIC */
        val civ_paths      = paths("civic_path")
        val civ_root       = civ_paths("root_path")
        val CIVIC_db       = getCIVIC(civ_root + "/" + civ_paths("gene_file"), spark, 1)
        val CIVIC4indexing = getCIVIC4Indexing(CIVIC_db)

        /** Indexing **/
        val sav_root_path = "/" + hgnc_root.split("/")(1)
        gene_indexing = create_element_indexing("gene_name", "GENE", Ens4indexing, HGNC4indexing, CIVIC4indexing).persist
        gene_indexing.write.mode("overwrite").save(sav_root_path + "/gene_indexing")


        /** Relationships **/
        getEnsembl_relationships(create_relationships, ens_root, spark, gene_indexing)
           .union(get_HGNC_relationships(create_relationships, HGNC_db, gene_indexing))
           .union(getCivic_relationships(create_relationships, CIVIC_db,gene_indexing))
           .distinct.groupBy("NAME1", "IDX1", "NAME2", "IDX2", "TYPE")
           .agg(collect_set(col("REFERENCE")).as("REFERENCE"))
           .write.mode("overwrite").parquet(sav_root_path + "/gene_relationships")


        gene_indexing.unpersist(); Ens4indexing.unpersist(); HGNC4indexing.unpersist(); CIVIC4indexing.unpersist()

    }
}
