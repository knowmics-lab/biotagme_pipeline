package Databases_Module.Gene

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import EssemblUtils._
import HGNCUtils._
import CIVICUtils._
import Databases_Module.DatabasesUtilsTrait

object GeneMain extends DatabasesUtilsTrait {
    var gene_indexing: DataFrame = _
    def get_Genes_dataframes(spark: SparkSession): Unit = {
        val root: String = "/Gene"

        /** Essembl **/
        val Ensembl_db     = getEnsembl(root + "/Essembl/gene_data.txt", spark, sel_file=0)
        val Ens4indexing   = getEnsembl2indexing(Ensembl_db).persist

        /** HGNC **/
        val HGNC_db        = getHGNC(root + "/HGNC/*", spark)
        val HGNC4indexing  = get_HGNC_df2indexing(HGNC_db).persist

        /** CIVIC */
        val CIVIC_db       = getCIVIC(root + "/CIVIC/nightly-ClinicalEvidenceSummaries.tsv", spark, 1)
        val CIVIC4indexing = getCIVIC4Indexing(CIVIC_db)

        /** Indexing **/
        gene_indexing = create_element_indexing("gene_name", "GENE", Ens4indexing, HGNC4indexing, CIVIC4indexing).persist
        gene_indexing.write.mode("overwrite").save(root + "/gene_indexing")


        /** Relationships **/
        getEnsembl_relationships(create_relationships, root, spark,     gene_indexing)
           .union(get_HGNC_relationships(create_relationships, HGNC_db, gene_indexing))
           .union(getCivic_relationships(create_relationships, CIVIC_db,gene_indexing))
           .distinct.groupBy("NAME1", "IDX1", "NAME2", "IDX2", "TYPE")
           .agg(collect_set(col("REFERENCE")).as("REFERENCE"))
           .write.mode("overwrite").parquet(root + "/gene_relationships")


        gene_indexing.unpersist(); Ens4indexing.unpersist(); HGNC4indexing.unpersist(); CIVIC4indexing.unpersist()

    }
}
