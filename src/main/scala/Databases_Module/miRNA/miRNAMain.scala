package Databases_Module.miRNA

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.SparkSession
import mirBaseUtils._
import mirCancerUtils._
import mirTarBaseUtils._
import mirHMDD._

object miRNAMain extends miRNAMainTrait with DatabasesUtilsTrait {
    def get_miRNA_dataframes(spark: SparkSession): Unit = {
        // ========================================================================================================== //
        // ============================================== miRNA Databases =========================================== //
        /** mirBase**/
        val root: String      = "/miRNA"
        val mirBase_mirna     = get_mirBase(root + "/mirbase", spark,  "Homo sapiens", 0)
        val mirBase_mature    = get_mirBase(root + "/mirbase", spark,  "Homo sapiens", 1)
        val mirBase4filt      = miRBase2Indexing(spark, mirBase_mirna, mirBase_mature).persist


        /** mirCancer **/
        val mirCancer   = get_mirCancer(root + "/mirCancer/*.txt", spark)
        val mirCan4filt = mirCancer.selectExpr("mirCancerID", "miRNA_name", "miRNA_name as other_name").distinct.persist


        /** mirTarBase**/
        val mirTarBase = getMirTarBase(root + "/mirTarBase/*", spark, rd=false, has_header="true" , species="Homo sapiens")
        val mirTarBase4filt = mirTarBase.selectExpr("mirTarBase_ID", "miRNA_name", "miRNA_name as other_name").distinct.persist


        /** HMDD **/
        val HMDD      = get_HMDD(root + "/HMDD/*", spark)
        val HMDD4filt = HMDD.selectExpr("HMDD_ID", "miRNA_name", "miRNA_name as other_name").distinct.persist

        // ========================================================================================================== //
        // ================================================ Processing ============================================== //
        /** Indexing **/
        miRNA_index = create_element_indexing("miRNA_name","miRNA", mirBase4filt, mirCan4filt, mirTarBase4filt, HMDD4filt)
        miRNA_index.write.mode("overwrite").parquet(root + "/miRNA_index")


        /** Relationship **/
        get_miRNA_relationships(create_relationships, mirCancer, miRNA_index, "mirCancerID",  "disease")
           .union(get_miRNA_relationships(create_relationships, HMDD, miRNA_index, "HMDD_ID", "disease"))
           .union(get_miRNA_relationships(create_relationships, mirTarBase, miRNA_index, "mirTarBase_ID", "gene"))
           .write.mode("overwrite").parquet(root + "/miRNA_relationships")


        /* Unpersist procedure */
        mirBase4filt.unpersist; mirCan4filt.unpersist; mirTarBase4filt.unpersist; HMDD4filt.unpersist; miRNA_index.unpersist

    }

}
