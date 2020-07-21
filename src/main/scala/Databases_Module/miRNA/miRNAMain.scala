package Databases_Module.miRNA

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.SparkSession
import mirBaseUtils._
import mirCancerUtils._
import mirTarBaseUtils._
import mirHMDD._
import scala.xml.Elem

object miRNAMain extends miRNAMainTrait with DatabasesUtilsTrait {
    def get_miRNA_dataframes(spark: SparkSession, conf_xml: Elem): Unit = {
        val paths = get_element_path(conf_xml, "hdfs_paths", "miRNA")

        // ========================================================================================================== //
        // ============================================== miRNA Databases =========================================== //
        /** mirBase**/
        val mirbase_paths     = paths("mirbase_path")
        val mirBase_mirna     = get_mirBase(mirbase_paths, spark,  "Homo sapiens", 0)
        val mirBase_mature    = get_mirBase(mirbase_paths, spark,  "Homo sapiens", 1)
        val mirBase4filt      = miRBase2Indexing(spark, mirBase_mirna, mirBase_mature).persist


        /** mirCancer **/
        val mirCancer_rpath = paths("mirCancer_path")("root_path")
        val mirCancer       = get_mirCancer(mirCancer_rpath + "/*.txt", spark)
        val mirCan4filt     = mirCancer.selectExpr("mirCancerID", "miRNA_name", "miRNA_name as other_name").distinct.persist


        /** mirTarBase**/
        val mirTarBase_rpath = paths("mirTarBase_path")("root_path")
        val mirTarBase       = getMirTarBase(mirTarBase_rpath + "/*", spark, rd=false, has_header="true" , species="Homo sapiens")
        val mirTarBase4filt  = mirTarBase.selectExpr("mirTarBase_ID", "miRNA_name", "miRNA_name as other_name").distinct.persist


        /** HMDD **/
        val HMDD_rpath = paths("HMDD_path")("root_path")
        val HMDD       = get_HMDD(HMDD_rpath + "/*", spark)
        val HMDD4filt  = HMDD.selectExpr("HMDD_ID", "miRNA_name", "miRNA_name as other_name").distinct.persist

        // ========================================================================================================== //
        // ================================================ Processing ============================================== //
        /** Indexing **/
        val saving_path = "/" + HMDD_rpath.split("/")(1)
        miRNA_index     = create_element_indexing("miRNA_name","miRNA", mirBase4filt, mirCan4filt, mirTarBase4filt, HMDD4filt)
        miRNA_index.write.mode("overwrite").parquet(saving_path + "/miRNA_index")


        /** Relationship **/
        get_miRNA_relationships(create_relationships, mirCancer, miRNA_index, "mirCancerID",  "disease")
           .union(get_miRNA_relationships(create_relationships, HMDD, miRNA_index, "HMDD_ID", "disease"))
           .union(get_miRNA_relationships(create_relationships, mirTarBase, miRNA_index, "mirTarBase_ID", "gene"))
           .write.mode("overwrite").parquet(saving_path + "/miRNA_relationships")


        /* Unpersist procedure */
        mirBase4filt.unpersist; mirCan4filt.unpersist; mirTarBase4filt.unpersist; HMDD4filt.unpersist; miRNA_index.unpersist

    }

}
