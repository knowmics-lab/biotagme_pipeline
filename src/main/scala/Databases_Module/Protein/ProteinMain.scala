package Databases_Module.Protein

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import UniprotUtils._
import scala.collection.mutable


object ProteinMain extends ProteinMainTrait with DatabasesUtilsTrait {
    def get_Proteins_dataframes(spark: SparkSession, protein_conf: mutable.Map[String, Any]): Unit = {
        val paths = protein_conf("Protein").asInstanceOf[mutable.Map[String, mutable.Map[String, String]]]

        /** Uniprot **/
        val Uniprot_root     = paths("UniProtKB_path")("root_path")
        val Uniprot_db       = getUniprot(Uniprot_root + "/*", spark).persist
        val Uniprot4indexing = getUniprotdb2indexing(Uniprot_db, spark)


        /** Indexing **/
        val saving_path  = paths("protein_metadata")("path")
        protein_indexing = create_element_indexing("protein_name", "PROTEIN", Uniprot4indexing)
        protein_indexing.write.mode("overwrite").parquet(saving_path + "/Protein_indexing")


        /** Relationships **/
        create_uniprot_relationship(create_relationships, Uniprot_db, protein_indexing)
           .groupBy("NAME1","IDX1","NAME2","IDX2","TYPE").agg(collect_set(col("REFERENCE")).as("REFERENCE"))
           .write.mode("overwrite").parquet(saving_path + "/Protein_relationships")

        protein_indexing.unpersist; Uniprot_db.unpersist
    }
}
