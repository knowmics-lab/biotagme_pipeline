package Databases_Module.Protein

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import UniprotUtils._

object ProteinMain extends ProteinMainTrait with DatabasesUtilsTrait {
    def get_Proteins_dataframes(spark: SparkSession): Unit = {
        val root: String   = "/Proteine"

        /** Uniprot **/
        val Uniprot_db       = getUniprot(root + "/Uniprot/*", spark).persist
        val Uniprot4indexing = getUniprotdb2indexing(Uniprot_db, spark)


        /** Indexing **/
        protein_indexing = create_element_indexing("protein_name", "PROTEIN", Uniprot4indexing)
        protein_indexing.write.mode("overwrite").parquet(root + "/Protein_indexing")


        /** Relationships **/
        create_uniprot_relationship(create_relationships, Uniprot_db, protein_indexing)
           .groupBy("NAME1","IDX1","NAME2","IDX2","TYPE").agg(collect_set(col("REFERENCE")).as("REFERENCE"))
           .write.mode("overwrite").parquet(root + "/Protein_relationships")

        protein_indexing.unpersist; Uniprot_db.unpersist
    }
}
