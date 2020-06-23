package Databases_Module.mRNA

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, lit, monotonically_increasing_id, typedLit}

trait mRNAMainTrait {
    var mRNA_index: DataFrame = _

    /*
    def create_mRNA_indexing(RefSeq:DataFrame, root: String): Unit = {
        name_index = RefSeq.select("gene_name").distinct
            .withColumn("IDX", concat_ws("", lit("BTG_mRNA:"), monotonically_increasing_id()))
        name_index.write.mode("overwrite").parquet(root + "/idx_mRNAsName")

        val synon_ids = RefSeq.join(name_index, "gene_name").drop("gene_name")
        synon_ids.selectExpr("IDX", "other_name as mRNAs_synonyms").distinct
            .write.mode("overwrite").parquet(root + "/idx_mRNAsSynonyms")
        synon_ids.selectExpr("IDX", "RNA_id as RefSeq_id").distinct
            .write.mode("overwrite").parquet(root + "/bioTgId_otherRNAId")
    }

    def create_mRNA_gene_associations(root: String): Unit = {
        name_index.select(
            col("gene_name").as("NAME1"),
            col("IDX").as("IDX1"),
            col("gene_name").as("NAME2"),
            lit("").as("IDX2"),
            typedLit(Seq.empty[String]).as("REFERENCES"),
            lit("mRNA-gene").as("TYPE")
        ).write.mode("overwrite").parquet(root + "/Relationships")
    }
    */
}
