package Databases_Module.mRNA

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.{DataFrame, SparkSession}
import RefSeqUtils._

import scala.xml.Elem

object mRNAMain extends mRNAMainTrait with DatabasesUtilsTrait{
      def get_mRNA_dataframes(spark: SparkSession, conf_xml: Elem): Unit = {
          val paths = get_element_path(conf_xml, "hdfs_paths", "mRNA")

          val root: String      = paths("RefSeq_path")("root_path")
          val mRNA: DataFrame   = getRefSeq(root + "/*", spark)
          val mRNA4indexing = mRNA
              .selectExpr("RNA_id", "gene_name as mRNA_name", "lower(RNA_name) as other_name")
              .union(mRNA.selectExpr("RNA_id", "gene_name as mRNA_name", "lower(gene_name) as other_name"))
              .persist()

          /** Indexing **/
          mRNA_index = create_element_indexing("mRNA_name", "mRNA", mRNA4indexing)
          mRNA_index.write.mode("overwrite").parquet(root + "/mRNA_indexing")

          /** Relationships **/
          create_relationships(mRNA, "RNA_id", "mRNA_name", mRNA_index, "mRNA-gene", "gene_name", "")
             .write.mode("overwrite").parquet(root + "/mRNA_relationships")

          mRNA4indexing.unpersist; mRNA_index.unpersist
      }
}
