package Databases_Module.Pathways

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.SparkSession
import PathBankUtils._
import ReactomeUtils._
import Panther._
import org.apache.spark.sql.functions.{col, collect_set}
import scala.collection.mutable



object PathwayMain extends PathwayMainTrait with DatabasesUtilsTrait{
   def get_pathway_dataframes(spark: SparkSession, pathway_conf: mutable.Map[String, Any]): Unit = {
       val paths = pathway_conf("Pathway").asInstanceOf[mutable.Map[String, mutable.Map[String, String]]]

       /** PathBank **/
       val path_bank_paths    = paths("PathBank_path")
       val path_bank_root     = path_bank_paths("root_path")
       val path_bank_info     = getPathBank(path_bank_root + "/" + path_bank_paths("proteins_pathwats_file"), spark, 1)
       val pbank4filtering    = getPathway2Indexing(path_bank_info, "SMPDB_ID").persist


       /** Reactome **/
       val reactome_paths      = paths("Reactome_path")
       val reactome_root       = reactome_paths("root_path")
       val reactome            = getReactome(reactome_root + "/" + reactome_paths("pathways_file"), spark, 0)
       val reactome4filtering  = getPathway2Indexing(reactome, "Reactome_ID").persist


       /** Panther **/
       val panther_root       = paths("Panther")("root_path")
       val Panther            = getPanther(panther_root + "/*", spark)
       val panther4filtering  = getPathway2Indexing(Panther, "Panther_ID").persist


       /** Indexing **/
       val saving_file = paths("pathway_metadata")("path")
       pathway_index   = create_element_indexing("Pathway_name","PATHWAY", pbank4filtering, reactome4filtering, panther4filtering)
       pathway_index.write.mode("overwrite").parquet(saving_file + "/Pathway_indexing")


       /** Relationships **/
       val reactome_pathw_pathw  = getReactome(reactome_root + "/" + reactome_paths("pathways_pathways_file"), spark, 1)
       create_pathbank_relationships(path_bank_info, pathway_index, create_relationships)
          .union(create_reactom_relationship(reactome_paths, spark, pathway_index, create_relationships))
          .union(create_panther_relationships(create_relationships, Panther, pathway_index))
          .union(create_belem_belem_relationships(pathway_index, reactome_pathw_pathw, "Pathway").distinct)
          .distinct.groupBy("NAME1", "IDX1", "NAME2", "IDX2", "TYPE")
          .agg(collect_set(col("REFERENCE")).as("REFERENCE"))
          .write.mode("overwrite").parquet(saving_file + "/Pathway_relationships")
   }

}
