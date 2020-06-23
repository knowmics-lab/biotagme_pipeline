package Databases_Module.Pathways

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.SparkSession
import PathBankUtils._
import ReactomeUtils._
import Panther._
import org.apache.spark.sql.functions.{col, collect_set}

object PathwayMain extends PathwayMainTrait with DatabasesUtilsTrait{
   def get_pathway_dataframes(spark: SparkSession): Unit = {
       val root: String = "/pathways"

       /** PathBank **/
       val path_bank_info     = getPathBank(root + "/PathBank/pathbank_all_proteins.csv", spark, 1)
       val pbank4filtering    = getPathway2Indexing(path_bank_info, "SMPDB_ID").persist


       /** Reactome **/
       val reactome           = getReactome(root + "/Reactome/ReactomePathways.txt", spark, 0)
       val reactome4filtering = getPathway2Indexing(reactome, "Reactome_ID").persist


       /** Panther **/
       val Panther            = getPanther(root + "/Panther/*", spark)
       val panther4filtering  = getPathway2Indexing(Panther, "Panther_ID").persist


       /** Indexing **/
       pathway_index = create_element_indexing("Pathway_name","PATHWAY", pbank4filtering, reactome4filtering, panther4filtering)
       pathway_index.write.mode("overwrite").parquet(root + "/Pathway_indexing")


       /** Relationships **/
       val reactome_pathw_pathw  = getReactome(root + "/Reactome/ReactomePathwaysRelation.txt", spark, 1)
       create_pathbank_relationships(path_bank_info, pathway_index, create_relationships)
          .union(create_reactom_relationship(root, spark, pathway_index, create_relationships))
          .union(create_panther_relationships(create_relationships, Panther, pathway_index))
          .union(create_belem_belem_relationships(pathway_index, reactome_pathw_pathw, "Pathway").distinct)
          .distinct.groupBy("NAME1", "IDX1", "NAME2", "IDX2", "TYPE")
          .agg(collect_set(col("REFERENCE")).as("REFERENCE"))
          .write.mode("overwrite").parquet(root + "/Pathway_relationships")
   }

}
