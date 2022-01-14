package Databases_Module

import org.apache.spark.sql.SparkSession
import Databases_Module.Disease.DiseaseMain.get_Diseases_dataframes
import Databases_Module.Drug.DrugMain.get_drugs_DataFrames
import Databases_Module.Gene.GeneMain.get_Genes_dataframes
import Databases_Module.LNC.LNCMain.get_LNC_dataframes
import Databases_Module.miRNA.miRNAMain.get_miRNA_dataframes
import Databases_Module.Pathways.PathwayMain.get_pathway_dataframes
import Databases_Module.Protein.ProteinMain.get_Proteins_dataframes
import Databases_Module.mRNA.mRNAMain.get_mRNA_dataframes
import Databases_Module.Enzyme.EnzymeMain._
import scala.collection.mutable

object DataBaseMain {
    def create_indexing_relationships(spark:SparkSession, conf_params: mutable.Map[String, Any]):Unit = {
        val map_tmp = conf_params("hdfs_paths").asInstanceOf[mutable.Map[String, Any]]

        // get_Diseases_dataframes(spark, map_tmp)
        // get_drugs_DataFrames   (spark, map_tmp)
        // get_Genes_dataframes   (spark, map_tmp)
        // get_LNC_dataframes     (spark, map_tmp)
        // get_miRNA_dataframes   (spark, map_tmp)
        // get_pathway_dataframes (spark, map_tmp)
        // get_Proteins_dataframes(spark, map_tmp)
        // get_mRNA_dataframes    (spark, map_tmp)
        // get_Enzyme_dataframes  (spark, map_tmp)
    }

}
