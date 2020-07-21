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
import scala.xml.Elem


object DataBaseMain {
    def create_indexing_relationships(spark:SparkSession, conf_xml_obj: Elem):Unit = {
        get_Diseases_dataframes(spark, conf_xml_obj)
        get_drugs_DataFrames   (spark, conf_xml_obj)
        get_Genes_dataframes   (spark, conf_xml_obj)
        get_LNC_dataframes     (spark, conf_xml_obj)
        get_miRNA_dataframes   (spark, conf_xml_obj)
        get_pathway_dataframes (spark, conf_xml_obj)
        get_Proteins_dataframes(spark, conf_xml_obj)
        get_mRNA_dataframes    (spark, conf_xml_obj)
    }

}
