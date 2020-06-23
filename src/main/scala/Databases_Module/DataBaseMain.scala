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


object DataBaseMain {
    def create_indexing_relationships(spark:SparkSession):Unit = {
        get_Diseases_dataframes(spark)
        get_drugs_DataFrames(spark)
        get_Genes_dataframes(spark)
        get_LNC_dataframes(spark)
        get_miRNA_dataframes(spark)
        get_pathway_dataframes(spark)
        get_Proteins_dataframes(spark)
        get_mRNA_dataframes(spark)
    }

}
