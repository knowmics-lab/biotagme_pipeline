package Databases_Module.Disease

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

trait DiseaseMainTrait {
    var disease_indexind: DataFrame = _

    /**
     * get_disease_gene function returns all the genes-diseases associations in DisGeNET and Disease Enhancer databases
     **/
     def get_disease_gene(
         elem_oelem: (DataFrame, String, String, DataFrame, String, String, String) => DataFrame,
         dgn: DataFrame, de: DataFrame
     ):  DataFrame =
     {
         elem_oelem(dgn, "disease_dgn_id", "disease_name", disease_indexind, "disease-gene", "gene_name", "")
            .union(elem_oelem(de, "de_id", "disease_name", disease_indexind, "disease-gene", "gene_name", ""))
     }


     /**
      * get_disease_disease function returns all the genes-diseases associations in DisGeNET and Disease ontology databases
     **/
     def get_disease_disease(
         elem_elem_rel: (DataFrame,DataFrame,String) => DataFrame,
         dgn:DataFrame, dc:DataFrame
     ):  DataFrame =
     {
         elem_elem_rel(disease_indexind,dgn,"disease")
            .union(elem_elem_rel(disease_indexind, dc, "disease"))
            .distinct
     }

}
