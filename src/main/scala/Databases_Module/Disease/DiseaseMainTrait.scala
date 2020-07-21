package Databases_Module.Disease

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.xml.{Elem, Node}

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
      *
      * @param elem_elem_rel is a function that takes three dataframes and a string as input parameters and return a
      *                      new dataframe containing the string-string (in our case the string is equal to disease)
      *                      relationships obtained by the given databases.
      *
      * @param dgn           is a dataframe containing all diseases information stored in DisGeNET
      * @param dc            is a dataframe containing all diseases information that compose the DiseaseOntology
      *
      * @return DataFrame
      *
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


     /**
      * get_Diseases_dataframes is used to combine the DiseaseOntology (DO), DisGeNET and DiseaseEnhancer databases
      * in a single one in order to generate the following two DataFrame: Disease_indexing and Disease_name_aliases.
      *
      * @param  spark  is the spark session used for communicating with the cluster manager
      * @return Unit
      *
      **/
     def get_Diseases_dataframes(spark: SparkSession, conf_xml_obj: Elem): Unit = {}
}
