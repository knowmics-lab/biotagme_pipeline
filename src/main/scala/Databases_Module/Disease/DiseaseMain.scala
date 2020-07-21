package Databases_Module.Disease

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import DisGenNetUtils._
import DiseaseCartUtils._
import DiseaseEnhancer._

import scala.xml.Elem

object DiseaseMain extends DiseaseMainTrait with DatabasesUtilsTrait {
       override def get_Diseases_dataframes(spark: SparkSession, conf_xml_obj: Elem): Unit = {
           val paths = get_element_path(conf_xml_obj, "hdfs_paths", "dis")


           /** DisGenNet **/
           val dgn_files              = paths("disgenet_path")
           val dgn_path_root          = dgn_files("root_path")
           val genes_diseases_dgn     = getDisGenNet(dgn_path_root + "/" + dgn_files("gene_disease"),    spark, 1)
           val variants_diseases_dgn  = getDisGenNet(dgn_path_root + "/" + dgn_files("variant_disease"), spark, 1)
           var diseases_diseases_dgn  = getDisGenNet(dgn_path_root + "/" + dgn_files("disease_disease"), spark, 2)
               diseases_diseases_dgn  = remove_phenotypes(genes_diseases_dgn, variants_diseases_dgn, diseases_diseases_dgn, 2)
           val dgn4filter = getDataFrame2Indexing(diseases_diseases_dgn, genes_diseases_dgn,variants_diseases_dgn).persist

           /** DiseaseCard **/
           val dc_path_root      = paths("disease_ontology_path")("root_path")
           val disease_card      = read_obo_disease(dc_path_root + "/*", spark)
           val dc4filter         = diseaseCart2indexing(disease_card).persist
           val disease_disease   = get_card_disease_disease(disease_card)


           /** DiseaseEnhancer **/
           val de_files         = paths("disease_enhancer_path")
           val de_root          = de_files("root_path")
           val disease_enhancer = get_disease_enhancer(de_root + "/" + de_files("disease_info"), spark)
           val de4filer         = get_disEnh2indexing(disease_enhancer).persist

           /** Indexing **/
           val root          = "/" + de_root.split("/")(1)
           disease_indexind  = create_element_indexing("disease_name", "DISEASE",dc4filter, dgn4filter,de4filer).persist
           disease_indexind.write.mode("overwrite").parquet(root + "/disease_indexing")


           /** Relationships **/
           get_disease_disease(create_belem_belem_relationships,diseases_diseases_dgn, disease_disease)
              .union(get_disease_gene(create_relationships, genes_diseases_dgn, disease_enhancer)).distinct
              .groupBy("NAME1","IDX1","NAME2","IDX2","TYPE")
              .agg(collect_set(col("REFERENCE")).as("REFERENCE"))
              .write.mode("overwrite").save(root + "/disease_relationships")


           dgn4filter.unpersist; dc4filter.unpersist; de4filer.unpersist; disease_indexind.unpersist()
       }
}
