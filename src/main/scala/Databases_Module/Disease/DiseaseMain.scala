package Databases_Module.Disease

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import DisGenNetUtils._
import DiseaseCartUtils._
import DiseaseEnhancer._

object DiseaseMain extends DiseaseMainTrait with DatabasesUtilsTrait {
       def get_Diseases_dataframes(spark: SparkSession): Unit = {
           val root = "/Disease"

           /** DisGenNet **/
           val genes_diseases_dgn     = getDisGenNet(root + "/DisGeNet/all_gene_disease_associations.tsv",    spark, 0)
           val variants_diseases_dgn  = getDisGenNet(root + "/DisGeNet/all_variant_disease_associations.tsv", spark, 1)
           var diseases_diseases_dgn  = getDisGenNet(root + "/DisGeNet/disease_to_disease_CURATED.tsv",       spark, 2)
               diseases_diseases_dgn  = remove_phenotypes(genes_diseases_dgn, variants_diseases_dgn, diseases_diseases_dgn, 2)
           val dgn4filter = getDataFrame2Indexing(diseases_diseases_dgn, genes_diseases_dgn,variants_diseases_dgn).persist

           /** DiseaseCard **/
           val disease_card      = read_obo_disease(root + "/DiseaseCard/*", spark)
           val dc4filter         = diseaseCart2indexing(disease_card).persist
           val disease_disease   = get_card_disease_disease(disease_card)


           /** DiseaseEnhancer **/
           val disease_enhancer = get_disease_enhancer(root, spark)
           val de4filer         = get_disEnh2indexing(disease_enhancer).persist

           /** Indexing **/
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
