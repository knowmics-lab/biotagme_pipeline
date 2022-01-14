package Databases_Module.Enzyme

import org.apache.spark.sql.{DataFrame, SparkSession}
import BrendaUtils._
import Databases_Module.DatabasesUtilsTrait
import scala.collection.mutable


object EnzymeMain extends DatabasesUtilsTrait{
    var enzyme_indexing: DataFrame = _
    def get_Enzyme_dataframes(spark: SparkSession, enzyme_conf: mutable.Map[String, Any]): Unit = {
        val enzyme_map  = enzyme_conf("Enzyme").asInstanceOf[mutable.Map[String, mutable.Map[String, String]]]

        /** Brenda dataset **/
        val brenda_root     = enzyme_map("Brenda_path")("root_path")
        val brenda_df       = getBrenda(spark, brenda_root).persist()
        val brenda4indexing = get_Brenda4Indexing(brenda_df)

        /** Indexing **/
        val saving_path = enzyme_map("enzyme_metadata")("path")
        enzyme_indexing = create_element_indexing("enzyme_name", "ENZYME", brenda4indexing).persist
        enzyme_indexing.write.mode("overwrite").parquet(saving_path + "/enzyme_indexing")


        /** Relationships **/
        get_Brenda_relationships(create_relationships, brenda_df, enzyme_indexing)
           .write.mode("overwrite").parquet(saving_path + "/enzyme_relationships")

        brenda_df.unpersist(); enzyme_indexing.unpersist()
    }
}
