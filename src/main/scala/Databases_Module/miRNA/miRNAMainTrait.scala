package Databases_Module.miRNA

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

trait miRNAMainTrait {
    var miRNA_index: DataFrame = _

    /**
     * get_mirCancer_relationships return all the miRNA-disease associations saved in mirCancer DB
     **/
    def get_miRNA_relationships(
        elem_oelem: (DataFrame, String, String, DataFrame, String, String, String) => DataFrame,
        mirDB:DataFrame, miRNA_indexing: DataFrame, id_col:String, elem:String
    ):  DataFrame =
    {
       elem_oelem(mirDB, id_col, "miRNA_name", miRNA_indexing, "miRNA-" + elem, elem + "_name", "")
    }

}
