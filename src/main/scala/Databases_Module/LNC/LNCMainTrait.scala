package Databases_Module.LNC

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

trait LNCMainTrait {
    var LNC_indexing: DataFrame = _

    val miRNA_reference_elaboration: UserDefinedFunction = udf((id:String) => {
        val id_components = id.split(":")
        var new_id        = id

        if(id_components.length > 1 && id_components(0).contains("miRNA"))
           new_id = id_components(0) + ":hsa-" + id_components(1).toLowerCase

        new_id
    })

}
