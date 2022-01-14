package Databases_Module.Disease

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DiseaseCartUtils {
    val desired_field = Seq("id", "name", "alt_id", "synonym", "is_a")

    /**
     * This function has been defined in order to convert the key and value vectors of the considered NCBI
     * disease term to a map. In this way we can specify all the term information by columns.
    **/
    var get_goMap: UserDefinedFunction = udf((key:Seq[String], values: Seq[Seq[String]]) => {
        var index: Int = 0
        var go_map: Map[String, Seq[String]] = Map(
            "alt_id"      -> null,
            "id"          -> null,
            "is_a"        -> null,
            "name"        -> null,
            "synonym"     -> null
        )
        while(index < key.length){
           go_map += (key(index) -> values(index))
           index += 1
        }
        go_map
    })


    def read_obo_disease(path:String, spark:SparkSession): DataFrame = {
        import spark.implicits._
        val spec = Window.orderBy("index").rowsBetween(Window.unboundedPreceding, 0)

        val disease_obo = spark.read.format("csv")
             .option("header", "false")
             .option("inferSchema","false")
             .option("sep", "\n")
             .load(path)

        disease_obo
             .withColumn(colName = "index",      monotonically_increasing_id)
             .withColumn(colName = "id_term",    when(condition  = $"_c0" === "[Term]",    value = $"index"))
             .withColumn(colName = "id_term",    last(columnName = "id_term", ignoreNulls = true).over(spec))
             .withColumn(colName = "id_typedef", when(condition  = $"_c0" === "[Typedef]", value = $"index"))
             .withColumn(colName = "id_typedef", last(columnName = "id_typedef", ignoreNulls = true).over(spec))
             .filter($"id_term".isNotNull && $"_c0" =!= "[Term]" && $"id_typedef".isNull).drop($"id_typedef")
             .withColumn(colName = "_c0", split($"_c0", ": "))
             .select(cols = $"id_term", $"_c0".getItem(0).as("key"), $"_c0".getItem(1).as("values"))
             .filter(col(colName = "key").isin(desired_field:_*))
             .withColumn(colName = "values",
                  when(condition = $"key" === "synonym", split($"values","\"").getItem(1)).
                  otherwise(value= $"values"))
             .withColumn(colName = "values",
                  when($"key" === "is_a", lower(split($"values"," ! ").getItem(1))).
                  otherwise($"values"))
             .groupBy("id_term", "key").agg(collect_list($"values").as("values"))
             .groupBy("id_term").agg(get_goMap(collect_list($"key"),collect_list($"values")).as("features"))
             .filter(!$"features.name"(0).contains("obsolete"))
    }


    def cart_select(df:DataFrame, opt:Int): DataFrame = {
        df.selectExpr("features.id[0] as dc_id", "lower(features.name[0]) as disease_name",
          (if(opt == 0) "features.name[0]" else "explode(features.synonym)") + " as other_name"
        ).withColumn("other_name", lower(col("other_name")))
    }
    def diseaseCart2indexing(df: DataFrame) : DataFrame = cart_select(df,0).union(cart_select(df,1))

    def get_card_disease_disease(df:DataFrame): DataFrame = {
         df.select(
            col("features.id")(0).as("dc_id1"),
            explode(col("features.is_a")).as("dc_id2")
         )
    }

}
