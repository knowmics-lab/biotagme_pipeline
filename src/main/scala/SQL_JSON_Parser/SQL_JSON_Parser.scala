package SQL_JSON_Parser

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.types.LongType

import scala.collection.mutable
import scala.collection.mutable.Map


object SQL_JSON_Parser {
    private[this] val mysql_type2scala_type_map: Map[String, String] = Map(
        "varchar" -> "String",
        "int"     -> "Int",
        "text"    -> "String",
        "integer" -> "Int",
        "float"   -> "Float",
        "double"  -> "Double",
        "decimal" -> "Double"
    )


    /**
      * mysql_splitting user define function has been implemented to avoid to split the abstract during the feature
      * extraction. We have to remember that the features are separated by comma
      **/
    private[this] val mysql_splitting: UserDefinedFunction = udf((row:String, num_col:Int) => {
        val splitted_row = row.split(",")
        val new_splitted = splitted_row.take(num_col -1)

        new_splitted :+ (splitted_row.drop(num_col -1).mkString(", ") + ".").substring(1)
    })

    private[this] val joinMap: UserDefinedFunction = udf { values: Seq[mutable.Map[String,String]] => values.flatten.toMap }


     /**
       *  is_long user defined function allow to remove all the PMIDs that can not be convertible to long due to
       *  some features extraction error
       **/
     private[this] val is_long: UserDefinedFunction = udf((id: String) => {
        var response = false
        try   {id.toLong; response = true}
        catch {case e: Exception   =>    }
        response
    })


    /**
      * create_header_type_vectors function creates through the SQL file a tuple containing the following two vector:
      *    -header: It contains all the features of the mysql table
      *    -type_ : It contains the features' type
      **/
    private[this] def create_header_type_vectors(spark: SparkSession, path:String): (Seq[String], Seq[String]) = {
        import spark.implicits._

        val spec = Window.orderBy("index").rowsBetween(Window.unboundedPreceding, 0)
        val table: DataFrame = spark.read.textFile(path)
            .where(!$"value".contains("INSERT INTO"))
            .withColumn("index",   monotonically_increasing_id)
            .withColumn("section", when($"value".startsWith("--"), $"index"))
            .withColumn("section", last("section", true).over(spec))
            .persist()

        val begin_sqlFile_createTable = table.where($"value".contains("CREATE TABLE"))
            .drop("value").withColumnRenamed("index", "index_crt")

        val header_type = table.join(begin_sqlFile_createTable, "section")
            .where($"value".contains("`") && $"index" > $"index_crt" && !$"value".contains("KEY"))
            .withColumn("value",   split($"value", "`"))
            .select($"value"(1) as "value", split($"value"(2)," ")(1) as "type")

        val header = header_type.select($"value").rdd.collect.map(row => row(0).toString)
        val type_  = header_type.select($"type").rdd.collect
            .map(row => mysql_type2scala_type_map(row(0).toString.toLowerCase.split("\\(")(0)))

        table.unpersist()
        (header, type_)
    }



    def create_title_abstract_df(spark: SparkSession, paths:mutable.Map[String, String]): Unit = {
        import spark.implicits._

        val path_reading    = paths("sentence_sql_path")
        val path_writing    = paths("allDocuments_path")
        val Semmed          = spark.read.textFile(path_reading).persist
        val (header,types)  = create_header_type_vectors(spark, path_reading)


        Semmed.selectExpr(Semmed.columns(0) + " as value")
            .where($"value".contains("INSERT INTO"))
            .withColumn("value", explode(split(regexp_replace($"value", "INSERT INTO `[^`]+` VALUES \\(", ""), "'\\),\\(")))
            .withColumn("value", mysql_splitting($"value",lit(9)))
            .where(is_long($"value"(0)))
            .select(header.indices.map(idx => $"value"(idx).cast(types(idx)) as header(idx)):_*)
            .sort("PMID", "NUMBER").groupBy("PMID", "TYPE")
            .agg(concat_ws(" ", collect_list($"SENTENCE")) as "SENTENCES")
            .groupBy("PMID").agg(joinMap(collect_list(map($"TYPE", $"SENTENCES"))) as "Title_Abst")
            .select(regexp_replace($"PMID", "'", "").cast(LongType) as "PMID",
                 $"Title_Abst"("'ti'") as "Title",
                 $"Title_Abst"("'ab'") as "Abstract")
            //.withColumn("Is_Long", is_long($"PMID")).where($"Is_Long" === true)
            //.select($"PMID".cast(LongType) as "PMID", $"Title", $"Abstract")
            .write.mode("overwrite").json(path_writing)

        Semmed.unpersist()
    }
}
