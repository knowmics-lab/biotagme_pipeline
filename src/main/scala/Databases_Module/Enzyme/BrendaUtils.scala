package Databases_Module.Enzyme

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BrendaUtils {
    private[this] val interest_parameter: Seq[String] = Seq("ID", "PR", "RN", "SY")


    /**
      *  get_protein_id is an user defined function that extracts the Uniprot ID from the string containing
      *  proteins information associated with the selected enzyme.
      **/
    private[this] val get_protein_id: UserDefinedFunction = udf((info:Seq[String]) => {
        val pattern     = "#\\d+# Homo sapiens (\\w+)? (\\w+)? <\\d+(,\\d+)*>"r
        var proteins_id:Set[String] = Set()

        info.foreach(row => {
            val prot_id_source  = pattern.findFirstMatchIn(row)
            if(prot_id_source.isDefined){
               val tmp = prot_id_source.get
               if(tmp.group(1) != null)
                  proteins_id = proteins_id + tmp.group(1)
             }
        })

        proteins_id.toSeq
    })


    /**
      * getBrenda function returns a DataFrame containing the ID, NAME, Synonims and other information associated
      * with all the Homo Sapiens' enzymes in Brenda database
      **/
    def getBrenda(spark: SparkSession, path:String): DataFrame = {
        import spark.implicits._

        val brenda: DataFrame = spark.read.format("csv")
            .option("header", "false").option("inferSchema", "false").option("sep", "\n")
            .load(path + "/*.txt")

        val spec = Window.orderBy("index").rowsBetween(Window.unboundedPreceding, 0)
        brenda.select(split($"_c0", "\t") as "_c0")
            .where(size($"_c0") > 1 && $"_c0"(0) =!= "")
            .select($"_c0"(0).as("key"), $"_c0"(1).as("value"))
            .filter($"key".isin(interest_parameter:_*))
            .withColumn("flag", when(
              ($"key" =!= "PR" && $"key" =!= "SY") ||
                 ($"key" === "PR" && lower($"value").contains("homo sapiens")) ||
                 ($"key" === "SY" && !$"value".startsWith("#")), 1)
            .otherwise(lit(0)))
            .where($"flag" =!= 0).drop("flag")
            .select($"key", trim($"value") as "value", monotonically_increasing_id as "index")
            .withColumn("enzyme_myId", when($"key" === "ID", $"index"))
            .withColumn("enzyme_myId", last($"enzyme_myId", ignoreNulls = true).over(spec))
            .groupBy("enzyme_myID", "key").agg(collect_list($"value") as "value")
            .groupBy("enzyme_myID").agg(map_from_arrays(collect_list($"key"), collect_list($"value")) as "parameters")
            .select(
                 split($"parameters"("ID")(0), " \\(")(0) as "brenda_id",
                 $"parameters"("RN")(0) as "enzyme_name",
                 $"parameters"("PR")    as "proteins_id",
                 $"parameters"("SY")    as "other_name"
            )
            .where($"proteins_id".isNotNull)
            .where($"enzyme_name".isNotNull && !$"brenda_id".contains("transferred to"))
            .withColumn("proteins_id", when($"proteins_id".isNotNull, get_protein_id($"proteins_id"))
            .otherwise($"proteins_id"))
    }


    /**
      *  get_Brenda4Indexing creates a DataFrame contains brenda_id, enzyme_namea and aliases. It will be used to
      *  make the BioTagME indexing operation
      **/
    def get_Brenda4Indexing(df:DataFrame): DataFrame = {
        df.selectExpr("brenda_id", "enzyme_name", "enzyme_name as other_name")
          .union(
              df.selectExpr("brenda_id", "enzyme_name", "explode(other_name) as other_name")
                .where(col("other_name") =!= "")
          )
        .withColumn("other_name", lower(col("other_name")))
        .distinct()
    }



    def get_Brenda_relationships
    (
        elem_oelem: (DataFrame,String,String,DataFrame,String,String,String) => DataFrame,
        brenda_db: DataFrame, enzyme_indexing: DataFrame
    ):  DataFrame =
    {
        elem_oelem(
          brenda_db.selectExpr("brenda_id", "explode(proteins_id) as UniProtKB_ID"),
          "brenda_id", "enzyme_name", enzyme_indexing, "enzyme-protein", "", "UniProtKB_ID"
        ).distinct()
    }

}
