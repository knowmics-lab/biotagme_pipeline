package Networks

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.hadoop.fs.{FileSystem, Path}

object STRINGMain {
    // ============================================================================================================== //
    // ================================================== ATTRIBUTES ================================================ //
    private[this] val root_string  = "/STRING"
    private[this] val root_protein = "/Proteine"

    private[this] val header_string_protInfo = Seq(
        "protein_external_id as string_id",
        "lower(preferred_name) as other_name"
    )

    private[this] val header_string_links = Seq(
        "protein1",
        "protein2",
        "INT(combined_score) as STR_Score"
    )
    // ============================================================================================================== //
    // ================================================== FUNCTIONS ================================================= //
    private[this] def get_path(spark:SparkSession, key: String): String  = {
        val paths = FileSystem.get(spark.sparkContext.hadoopConfiguration).listFiles(new Path(root_string),true)
        var path:  String = null

        while(paths.hasNext){
            val tmp  = paths.next().getPath.toString
            if( tmp.toLowerCase().contains(key))
                path = tmp.replace("hdfs://localhost:9000/", "/")
        }

        path
    }


    private[this] def create_string_protein(spark: SparkSession):DataFrame = {
        import spark.implicits._

        val prot_info_path = get_path(spark, "info")

        val Protein = spark.read.parquet(root_protein + "/Protein_indexing")
            .withColumn("other_name", explode($"other_name"))
        val STRING  = read_tsv(prot_info_path, spark, req_drop = false, header_string_protInfo)
            .withColumn("string_id", split($"string_id", "\\.")(1))

        Protein.join(STRING, "other_name")
           .select("IDX", "protein_name","string_id").distinct
    }


    def create_string_network(spark: SparkSession): Unit= {
        import spark.implicits._

        val prot_info_path = get_path(spark, "links")

        val Protein      = create_string_protein(spark).persist
        val STRING_links = spark.read.format("csv")
            .option("header","true").option("inferSchema", "true").option("sep"," ").load(prot_info_path)
            .selectExpr(header_string_links:_*)
            .select(split($"protein1","\\.")(1) as "protein1", split($"protein2","\\.")(1) as "protein2", $"STR_Score")


        STRING_links.join(Protein.selectExpr("IDX as IDX1", "string_id as protein1").distinct, "protein1")
            .join(Protein.selectExpr("IDX as IDX2", "string_id as protein2").distinct, "protein2")
            .where($"IDX1" =!= $"IDX2")
            .groupBy("IDX1", "IDX2").agg(avg($"STR_Score").as("STR_Score"))
            .write.mode("overwrite").parquet("/Networks/STRING_network")


        Protein.groupBy("IDX", "protein_name").agg(collect_set($"string_id").as("string_id"))
            .withColumn("TYPE", lit("PROTEIN"))
            .write.mode("overwrite").parquet("/STRING_DATA/protein_associations")

        Protein.unpersist
    }

}
