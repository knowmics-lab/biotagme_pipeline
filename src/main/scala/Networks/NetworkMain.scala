package Networks

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import BioTagMEMain._
import STRINGMain._
import Annotation_Module.AnnotationUtils.arrayBytes2String

object NetworkMain {
    def save_csv(df: DataFrame, path: String): Unit = {
        df.coalesce(1).write.format("csv").option("header", "true").option("sep", "\t")
          .mode("overwrite").save(path)
    }


    def create_biotg_and_string_network(spark: SparkSession): Unit = {
        create_biologicalElems_inBioTAGME(spark)
        create_biotagme_network(spark)
        create_string_network(spark)
    }


    def merge_biotgNodes_StringNodes(path_tg: String, path_st: String, spark:SparkSession): Unit = {
        import spark.implicits._

        /* BioTagME_Nodes */
        val nodes_biotg = spark.read.parquet(path_tg + "/*")

        /* STRING_Nodes */
        val nodes_str: DataFrame = spark.read.parquet(path_st + "/*")
            .withColumnRenamed("protein_name", "NAME")

        /* Saving Nodes information */
        val total_nodes = nodes_biotg.join(nodes_str, Seq("IDX", "NAME", "TYPE"), "full")
            .select($"IDX" as "id", $"NAME" as "name", $"wiki_ids", $"TYPE" as "label")

        save_csv(total_nodes.select("id","name", "label").distinct, "/Networks/Nodes_csv")
        save_csv(
            total_nodes.select($"id", explode_outer($"wiki_ids") as "wiki_id").distinct,
            "/Networks/BioIDs_WikiIDs_csv"
        )

    }


    def create_csv_for_NEO4J(spark: SparkSession):Unit = {
        import spark.implicits._
        val Biotg_Net  = spark.read.parquet("/Networks/BioTG_network")
        val STRING_Net = spark.read.parquet("/Networks/STRING_network")

        val Edges = Biotg_Net.join(STRING_Net, Seq("IDX1", "IDX2"), "full")
            .withColumn("BioTG_Score", when($"BioTG_Score".isNull, 0).otherwise($"BioTG_Score"))
            .withColumn("STR_Score",   when($"STR_Score".isNull,   0).otherwise($"STR_Score"))

        /* Saving */
        merge_biotgNodes_StringNodes("/BioTG_DATA", "/STRING_DATA", spark)
        save_csv(Edges, "/Networks/Edges_csv")
    }


    def create_csv_for_mysql(spark: SparkSession):Unit = {
        import spark.implicits._

        val nodes = spark.read.format("csv").option("header","true").option("inferSchema", "true").option("sep", "\t")
            .load("/Networks/Nodes_csv").select("id", "label").distinct

        val paths_indexing            = get_path_indexing(spark)
        var names_aliases: DataFrame  = null
        paths_indexing.foreach(path => {
            val tmp       = spark.read.parquet(path).selectExpr("IDX as id", "other_name as aliases").distinct
            names_aliases = if(names_aliases == null) tmp else names_aliases.union(tmp)
        })

        /* Saving name-aliases associations */
        save_csv(
            names_aliases.join(nodes.select("id", "label"), "id")
                .select($"id", explode($"aliases") as "alias", $"label").distinct,
            "/Networks/Names_aliases_csv"
        )

        /* Saving wiki_id-title associations */
        save_csv(
            spark.read.parquet("/Documents_Annotations_filtered/*")
                 .select("wiki_id", "title").distinct
                 .withColumn("title", arrayBytes2String($"title")),
            "/Networks/Wiki_title_csv"
        )
    }
}
