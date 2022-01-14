package Networks

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import BioTagMEMain._
import STRINGMain._
import Annotation_Module.AnnotationUtils.arrayBytes2String
import LiteratureMain._
import scala.collection.mutable



object NetworkMain {
    def save_csv(df: DataFrame, path: String): Unit = {
        df.coalesce(1).write.format("csv").option("header", "true").option("sep", "\t")
          .mode("overwrite").save(path)
    }


    def create_biotg_and_string_network(spark: SparkSession, conf_params: mutable.Map[String, Any]): Unit = {
        create_literature_network(spark, conf_params)
        create_biologicalElems_inBioTAGME(spark, conf_params)
        create_biotagme_network(spark, conf_params)
        create_string_network(spark, conf_params)
    }


    def merge_biotgNodes_StringNodes(path_tg: String, path_st: String, path_lt:String, spark:SparkSession, root:String): Unit = {
        import spark.implicits._

        /* BioTagME_Nodes */
        val nodes_biotg = spark.read.parquet(root + "/" + path_tg + "/*")

        /* STRING_Nodes */
        val nodes_str   = spark.read.parquet(root + "/" + path_st + "/*")
            .withColumnRenamed("protein_name", "NAME")

        /* LITERATURE_Nodes */
        val nodes_liter = spark.read.parquet(root + "/" + path_lt + "/*")

        /* Saving Nodes information */
        val total_nodes = nodes_biotg.join(nodes_str, Seq("IDX", "NAME", "TYPE"), "full")
            .join(nodes_liter, Seq("IDX", "NAME", "TYPE"), "full")
            .select($"IDX" as "id", $"NAME" as "name", $"wiki_ids", $"TYPE" as "label")

        save_csv(total_nodes.select("id","name", "label").distinct, root + "/Networks/Nodes_csv")
        save_csv(
            total_nodes.select($"id", explode_outer($"wiki_ids") as "wiki_id").distinct,
            root + "/Networks/BioIDs_WikiIDs_csv"
        )

    }


    def create_csv_for_NEO4J(spark: SparkSession, conf_map: mutable.Map[String, Any]):Unit = {
        import spark.implicits._

        val paths      = conf_map("hdfs_paths").asInstanceOf[mutable.Map[String, String]]
        val Biotg_Net  = spark.read.parquet(paths("main_directory") + "/Networks/BioTG_network")
        val STRING_Net = spark.read.parquet(paths("main_directory") + "/Networks/STRING_network")
        val Literature = spark.read.parquet(paths("main_directory") + "/LITERATURE/liter_associations")

        val Edges = Biotg_Net.join(STRING_Net, Seq("IDX1", "IDX2"), "full")
            .join(Literature.select($"IDX1", $"IDX2", lit(1) as "Liter"), Seq("IDX1", "IDX2"), "full")
            .withColumn("BioTG_Score", when($"BioTG_Score".isNull, 0).otherwise($"BioTG_Score"))
            .withColumn("STR_Score",   when($"STR_Score".isNull,   0).otherwise($"STR_Score"))
            .withColumn("Liter",       when($"Liter".isNull,   0).otherwise($"Liter"))

        /* Saving */
        merge_biotgNodes_StringNodes("BioTG_DATA", "STRING_DATA", "LITERATURE/liter_nodes", spark, paths("main_directory"))
        save_csv(Edges, paths("main_directory") + "/Networks/Edges_csv")
    }



    def create_csv_for_mysql(spark: SparkSession, conf_map: mutable.Map[String, Any]):Unit = {
        import spark.implicits._

        val paths = conf_map("hdfs_paths").asInstanceOf[mutable.Map[String, String]]
        val nodes = spark.read.format("csv").option("header","true").option("inferSchema", "true").option("sep", "\t")
            .load(paths("main_directory") + "/Networks/Nodes_csv").select("id", "label").distinct

        val paths_indexing            = get_path_indexing(spark, paths("main_directory"))
        var names_aliases: DataFrame  = null
        paths_indexing.foreach(path => {
            val tmp       = spark.read.parquet(path).selectExpr("IDX as id", "other_name as aliases").distinct
            names_aliases = if(names_aliases == null) tmp else names_aliases.union(tmp)
        })

        /* Saving name-aliases associations */
        save_csv(
            names_aliases.join(nodes.select("id", "label"), "id")
                .select($"id", explode($"aliases") as "alias", $"label").distinct,
            paths("main_directory") + "/Networks/Names_aliases_csv"
        )

        /* Saving wiki_id-title associations */
        save_csv(
            spark.read.parquet(paths("main_directory") + "/Documents_Annotations_filtered/*")
                 .select("wiki_id", "title").distinct
                 .withColumn("title", arrayBytes2String($"title")),
            paths("main_directory") + "/Networks/Wiki_title_csv"
        )

        save_csv(
            spark.read.parquet(paths("wid1_wid2_pmid") + "/*").distinct,
            paths("main_directory") + "/Networks/wid1_wid2_pmid_csv"
        )
    }
}
