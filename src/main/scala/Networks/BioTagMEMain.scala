package Networks

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import Annotation_Module.AnnotationUtils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object BioTagMEMain extends NetworksUtils {

    def get_path_indexing(spark:SparkSession):Set[String] = {
        val paths = FileSystem.get(spark.sparkContext.hadoopConfiguration).listFiles(new Path("/"),true)

        var paths_indexing: Set[String] = Set()
        while(paths.hasNext){
              var path = paths.next().getPath.toString
              if(path.contains("_index")){
                 path = path.replace("hdfs://localhost:9000/", "/")
                 paths_indexing += path.split("/").dropRight(1).mkString("/")
              }
        }

        paths_indexing
    }

    /**
     *  create_biologicalElems_inBioTAGME function return a new DataFrame contains all the BioTAGME ids and
     *  wiki_ids associations. The job of this function has been divided in three tasks:
     *     Task 1: All the indexing paths are obtained by the FileSystem function
     *     Task 2: Once the Annotations DataFrame has been built, the columns contains wikipedia spot and title
     *        are merged in a single one. Then the spot/title string is elaborated by our implemented regex udf
     *        in order to guarantee a great matching between the annotation DataFrame and ones containing all the
     *        literature information.
     *     Task 3: Return the DataFrame contains all the BioTAGME ids, wiki_ids associations.
     **/
    def create_biologicalElems_inBioTAGME(spark:SparkSession):Unit = {
        import spark.implicits._

        /* 1. read files from hdfs */
        val paths_indexing = get_path_indexing(spark)

        /* 2. read annotations */
        var filtered_annotations = spark.read.parquet("/Documents_Annotations_filtered")
            .select(
                $"wiki_id",
                arrayBytes2String($"spot").as("spot"),
                arrayBytes2String($"title").as("title")
            ).distinct
        filtered_annotations = filtered_annotations.select("wiki_id", "spot").distinct
            .union(filtered_annotations.select("wiki_id", "title").distinct)
            .withColumn("spot", lower(regex_term($"spot"))).distinct.persist

        /* 3. matching between annotations DataFrame and elements one */
        var bio_nodes: DataFrame = null  //
        paths_indexing.foreach(path_idx => {
            var elements = spark.read.parquet(path_idx)
            val col_name = elements.columns.filter(cl => cl.toLowerCase().contains("name"))(0)
                elements = elements
                   .select($"IDX", col(col_name), explode($"other_name").as("other_name"))
                   .withColumn("other_name", lower(regex_term($"other_name"))).distinct

            val tmp = filtered_annotations.join(elements, expr("spot = other_name"))
                .groupBy("IDX", col_name).agg(collect_set($"wiki_id").as("wiki_ids"))
                .select($"IDX", col(col_name) as "NAME", $"wiki_ids", upper(lit(col_name.split("_")(0))) as "TYPE") //

            bio_nodes = if(bio_nodes == null) tmp else bio_nodes.union(tmp) //
        })
        bio_nodes.write.mode("overwrite").parquet("/BioTG_DATA/bionode_associations") //
        filtered_annotations.unpersist
    }


    def create_biotagme_network(spark:SparkSession): Unit = {
        import spark.implicits._
        val wid1_window = Window.orderBy("wid1").partitionBy("wid1")
        val w_matrix    = spark.read.parquet("/w_matrix_path/*")
            .withColumn("w_max", max($"w").over(wid1_window))
            .select($"wid1", $"wid2", ($"w"/$"w_max").as("w_norm"))
            .where($"wid1" =!= $"wid2").repartitionByRange(col("wid1"))

        val biotg_id_wids = spark.read.parquet("/BioTG_DATA/bionode_associations/*")
            .withColumn("wiki_ids", explode($"wiki_ids"))

        w_matrix.join(biotg_id_wids.selectExpr("IDX as IDX1", "wiki_ids as wid1").distinct, "wid1")
                .join(biotg_id_wids.selectExpr("IDX as IDX2", "wiki_ids as wid2"), "wid2")
                .selectExpr("IDX1", "IDX2", "w_norm as BioTG_Score").distinct
                .where($"IDX1" =!= $"IDX2")
                .groupBy("IDX1", "IDX2").agg(avg($"BioTG_Score").as("BioTG_Score"))
                .write.mode("overwrite").parquet("/Networks/BioTG_network")
    }

}
