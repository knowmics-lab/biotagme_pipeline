package Networks

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable




object LiteratureMain extends NetworksUtils {
    private[this] val components: Set[String] = Set(
        "gene",
        "disease",
        "drug",
        "mirna",
        "mrna",
        "lnc",
        "pathway",
        "protein",
        "enzyme"
    )

    private[this] var indexing_path_map:     Map[String, String] = Map.empty
    private[this] var relationship_path_map: Map[String, String] = Map.empty
    private[this] var root_path: String = _


    /**
      * dropPrefix is a user defined function implemented to remove the external database name from the reference field
      **/
    private[this] val dropPrefix: UserDefinedFunction = udf((id_ref_components: Seq[String]) => id_ref_components.drop(1))


    /**
      *  indexing_rels_path_building returns all paths containing indexing and relationships information0
      **/
    private[this] def push_path_inMap(path: String, type_file:String, root_hdfs:String):Unit = {
        val dir_path           = path.substring(root_hdfs.length -1, path.lastIndexOf("/"))
        val missing_components = type_file match {
            case "index"         => components.diff(indexing_path_map.keySet)
            case "relationships" => components.diff(relationship_path_map.keySet)
        }

        missing_components.foreach(elem_type => {
            if(dir_path.toLowerCase().contains(elem_type))
                type_file match {
                    case "index"         => indexing_path_map += (elem_type -> dir_path)
                    case "relationships" => relationship_path_map += (elem_type -> dir_path)
                }
        })
    }

    private[this] def indexing_rels_path_building(spark: SparkSession): Unit ={
        val root_hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration).globStatus(new Path("/"))(0).getPath.toString
        val paths_lst = FileSystem.get(spark.sparkContext.hadoopConfiguration).listFiles(new Path(root_path), true)

        while(paths_lst.hasNext) {
            val path = paths_lst.next.getPath.toString
            if(path.toLowerCase.contains("index"))          push_path_inMap(path, "index", root_hdfs)
            else if(path.toLowerCase.contains("relations")) push_path_inMap(path, "relationships", root_hdfs)
        }

    }


    /** join_tables has been implemented to create the relationships between two biological components through the
      * information obtained from the well known literature databases. Many time, the databases report the Id of the
      * other databases as REFERENCE, so we make a based-REFERENCE join in order to  get such relationships (opt = 1).
      * Instead, some cases these report the name of the other biological elements as reference (we called it NAME2),
      * so, we make a based-name join (opt=0)*/
    private[this] def join_tables(tb1:DataFrame, tb2:DataFrame, col_name:String, opt:Int): DataFrame = {
        var tmp: DataFrame = null
        if(opt == 0)
           tmp = tb2.selectExpr("IDX", col_name, "explode(other_name) as NAME2")
              .distinct.withColumn("NAME2", lower(col("NAME2")))
              .join(tb1.withColumn("NAME2", lower(col("NAME2"))), "NAME2")
        else
           tmp = tb2.selectExpr("IDX", col_name, "explode(SOURCE_ID) as REFERENCE")
              .distinct.join(tb1, "REFERENCE")

        tmp.selectExpr("NAME1", "IDX1", col_name + " as NAME2", "IDX as IDX2").distinct
    }


    /**
      * create_relationships_df split the references id and the other biological components into two
      * DataFrame called respectively: tb1_name2, tb1_reference.
      * After that, a relationships DataFrame is built by calling the join_tables on the two DataFrame
      **/
    private[this] def create_relationships_df(tb1:DataFrame, tb2:DataFrame):DataFrame = {
        val name_col   = tb2.columns.filter(col => col.contains("name") && !col.contains("other"))(0)
        val tb1_name2  = tb1.drop("REFERENCE").distinct.where(col("NAME2") =!= "")
        var tb1_refers = tb1.drop("NAME2")

        val tb1_refers_col_type = tb1_refers.schema("REFERENCE").dataType.toString
        if( tb1_refers_col_type.contains("ArrayType"))
            tb1_refers = tb1_refers.withColumn("REFERENCE", explode(col("REFERENCE")))
        tb1_refers = tb1_refers.where(col("REFERENCE") =!= "")

        var relationships_1_2:DataFrame = null
        if(tb1_name2.head(1).nonEmpty) {
           val tmp = join_tables(tb1_name2, tb2, name_col, 0)
           relationships_1_2 = if(relationships_1_2 == null) tmp else  relationships_1_2.union(tmp)
        }

        if(tb1_refers.head(1).nonEmpty) {
           tb1_refers = tb1_refers
              .withColumn("REFERENCE", split(col("REFERENCE"), ":"))
              .withColumn("REFERENCE", dropPrefix(col("REFERENCE")))
              .withColumn("REFERENCE", regexp_replace(concat_ws(":", col("REFERENCE")), "=", ":"))

           val tmp = join_tables(tb1_refers, tb2, name_col, 1)
           relationships_1_2 = if (relationships_1_2 == null) tmp else relationships_1_2.union(tmp)
        }

        relationships_1_2
    }



    private[this] def merge_relationships(spark: SparkSession, type1:String, type2:String): DataFrame = {
        import spark.implicits._
        var type1_type2_rel: DataFrame = null

        if(type1 == type2){
            type1_type2_rel = spark.read.parquet(relationship_path_map(type1))
               .where(lower($"TYPE").contains(type1 + "-" + type2))
               .select("NAME1", "IDX1", "NAME2", "IDX2")
        }
        else{
            val indexing_type1  = spark.read.parquet(indexing_path_map(type1) + "/*")
            val indexing_type2  = spark.read.parquet(indexing_path_map(type2) + "/*")
            val relations_type1 = spark.read.parquet(relationship_path_map(type1) + "/*").where(lower($"TYPE").contains(type2))
            val relations_type2 = spark.read.parquet(relationship_path_map(type2) + "/*").where(lower($"TYPE").contains(type1))

            if(!relations_type1.head(1).isEmpty){
                val tmp = create_relationships_df(relations_type1, indexing_type2)
                type1_type2_rel = if(type1_type2_rel == null) tmp else type1_type2_rel.union(tmp)
            }
            if(!relations_type2.head(1).isEmpty){
                val tmp = create_relationships_df(relations_type2, indexing_type1)
                   .selectExpr("NAME2 as NAME1", "IDX2 as IDX1", "NAME1 as NAME2", "IDX1 as IDX2")
                type1_type2_rel = if(type1_type2_rel == null) tmp else type1_type2_rel.union(tmp)
            }
        }

        type1_type2_rel
    }



    def create_literature_network(spark: SparkSession, conf_map: mutable.Map[String, Any]): Unit = {
        import spark.implicits._

        val paths = conf_map("hdfs_paths").asInstanceOf[mutable.Map[String, String]]
        root_path = paths("main_directory")
        indexing_rels_path_building(spark)


        val tmp_components = components.toSeq
        var relationships:DataFrame = null
        for(i <- tmp_components.indices)
            for(j <- i until components.size){
                val tmp = merge_relationships(spark, tmp_components(i), tmp_components(j))
                if(tmp != null && tmp.head(1).nonEmpty)
                   relationships = if(relationships == null) tmp.distinct else relationships.union(tmp.distinct)
            }

        if(relationships != null){
           // Literature relationships saving phase
           relationships.select("IDX1", "IDX2").distinct
              .write.mode("overwrite").parquet(root_path + "/LITERATURE/liter_associations")

           // Literature nodes saving phase
           relationships
              .select($"IDX1" as "IDX", $"NAME1" as "NAME", upper(split($"IDX1", "_|:")(1)) as "TYPE")
              .distinct.union(relationships
                 .select($"IDX2" as "IDX", $"NAME2" as "NAME", upper(split($"IDX2", "_|:")(1)) as "TYPE")
                 .distinct)
              .write.mode("overwrite").parquet(root_path + "/LITERATURE/liter_nodes")
        }

    }

}
