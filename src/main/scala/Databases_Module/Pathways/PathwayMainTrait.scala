package Databases_Module.Pathways

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait PathwayMainTrait {
    var pathway_index: DataFrame = _

    /*
    def df_elaboration(dfa:DataFrame, dfb:DataFrame, idx:Int):DataFrame = {
        val name_col = dfa.columns(idx)
        dfb.join(dfa.select(name_col).distinct, Seq(name_col), joinType="leftanti")
    }

    def create_pathway_indexing(root:String, df_set:DataFrame*): Unit = {
        var ptw_idx: DataFrame = null
        (0 until df_set.size).foreach(i => {
            var df1 = (if(ptw_idx == null) df_set(i) else df_elaboration(ptw_idx, df_set(i),i)).drop(colName="Pathway_name")
            (i+1 until df_set.size).foreach(j => {
                val df2 = (if(ptw_idx == null) df_set(j) else df_elaboration(ptw_idx, df_set(j),j)).drop(colName="Pathway_name")
                df1 = df1.join(df2, Seq("other_name"), "left_outer")
            })
            df1 = df1.drop("other_name")
            if(ptw_idx != null){
               val colm = ptw_idx.columns
               ptw_idx = ptw_idx.union(
                  df1.select((0 until i).map(c => lit(null).as(colm(c))) ++ df1.columns.map(c => col(c)):_*)
               )
            } else ptw_idx = df1
        })
        ptw_idx = ptw_idx
           .withColumn("IDX", concat_ws("", lit("BTG_PATHWAY:"), monotonically_increasing_id()))
           .persist

        /* Name and aliases indexing */
        df_set.foreach(df => {
            val col_name   = df.columns.filter(c => c.contains("ID"))(0)
            val tmp = df.join(ptw_idx.select(col_name, "IDX"), Seq(col_name)).withColumnRenamed(col_name, newName="SOURCE_ID")
            name_alias_idx = if(name_alias_idx == null) tmp.distinct else name_alias_idx.union(tmp.distinct)
        })
        name_alias_idx = name_alias_idx.groupBy("IDX").agg(
            collect_set(col("Pathway_name"))(0).as("Pathway_name"),
            collect_set(col("other_name")).as("Pathway_synonyms"),
            collect_set(col("Source_ID")).as("Source_ID")
        )
        name_alias_idx.write.mode("overwrite").parquet(root + "/Pathway_index")
        name_alias_idx.persist
        ptw_idx.unpersist
    }


    /**
     * creates a DataFrame that contains all pathways-other_elements relationships saved into the databases used
     **/
    def create_pathway_relationships(df:DataFrame, ext_id:String = "", col_id:String, type_rel:String, ext_name:String = ""): DataFrame = {
        var relationship: DataFrame = df.select(
            col(col_id).as("Source_ID"),
            (if(ext_name == "") lit(ext_name) else col(ext_name)).as("NAME2"),
            (if(ext_id   == "") lit(null) else col(ext_id)).as("REFERENCE")
        ).distinct.where(col("NAME2").isNotNull)
        if(ext_id != "")
            relationship = relationship.where(col(ext_id).isNotNull)
               .withColumn("REFERENCE", concat_ws(sep=":", lit(ext_id.split("_")(0)), col("REFERENCE")))

        relationship = relationship
            .join(name_alias_idx.selectExpr("IDX", "Pathway_name", "explode(Source_ID) as SOURCE_ID"), "Source_ID")

        relationship.select(
            col("Pathway_name").as("NAME1"),
            col("IDX").as("IDX1"),
            col("NAME2"),
            lit("").as("IDX2"),
            col("REFERENCE"),
            lit(type_rel).as("TYPE")
        )
    }
    */

}
