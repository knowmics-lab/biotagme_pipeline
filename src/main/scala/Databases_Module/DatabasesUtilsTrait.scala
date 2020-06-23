package Databases_Module

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

trait DatabasesUtilsTrait {
  /**
   * df_elaboration return a dataframe contains only the dfb columns don't have corresponding ids in dfa
   **/
  private[this] def df_elaboration(dfa:DataFrame, dfb:DataFrame, idx:Int):DataFrame = {
    val name_col = dfa.columns(idx)
    dfb.join(dfa.select(name_col).distinct, Seq(name_col), joinType="leftanti")
  }


  private[this] def bioCollector(n:String,opt:Int = 1): Column = {
     if(opt == 0) collect_set(col(n))(0).as(n) else collect_set(col(n)).as(n)
  }


  val merge_arrays: UserDefinedFunction = udf((elements:Seq[Seq[String]]) => {
      var new_list:Seq[String] = null
      elements.foreach(list => {
          new_list = if(new_list == null) list else new_list.union(list).distinct
      })
      new_list
  })


  /**
   * create_element_indexing is a function which associates an idx to each element name. The computation is
   * divided into two phases:
   *   - Indexing generation: During this phase, a matching among the IDs of all used database is carried out
   *     at first. Then an biotagme idx is assigned to each match.
   *   - Name and aliases Indexing: During this phase, a DataFrame contains biotagme ids, elements name and aliases,
   *     and references is generated.
   **/
  def create_element_indexing(col_name:String, Type:String ,df_set:DataFrame*): DataFrame = {
      /* Indexing generation */
      var element_idx: DataFrame = null
      (0 until df_set.size).foreach(i => {
          val cond = element_idx == null
          var df1  = (if(cond) df_set(i) else df_elaboration(element_idx, df_set(i),i)).drop(col_name).distinct
          (i+1 until df_set.size).foreach(j => {
              val df2 = (if(cond) df_set(j) else df_elaboration(element_idx, df_set(j),j)).drop(col_name).distinct
              df1     = df1.join(df2, Seq("other_name"), joinType="left_outer")
          })
          df1 = df1.drop("other_name").distinct
          df1 = df1.withColumn("IDX", col(df1.columns.head))

          element_idx =
              if(!cond){
                 val first_columns = element_idx.columns.take(i).map(name_col => lit(null).as(name_col))
                 val last_columns  = df1.columns.map(name_col => col(name_col))
                 element_idx.union(df1.select(first_columns ++ last_columns:_*))
              } else df1
      })
      element_idx.persist()

      /* Name and aliases indexing */
      var name_aliases_idx: DataFrame = null
      df_set.foreach(df => {
          val col_id = df.columns.filter(c => c.toLowerCase.contains("id"))(0)
          val tmp = df.join(element_idx.select(col_id, "IDX").distinct, Seq(col_id))
              .withColumnRenamed(col_id, newName="SOURCE_ID")
          name_aliases_idx = if(name_aliases_idx == null) tmp.distinct else name_aliases_idx.union(tmp).distinct
      })
      name_aliases_idx = name_aliases_idx.groupBy("IDX")
         .agg(bioCollector(col_name, opt=0), bioCollector("other_name"), bioCollector("SOURCE_ID"))
         .drop("IDX").distinct.groupBy(col_name)
         .agg(merge_arrays(collect_list(col("other_name"))).as("other_name"),
              merge_arrays(collect_list(col("SOURCE_ID"))).as("SOURCE_ID")
         ).withColumn("IDX", concat_ws(sep="", lit(literal="BTG_" + Type +":"), monotonically_increasing_id()))

      element_idx.unpersist
      name_aliases_idx.select("IDX", col_name, "other_name", "SOURCE_ID").persist
  }


  /**
   * creates a DataFrame that contains all the relationships saved into the databases used. The relationships
   * are between two different type of biological elements such as gene-protein, protein-drug and so on.
   **/
   private[this] def col_selector(ext_name:String, alias:String):Column = {
      (if(ext_name == "") lit(ext_name) else col(ext_name)).as(alias)
   }
   def create_relationships(
       df:DataFrame, col_id:String, int_name:String, name_aliases_idx:DataFrame,
       type_rel:String, ext_name:String = "", ext_id:String = ""
   ):  DataFrame =
   {
       var relationship: DataFrame = df.select(
           col(col_id).as(alias = "SOURCE_ID"),
           col_selector(ext_name, alias = "NAME2"),
           col_selector(ext_id,   alias = "REFERENCE")
       ).where(col(colName="NAME2").isNotNull).distinct

       if(ext_id != "")
          relationship = relationship.where(col(ext_id).isNotNull)
              .withColumn("REFERENCE", concat_ws(sep=":", lit(ext_id.split("_")(0)), col("REFERENCE")))

       relationship.join(
           name_aliases_idx.selectExpr("IDX", int_name, "explode(SOURCE_ID) as SOURCE_ID"),
           usingColumn="SOURCE_ID"
       ).select(
           col(int_name).as("NAME1"), col("IDX").as("IDX1"), col("NAME2"),
           lit("").as("IDX2"), col("REFERENCE"), lit(type_rel).as("TYPE")
       ).distinct
   }


   /**
    * creates a DataFrame that contains all the relationships between the same biological element type saved into
    * the databases used. gene-gene, protein-protein, and so on
    **/
   def create_belem_belem_relationships(indexing: DataFrame, df_bel:DataFrame, type_rel:String): DataFrame = {
       var belem_relationships = df_bel
       val pref_column_id      = belem_relationships.columns(0).dropRight(1)
       val col_name            = type_rel + "_name"

       (1 to 2).foreach(id => {
           belem_relationships = belem_relationships.join(
               indexing.selectExpr("IDX as IDX" + id, col_name + " as NAME" + id ,"explode(SOURCE_ID) as " + pref_column_id + id),
               usingColumn=pref_column_id + id
           )
       })

       belem_relationships.select(
           col("NAME1"), col("IDX1"),
           col("NAME2"), col("IDX2"),
           lit("").as("REFERENCE"), lit(type_rel + "-" + type_rel).as("TYPE")
       ).distinct
   }

}
