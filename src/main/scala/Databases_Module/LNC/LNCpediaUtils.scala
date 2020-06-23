package Databases_Module.LNC

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Databases_Module.Drug.ChEBIUtils.read_tsv

object LNCpediaUtils {
       val header_hg19_38 = Seq(
           "gene_id",
           "transcript_id",
           "gene_alias",
           "transcript_alias"
       )

       val header_lnc_gene_ensembl = Seq(
           "lncipediaGeneID as gene_name",
           "ensemblGeneID as Ensembl_ID"
       )

       val header_lnc_tran_ensembl = Seq(
           "lncipediaTranscriptID as lncpedia_transcript_id",
           "ensemblTranscriptID as ensembl_transcript_id"
       )

       val header_lnc_tran_refSeq = Seq(
           "lncipediaTranscriptID as lncpedia_transcript_id",
           "RefSeqID as RefSeq_transcript_id"
       )

       /**
        * read_file function is used to generate a DataFrame which rows are the file ones.
        **/
       def read_file(path:String, spark:SparkSession): DataFrame = {
           spark.read.format("csv")
                .option("sep", "\n")
                .option("header", "false")
                .option("inferSchema", "false")
                .load(path)
       }

       /**
        * split_info_pedia is a user defined function which allows to parse an info string to map of keys-valuse.
        * Such map columns will be used to generate a DataFrame having all element information as columns.
        **/
       val split_info_pedia: UserDefinedFunction = udf((info:String) => {
           val splitting = info.split("; ")
           var info_map: Map[String, String] = Map(
               "gene_id"            -> null,
               "transcript_id"      -> null,
               "gene_alias"         -> null,
               "transcript_alias"   -> null
           )

           splitting.foreach(column => {
               val key_val = column.split(" ")
               var key     = key_val(0)
               var value   = key_val(1).replaceAll("\"","")

               if(key.contains("transcript_alias")){
                  key      = "transcript_alias"
                  if(info_map("transcript_alias") != null)
                     value = info_map("transcript_alias") + ";" + value
               }
               if(key.contains("gene_alias")){
                  key      = "gene_alias"
                  if(info_map("gene_alias") != null)
                     value = info_map("gene_alias") + ";" + value
               }

               info_map   += (key -> value)
           })

           info_map
       })


       def get_LNCpedia(path:String, spark:SparkSession, sel_file:Int): DataFrame = {
           import spark.implicits._
           val header = sel_file match {
               case 0 => header_hg19_38
               case 1 => header_lnc_gene_ensembl
               case 2 => header_lnc_tran_ensembl
               case 3 => header_lnc_tran_refSeq
           }

           if(sel_file == 0) {
              val LNC_data = read_file(path, spark)
                  .where($"_c0".contains("chr"))
                  .withColumn("_c0", split($"_c0", "\t"))
                  .select($"_c0".getItem(2).as("type"), $"_c0".getItem(8).as("info"))
                  .select($"type", split_info_pedia($"info").as("info"))

              LNC_data.selectExpr(LNC_data.columns.filter(c => c != "info") ++ header.map(v => "info." + v + " as " + v): _*)
                  .distinct
           }
           else
               read_tsv(path, spark, req_drop=false, header).distinct
       }



       def get_LNCPedia4Indexing(set_df:DataFrame*): DataFrame = {
           var LNC4Indexing: DataFrame = null

           set_df.foreach(df => {
               val tmp = df.selectExpr("gene_id as LNC_name", "gene_id as other_name")
               LNC4Indexing = if(LNC4Indexing == null) tmp else LNC4Indexing.union(tmp)
               LNC4Indexing = LNC4Indexing.distinct
           })

           LNC4Indexing.select(
              concat_ws(":", lit("LNCp"), monotonically_increasing_id).as("LNCpedia_id"),
              col("LNC_name"), col("other_name")
           )
       }


       def create_LCPedia_rel(
           elem_oelem: (DataFrame,String,String,DataFrame,String,String,String) => DataFrame,
           lnc_indexing: DataFrame, LNCP4Index:DataFrame, df_set:DataFrame*
       ):  DataFrame =
       {
           var LNCPedia4rel: DataFrame = null
           df_set.foreach(df => {
               val tmp = df.join(LNCP4Index.select("LNCpedia_ID", "LNC_name"), expr("LNC_name=gene_id"))
                   .select(col("LNCpedia_ID"), explode(split(col("gene_alias"), ";")).as("gene_id"))
               LNCPedia4rel = if(LNCPedia4rel == null) tmp else LNCPedia4rel.union(tmp)
           })
           LNCPedia4rel = LNCPedia4rel.distinct()

           elem_oelem(LNCPedia4rel, "LNCpedia_ID", "LNC_name", lnc_indexing, "LNC-gene", "", "gene_id")
       }
}
