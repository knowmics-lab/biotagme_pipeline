package Databases_Module.mRNA

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Databases_Module.LNC.LNCpediaUtils.read_file
import org.apache.spark.sql.expressions.UserDefinedFunction

object RefSeqUtils {
    val header_mRNA: Array[String] = Array(
        "RNA_name",
        "gene_name",
        "variant",
        "type"
    )


    /**
     *  The following function is used to translate the RNA information in DataFrame Columns.
     *  This information are obtained by the appropriate fasta file.
     **/
    val create_mRNA_columns: UserDefinedFunction = udf((info:String, header:Seq[String]) => {
        var mRNA_data: Map[String, String] = Map()
        header.foreach(elem => mRNA_data += (elem -> null))

        var data  = info.replace("')",">").split("\\),")
        var mRNA_gene_name = data(0).split(" \\(")

        if(mRNA_gene_name.length > 2)
        {
           var tmp: Array[String] = Array()
           tmp = tmp :+ mRNA_gene_name.last
           tmp = mRNA_gene_name.dropRight(1).mkString(" (") +: tmp
           mRNA_gene_name = tmp
        }

        data  = data(1).split(",")
        try{
            Array(0,1).foreach(i => mRNA_data += (header(i) -> mRNA_gene_name(i).replace(">","')")))
            mRNA_data += ("type" -> data.last)
            if(data.length > 1) mRNA_data += ("variant" -> data(0))
        }
        catch{case e: Exception => mRNA_data += ("RNA_name" -> info)}

        mRNA_data
    })


    /**
     * getRefSeq returns a DataFrame containing all mRNA information by the following operations:
     *   - the where function deletes all the NO-Homo sapiens mRNA as well as other RNA types.
     *   - the first withColumn replace the Homo sapiens word with "". In addition it does a split on double space
     *     so that the mRNA ID and the mRNA name are separated.
     *   - the select function replace the character ">" with "", in addition generate a DataFrame having the
     *     following column: RNA_id and info.
     *   - the second withColumn and the selectExpr functions are used to generate the DataFrame having all the
     *     mRNA data
     **/
    def getRefSeq(path: String, spark:SparkSession, type_rna: String = "mRNA"): DataFrame = {
        import spark.implicits._

        read_file(path, spark)
           .where($"_c0".contains(">") && $"_c0".contains(type_rna) && $"_c0".contains("Homo sapiens"))
           .withColumn("_c0", split(regexp_replace($"_c0", "Homo sapiens", ""), "\\s{2}"))
           .select(regexp_replace($"_c0"(0),">","").as("RNA_id"), $"_c0"(1).as("info"))
           .withColumn("info", create_mRNA_columns($"info", lit(header_mRNA)))
           .selectExpr(("RNA_id" +: header_mRNA.map(elem => "info." + elem)):_*)
           .sort("gene_name").filter($"gene_name".isNotNull)
           .where(!$"RNA_id".contains("PREDICTED"))
           .withColumn("RNA_id", split($"RNA_id", "\\.")(0))
    }

}
