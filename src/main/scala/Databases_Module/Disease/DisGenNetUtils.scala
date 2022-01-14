package Databases_Module.Disease

import Databases_Module.Drug.ChEBIUtils.read_tsv
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DisGenNetUtils {
    val header_gen_dis = Seq(
        "geneSymbol          as gene_name",
        "diseaseId           as disease_dgn_id",
        "lower(diseaseName)  as disease_name",
        "diseaseType"
    )

    val header_var_dis = Seq(
        "snpId               as snp_id",
        "diseaseId           as disease_dgn_id",
        "lower(diseaseName)  as disease_name",
        "source",
        "diseaseType"
    )

    val header_dis_dis = Seq(
        "diseaseId1             as disease_dgn_id1",
        "diseaseId2             as disease_dgn_id2",
        "lower(diseaseId1_name) as disease_name1",
        "lower(diseaseId2_name) as disease_name2"
    )

    val header_mapping = Seq(
        "diseaseId       as disease_dgn_id",
        "lower(name)     as disease_name",
        "vocabularyName  as other_name"
    )


    /**
     * this function is invoked to get all information about disease and logical relations among genes,variants,
     * and disease. Therefore:
     *   - case 0 : return a DataFrame containing all gene-disease associations
     *   - case 1 : return a DataFrame containing all variant-disease associations
     *   - case 2 : return a DataFrame containing all disease-disease associations
     **/
    def getDisGenNet(path: String, spark:SparkSession, flag:Int):DataFrame = {
        import spark.implicits._
        val header = flag match {
            case 0 => header_gen_dis
            case 1 => header_var_dis
            case 2 => header_dis_dis
            case 3 => header_mapping
        }

        var disease = read_tsv(path, spark, req_drop=false, header)
        if(flag <2)
           disease = disease.filter($"diseaseType" =!= "phenotype").drop("phenotype")

        disease.distinct
    }


    /**
     * build_synonyms is a user defined function that allows to write the disease name in a different way.
     * For example, if the DisGenNET name is Lymphohistiocytosis, Hemophagocytic then a new DataFrame contains
     * the name Hemophagocytic Lymphohistiocytosis will be created. At this regard, we can increase the DisGenNET
     * DO associations.
     **/
    val build_synonyms: UserDefinedFunction = udf((name: String) => {
        var spl_name: Seq[String] = Seq()

        if(!name.toLowerCase().contains("and")) {
            spl_name = name.split(", ")
            if(spl_name.size > 1){
                val digit_part = ("""\d+\w*""".r findAllIn spl_name.last).toList
                if(digit_part.isEmpty)
                    spl_name = Seq(spl_name.reverse.mkString(" "))
                else
                    spl_name = Seq(spl_name.dropRight(1).reverse.mkString(" ") + " " + spl_name.last)
            }
        }

        spl_name :+ name
    })


    /**
     * The following function returns a DataFrame to be used during the indexing procedure
     **/
    def getDataFrame2Indexing(dis_dis: DataFrame, df_seq: DataFrame*):DataFrame = {
        var df_indexing = dis_dis.selectExpr("disease_dgn_id1 as disease_dgn_id", "disease_name1 as disease_name")
            .union(dis_dis.selectExpr("disease_dgn_id2 as disease_dgn_id", "disease_name2 as disease_name"))
            .distinct

        df_seq.foreach(df => df_indexing = df_indexing.union(df.select("disease_dgn_id", "disease_name")))
        /*
        df_indexing.distinct.select(df_indexing.columns.map(c => col(c)) :+
            explode(build_synonyms(col("disease_name"))).as("other_name"):_*
        ).distinct
        */
        df_indexing.distinct.selectExpr("disease_dgn_id", "disease_name", "disease_name as other_name").distinct
    }


    def remove_phenotypes(gen_dis_df:DataFrame, var_dis_df:DataFrame, df:DataFrame, opt:Int):DataFrame = {
        val sel_id = gen_dis_df.select("disease_dgn_id").union(var_dis_df.select("disease_dgn_id")).distinct
        if(opt == 1)
            df.join(sel_id,"disease_dgn_id")
        else{
            df.join(
                 sel_id.withColumnRenamed("disease_dgn_id", "disease_dgn_id1"),
                 usingColumn = "disease_dgn_id1"
            ).join(
                 sel_id.withColumnRenamed("disease_dgn_id", "disease_dgn_id2"),
                 usingColumn = "disease_dgn_id2"
            )
        }
    }
}
