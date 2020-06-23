package Databases_Module.Drug

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

trait DrugMainTrait {
    var drug_indexing: DataFrame = _

    /**
     * create_index_drugDF generates a DataFrame contains the id relationship among DrugBank and PharmGKB
     * It is called preindex because there will not be all database1 ids that have a corresponding one in
     * database2. So, a final full join among the created DataFrame and DrugBank, and PharmGKB id column
     * will be necessary to add the missing ids.
     **/
    private[this] def create_preindex_drugDF(refDBK:DataFrame, refPGKB:DataFrame):DataFrame = {
        refDBK.where(col("external_identifier_resource") === "PharmGKB")
              .selectExpr("drugbank_id", "external_identifier_identifier as PGKB_ID")
              .join(refPGKB.where(col("Cross_references").contains("DrugBank"))
                           .select(col("PGKB_ID"), split(col("Cross_references"),":")(1).as("drugbank_id")),
                    Seq("drugbank_id", "PGKB_ID"), "full"
              ).distinct
    }


    /**
     * create_drugs_indexing returns a DataFrame containing the DrugBank and PharmGKB associations as well as
     * an associated biotagme drug id
     **/
    private[this] def bioCollector(n:String,opt:Int = 1): Column = {
        if(opt == 0) collect_set(col(n))(0).as(n) else collect_set(col(n)).as(n)
    }

    def create_drugs_indexing(DBK:DataFrame, PGKB:DataFrame, refDBK:DataFrame, refPGKB:DataFrame): DataFrame = {
        var drugs_indexing: DataFrame = create_preindex_drugDF(refDBK, refPGKB)

        drugs_indexing = DBK.select("drugbank_id" ).distinct
           .join(drugs_indexing, Seq("drugbank_id"), "full")
           .join(PGKB.select("PGKB_ID").distinct, Seq("PGKB_ID"), "full")
           .withColumn("IDX", concat_ws("", lit("BTG_DRUG:"), monotonically_increasing_id()))

        DBK.join(drugs_indexing.select("drugbank_id","IDX"), "drugbank_id")
           .union(PGKB.join(drugs_indexing.select("PGKB_ID", "IDX"), "PGKB_ID"))
           .withColumnRenamed("drugbank_id", "SOURCE_ID")
           .distinct.groupBy("IDX")
           .agg(bioCollector("drug_name", opt=0), bioCollector("other_name"), bioCollector("SOURCE_ID"))
           .distinct
    }
}
