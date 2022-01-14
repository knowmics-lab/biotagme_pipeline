package Databases_Module.Drug

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

trait DrugMainTrait {
    var drug_indexing: DataFrame = _

    /** CONSTANT **/
    val DRUGBANK_REFERENCE   = "external_identifier_resource"
    val DRUG_ID              = "drugbank_id"
    val DRUGBANK_REF_RENAME  = "external_identifier_identifier as PGKB_ID"
    val PGKB_REFERENCE       = "Cross_references"
    val PGKB_ID              = "PGKB_ID"
    val PGKB                 = "PharmGKB"
    val DRUGBANK             = "DrugBank"
    val PREFIX_BIOTG_ID      = "BTG_DRUG:"
    val INDEX_COL            = "IDX"
    val SOURCE_COL           = "SOURCE_ID"
    val DRUG_NAME            = "drug_name"
    val OTHER_NAME           = "other_name"


    /**
     * create_index_drugDF generates a DataFrame contains the id relationship among DrugBank and PharmGKB
     * It is called preindex because there will not be all database1 ids that have a corresponding one in
     * database2. So, a final full join among the created DataFrame and DrugBank, and PharmGKB id column
     * will be necessary to add the missing ids.
     **/
    private[this] def create_preindex_drugDF(refDBK:DataFrame, refPGKB:DataFrame):DataFrame = {
        refDBK.where(col(DRUGBANK_REFERENCE) === PGKB)
              .selectExpr(DRUG_ID, DRUGBANK_REF_RENAME)
              .join(refPGKB.where(col(PGKB_REFERENCE).contains(DRUGBANK))
                    .select(col(PGKB_ID), split(col(PGKB_REFERENCE),":")(1).as(DRUG_ID)),
                    Seq(DRUG_ID, PGKB_ID), joinType = "full"
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
        drugs_indexing = DBK.select(DRUG_ID).distinct
           .join(drugs_indexing, Seq(DRUG_ID), joinType="full")
           .join(PGKB.select(PGKB_ID).distinct, Seq(PGKB_ID), joinType="full")
           .withColumn(INDEX_COL, concat_ws(sep="", lit(PREFIX_BIOTG_ID), monotonically_increasing_id()))

        DBK.join(drugs_indexing.select(DRUG_ID, INDEX_COL), DRUG_ID)
           .union(PGKB.join(drugs_indexing.select(PGKB_ID, INDEX_COL), PGKB_ID))
           .withColumnRenamed(DRUG_ID, SOURCE_COL)
           .distinct.groupBy(INDEX_COL)
           .agg(bioCollector(DRUG_NAME, opt=0), bioCollector(OTHER_NAME), bioCollector(SOURCE_COL))
           .distinct
    }
}
