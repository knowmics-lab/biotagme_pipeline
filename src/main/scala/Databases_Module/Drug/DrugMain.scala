package Databases_Module.Drug

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import DrugBankUtils._
import PharmGKBUtils._
import org.apache.spark.sql.functions._
import scala.collection.mutable


object DrugMain extends DrugMainTrait with DatabasesUtilsTrait{
    def get_drugs_DataFrames(spark: SparkSession, drug_conf: mutable.Map[String, Any]):Unit = {
        import spark.implicits._
        val paths = drug_conf("drug").asInstanceOf[mutable.Map[String, mutable.Map[String, String]]]

        /** DrugBank **/
        val drugbank_paths = paths(Fields.DRUGBANK_PATH)
        val dbr_root       = drugbank_paths(Fields.ROOT_PATH)
        val drugbank       = spark.read.option("rowTag", "drug").xml(dbr_root + "/*").persist()


        /** DataFrame necessary to build the indexing biological elements DataFrame **/
        val drug_extId  = get_drugs_otherInfo(drugbank, Fields.IDENTIFIER, spark, Fields.IDENTIFIER_PARAMS)
        val drug_mixt   = get_drugs_otherInfo(drugbank, Fields.MIXTURE,    spark, Fields.MIXTURE_PARAMS)
        val drug_synon  = get_drugs_otherInfo(drugbank, Fields.SYNONYM,    spark, Fields.SYNONYM_PARAMS)
        val drug_brand  = get_drugs_otherInfo(drugbank, Fields.BRAND,      spark, Fields.BRAND_PARAMS)
        val drugBank4indexing = drugbank
            .select($"`drugbank-id`"(0)("_VALUE").as("drugbank_id"), expr("name as DRUG_NAME"), expr("name as OTHER_NAME"))
            .union(drug_mixt).union(drug_synon).union(drug_brand).distinct

        /** Dataframes containing interactions **/
        var drug_drug     = get_drugs_otherInfo(drugbank, Fields.DRUG_INTERACTIONS, spark, Fields.DRUG_INTER_PARAMS)
            drug_drug     = drug_drug.selectExpr("drugbank_id as drugbank_id1", "drug_interaction_drugbank_id as drugbank_id2")
        val drug_enzymes  = get_drugs_component_polypeptides_gene(drugbank, Fields.ENZYME,      spark)
        val drug_targets  = get_drugs_component_polypeptides_gene(drugbank, Fields.TARGET,      spark)
        val drug_transp   = get_drugs_component_polypeptides_gene(drugbank, Fields.TRANSPORTER, spark)


        /** ChEBI **/
        /*
        // DataFrame necessary to build the indexing biological elements DataFrame
        var root_path     = "/DRUGS_COMPOUNDS/ChEBI/COMPOUNDS/"
        val compounds     = read_tsv(root_path + "compounds.tsv",          spark, req_drop=false, header2compound)
        val db_access     = read_tsv(root_path + "database_accession.tsv", spark, req_drop=true,  droppedINdbaccess)
        val names         = read_tsv(root_path + "names.tsv",              spark, req_drop=false, header2names)
        val chemical_data = read_tsv(root_path + "chemical_data.tsv",      spark, req_drop=true,  droppedINchemical)
        val chebi_unip = spark.read.format("csv").option("header", "false").option("sep", "\t").load(root_path + "chebi_uniprot.tsv")
        compounds.persist()
        val compound_dbaccess = join_select(compounds,db_access,"COMPOUND_ID", selector(0))
            .withColumnRenamed("COMPOUND_ID", "ChEBI_ID")
        val compounds_final   = compounds.selectExpr("COMPOUND_ID", "DRUG_NAME", "DRUG_NAME as OTHER_NAME")
            .union(join_select(compounds,names,         joinCol="COMPOUND_ID", selector(0), selector(1)))
            .union(join_select(compounds,chemical_data, joinCol="COMPOUND_ID", selector(0), selector(2)))
            .union(join_select(compounds,chebi_unip,    joinCol="COMPOUND_ID", selector(0), selector(3)))
            .withColumnRenamed("COMPOUND_ID", "ChEBI_ID")
            .where($"DRUG_NAME".isNotNull).distinct

        // Dataframes containing interactions
        val chebi_drug_drug = read_tsv(root_path + "relation.tsv", spark, req_drop=true, droppedINdrug_drug)
            .withColumnRenamed("COMPOUND_ID", "ChEBI_ID")
        */


        /** PharmGKB **/
        val pharm_paths = paths(Fields.PGKB_PATH)
        val phm_root    = pharm_paths(Fields.ROOT_PATH)
        val pharm_drugs = pharm_loader(phm_root, spark).persist()
        val pharm_ref   = columns_group(pharm_drugs, Fields.CROSS_REF)
        val pharm_final = pharm_drugs.selectExpr(Fields.PGKB_ID, Fields.PGKB_DRUG_NAME, Fields.PGKB_OTHER_NAME).
            union(columns_group(pharm_drugs, Fields.GENERIC_NAME)).
            union(columns_group(pharm_drugs, Fields.TRADE_NAME)).
            union(columns_group(pharm_drugs, Fields.BRAND_MIXTURE))


        /** Indexing **/
        val sav_root_path = paths("drug_metadata")("path")
        drug_indexing     = create_drugs_indexing(drugBank4indexing, pharm_final, drug_extId, pharm_ref)
        drug_indexing.persist
        drug_indexing.write.mode("overwrite").parquet(sav_root_path + "/Drug_indexing")


        /** Relationships **/
        create_belem_belem_relationships(drug_indexing, drug_drug, "drug")
           .union(create_relationships(drug_enzymes, Fields.DRUG_ID, Fields.DRUG_NAME, drug_indexing, Fields.DRUG_ENZYME,      Fields.ENZYME_NAME, Fields.ENZYME_ID))
           .union(create_relationships(drug_enzymes, Fields.DRUG_ID, Fields.DRUG_NAME, drug_indexing, Fields.DRUG_GENE,        Fields.ENZYME_POLYP))
           .union(create_relationships(drug_targets, Fields.DRUG_ID, Fields.DRUG_NAME, drug_indexing, Fields.DRUG_TARGET,      Fields.TARGET_NAME, Fields.TARGET_ID))
           .union(create_relationships(drug_targets, Fields.DRUG_ID, Fields.DRUG_NAME, drug_indexing, Fields.DRUG_GENE,        Fields.TARGET_POLYP))
           .union(create_relationships(drug_transp,  Fields.DRUG_ID, Fields.DRUG_NAME, drug_indexing, Fields.DRUG_TRANSPORTER, Fields.TRANSPORTER_NAME, Fields.TRANSPORTER_ID))
           .union(create_relationships(drug_transp,  Fields.DRUG_ID, Fields.DRUG_NAME, drug_indexing, Fields.DRUG_GENE,        Fields.TRANSPORTER_POLYP))
           .write.mode("overwrite").parquet(sav_root_path + "/Drug_relationships")

        drugbank.unpersist; pharm_drugs.unpersist; drug_indexing.unpersist()
    }
}
