package Databases_Module.Drug

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import DrugBankUtils._
import PharmGKBUtils._
import org.apache.spark.sql.functions._

object DrugMain extends DrugMainTrait with DatabasesUtilsTrait{
    def get_drugs_DataFrames(spark: SparkSession):Unit = {
        import spark.implicits._

        /** DrugBank **/
        val drugbank    = spark.read.option("rowTag", "drug").xml("/Drug/DrugBank/drugBank_db.xml").persist()

        // DataFrame necessary to build the indexing biological elements DataFrame
        val drug_extId  = get_drugs_otherInfo(drugbank, "external-identifiers.external-identifier", spark, Seq("identifier","resource"))
        val drug_mixt   = get_drugs_otherInfo(drugbank, "mixtures.mixture", spark, Seq("name"))
        val drug_synon  = get_drugs_otherInfo(drugbank, "synonyms.synonym", spark, Seq("_VALUE"))
        val drug_brand  = get_drugs_otherInfo(drugbank, "international-brands.international-brand", spark, Seq("name"))
        val drugBank4indexing = drugbank
            .select($"`drugbank-id`"(0)("_VALUE").as("drugbank_id"), expr("name as DRUG_NAME"), expr("name as OTHER_NAME"))
            .union(drug_mixt).union(drug_synon).union(drug_brand).distinct


        // Dataframes containing interactions
        var drug_drug     = get_drugs_otherInfo(drugbank, "drug-interactions.drug-interaction", spark, Seq("drugbank-id","name"))
            drug_drug     = drug_drug.selectExpr("drugbank_id as drugbank_id1", "drug_interaction_drugbank_id as drugbank_id2")
        val drug_enzymes  = get_drugs_component_polypeptides_gene(drugbank, "enzymes.enzyme",   spark)
        val drug_targets  = get_drugs_component_polypeptides_gene(drugbank, "targets.target",   spark)
        val drug_transp   = get_drugs_component_polypeptides_gene(drugbank, "transporters.transporter", spark)


        /*
        /** ChEBI **/
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
        val root_path   = "/Drug/PharmGKB"
        val pharm_drugs = pharm_loader(root_path, spark).persist()
        val pharm_ref   = columns_group(pharm_drugs, "Cross_references")
        val pharm_final = pharm_drugs.selectExpr("PGKB_ID", "DRUG_NAME", "DRUG_NAME as OTHER_NAME").
            union(columns_group(pharm_drugs, "Generic_Name")).
            union(columns_group(pharm_drugs, "Trade_Names")).
            union(columns_group(pharm_drugs, "Brand_Mixtures"))


        /** Indexing **/
        drug_indexing = create_drugs_indexing(drugBank4indexing, pharm_final, drug_extId, pharm_ref)
        drug_indexing.persist
        drug_indexing.write.mode("overwrite").parquet("/Drug/Drug_indexing")

        /** Relationships **/
        create_belem_belem_relationships(drug_indexing, drug_drug, "drug")
           .union(create_relationships(drug_enzymes, "drugbank_id", "drug_name", drug_indexing, "drug-enzyme", "enzyme_name", "enzyme_id"))
           .union(create_relationships(drug_enzymes, "drugbank_id", "drug_name", drug_indexing, "drug-gene",   "enzyme_polypeptide_gene_name"))
           .union(create_relationships(drug_targets, "drugbank_id", "drug_name", drug_indexing, "drug-gene",   "target_name", "target_id"))
           .union(create_relationships(drug_targets, "drugbank_id", "drug_name", drug_indexing, "drug-gene",   "target_polypeptide_gene_name"))
           .union(create_relationships(drug_transp,  "drugbank_id", "drug_name", drug_indexing, "drug-transporter", "transporter_name", "transporter_id"))
           .union(create_relationships(drug_transp,  "drugbank_id", "drug_name", drug_indexing, "drug-gene",   "transporter_polypeptide_gene_name"))
           .write.mode("overwrite").parquet("/Drug/Drug_relationships")

        drugbank.unpersist; pharm_drugs.unpersist; drug_indexing.unpersist()
    }
}
