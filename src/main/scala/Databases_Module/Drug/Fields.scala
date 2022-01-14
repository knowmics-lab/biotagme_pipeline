package Databases_Module.Drug

object Fields extends Enumeration {
    type Field = Value

    val ROOT_PATH:          String      = "root_path"

    /** DrugBank **/
    val DRUGBANK_PATH:      String      = "drugbank_path"
    val IDENTIFIER:         String      = "external-identifiers.external-identifier"
    val IDENTIFIER_PARAMS:  Seq[String] = Seq("identifier","resource")
    val MIXTURE:            String      = "mixtures.mixture"
    val MIXTURE_PARAMS:     Seq[String] = Seq("name")
    val SYNONYM:            String      = "synonyms.synonym"
    val SYNONYM_PARAMS:     Seq[String] = Seq("_VALUE")
    val BRAND:              String      = "international-brands.international-brand"
    val BRAND_PARAMS:       Seq[String] = Seq("name")
    val DRUG_INTER:         String      = "drug-interactions.drug-interaction"
    val ENZYME:             String      = "enzymes.enzyme"
    val TARGET:             String      = "targets.target"
    val TRANSPORTER:        String      = "transporters.transporter"
    val DRUG_ID:            String      = "drugbank_id"
    val DRUG_NAME:          String      = "drug_name"
    val ENZYME_ID:          String      = "enzyme_id"
    val ENZYME_NAME:        String      = "enzyme_name"
    val DRUG_ENZYME:        String      = "drug-enzyme"
    val ENZYME_POLYP:       String      = "enzyme_polypeptide_gene_name"
    val TARGET_ID:          String      = "target_id"
    val TARGET_NAME:        String      = "target_name"
    val DRUG_TARGET:        String      = "drug-target"
    val TARGET_POLYP:       String      = "target_polypeptide_gene_name"
    val TRANSPORTER_ID:     String      = "transporter_id"
    val TRANSPORTER_NAME:   String      = "transporter_name"
    val DRUG_TRANSPORTER:   String      = "drug-transporter"
    val TRANSPORTER_POLYP:  String      = "transporter_polypeptide_gene_name"
    val DRUG_GENE:          String      = "drug-gene"
    val DRUG_INTERACTIONS:  String      = "drug-interactions.drug-interaction"
    val DRUG_INTER_PARAMS:  Seq[String] = Seq("drugbank-id","name")

    /** PharmGKB **/
    val PGKB_PATH:          String      = "pharmkb_path"
    val CROSS_REF:          String      = "Cross_references"
    val PGKB_ID:            String      = "PGKB_ID"
    val PGKB_DRUG_NAME:     String      = "DRUG_NAME"
    val PGKB_OTHER_NAME:    String      = "DRUG_NAME as OTHER_NAME"
    val GENERIC_NAME:       String      = "Generic_Name"
    val TRADE_NAME:         String      = "Trade_Names"
    val BRAND_MIXTURE:      String      = "Brand_Mixtures"
}
