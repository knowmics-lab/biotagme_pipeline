package Databases_Module.Protein

import org.apache.spark.sql.{DataFrame, SparkSession}
import Databases_Module.Drug.ChEBIUtils.read_tsv

object BioGridUtils {
    val header_biogrid = Seq(
        "`#BioGRID Interaction ID`      as biogrid_interaction_id",
        "`Entrez Gene Interactor A`     as entrez_gene_inter_a",
        "`BioGRID ID Interactor A`      as biogrid_interactor_a",
        "`Systematic Name Interactor A` as systematic_name_a",
        "`Official Symbol Interactor A` as official_symbol_a",
        "`Synonyms Interactor A`        as synonyms_interactor_a",
        "`Entrez Gene Interactor B`     as entrez_gene_inter_b",
        "`BioGRID ID Interactor B`      as biogrid_interactor_b",
        "`Systematic Name Interactor B` as systematic_name_b",
        "`Official Symbol Interactor B` as official_symbol_b",
        "`Synonyms Interactor B`        as synonyms_interactor_b",
        "`Organism Name Interactor A`   as organism_a",
        "`Organism Name Interactor B`   as organism_b",
        "`SWISS-PROT Accessions Interactor A` as swiss_prot_accession_a",
        "`SWISS-PROT Accessions Interactor B` as swiss_prot_accession_b",
        "`REFSEQ Accessions Interactor A` as refseq_accession_a",
        "`REFSEQ Accessions Interactor B` as refseq_accession_b"
    )

    def getBiogrid(path:String, spark: SparkSession, species_a:String="Homo sapiens", species_b:String="Homo sapiens"): DataFrame = {
        import spark.implicits._
        read_tsv(path, spark, req_drop=false, header_biogrid)
           .where($"organism_a" === species_a && $"organism_b" === species_b)
           .drop("organism_a", "organism_b")
    }

}
