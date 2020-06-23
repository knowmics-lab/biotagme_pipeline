package Databases_Module.Drug

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object DrugBankUtils {
   /**
    * This function has been implemented to get the drug-elements-polypeptides-gene associations in DrugBank.
    * elements can assume the following values:
    *   - enzymes.enzyme,
    *   - carriers.carrier,
    *   - targets.target,
    *   -transporters.transporter
   **/
    def get_drugs_component_polypeptides_gene(data:DataFrame, component_name:String, spark:SparkSession): DataFrame = {
        import spark.implicits._
        val component = component_name.split("\\.")(1)

        /** 1. drugbank_id, drug_name and element part selection **/
        var d_c_p_e = data
            .select(
               $"`drugbank-id`"(0)("_VALUE").as("drugbank_id"),
               $"name".as("drug_name"),
               explode(col(component_name)).as(component)
            )

        /** 2. element part explosion **/
        d_c_p_e = d_c_p_e
            .selectExpr(
                d_c_p_e.columns.filter(c => c != component) ++
                Seq(
                    component  + ".id as "       + component + "_id",
                    component  + ".name as "     + component + "_name",
                    component  + ".organism as " + component + "_organism",
                    "explode(" + component + ".polypeptide) as " + component + "_polypeptide"
                ):_*)
            .where(col(component + "_organism").contains("Humans"))
            .drop(component + "_organism")

        /** 3. polypeptide part explosion **/
        d_c_p_e = d_c_p_e
             .selectExpr(
                 d_c_p_e.columns.filter(c => c != component + "_polypeptide") ++
                 Seq(
                     component + "_polypeptide._id as "         + component + "_polypeptide_id",
                     component + "_polypeptide.name as "        + component + "_polypeptide_name",
                     component + "_polypeptide._source as "     + component + "_polypeptide_source",
                     component + "_polypeptide.`gene-name` as " + component + "_polypeptide_gene_name",
                     component + "_polypeptide.`external-identifiers` as " + component + "_polypeptide_ext_id",
                     "explode("+ component + "_polypeptide.synonyms.`synonym`) as " + component + "_polypeptide_synonym"
                 ):_*)
            .withColumn(component + "_polypeptide_ext_id", explode(col(component + "_polypeptide_ext_id.`external-identifier`")))
            .withColumn(component + "_polypeptide_ext_id_ref", col(component     + "_polypeptide_ext_id.identifier"))
            .withColumn(component + "_polypeptide_ext_id_res", col(component     + "_polypeptide_ext_id.resource"))
            .drop(component + "_polypeptide_ext_id")

        d_c_p_e
    }


    def get_drugs_otherInfo(data:DataFrame, other:String, spark:SparkSession, final_par:Seq[String]): DataFrame = {
        import spark.implicits._
        val alias = other.split("\\.")(1).replace("-", "_")

        val drugs_other = data.select(
            $"`drugbank-id`".getItem(0).getItem("_VALUE").as("drugbank_id"),
            $"name".as("drug_name"),
            explode(col(other)).as(alias)
        )

        drugs_other.selectExpr(
            drugs_other.columns.filter(c => c != alias) ++
            final_par.map(c => alias + ".`" + c + "` as " + alias + "_" + c.replace("-","_")):_*
        ).distinct()
    }

}
