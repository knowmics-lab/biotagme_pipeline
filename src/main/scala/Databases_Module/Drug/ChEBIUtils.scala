package Databases_Module.Drug

import org.apache.spark.sql.{DataFrame, SparkSession}

object ChEBIUtils {
    val header2compound:   Seq[String]  = Seq("ID as COMPOUND_ID","SOURCE","PARENT_ID","NAME as DRUG_NAME")
    val header2names:      Seq[String]  = Seq("COMPOUND_ID","SOURCE","NAME as OTHER_NAME")
    val droppedINchemical: Seq[String]  = Seq("ID", "TYPE")
    val droppedINdbaccess: Seq[String]  = Seq("ID")
    val droppedINdrug_drug:Seq[String]  = Seq("ID", "STATUS")

    /**
     * read_csv function generates a DataFrame from a TSV file. In addition, when the req_drop is
     * set to true, all the specified columns are dropped. Instead, when this is set to false, all
     * the specified ones are selected.
    **/
    def read_tsv(path:String, spark:SparkSession, req_drop:Boolean, columns:Seq[String], header:String = "true"): DataFrame = {
        var DatFr = spark.read.format("csv")
            .option("header", header)
            .option("sep", "\t")
            .option("inferSchema", header)
            .load(path)

        if(req_drop) DatFr.drop(columns:_*) else DatFr.selectExpr(columns:_*)
    }

    /**
     * join_select function joins two DataFrame on the basis of the joinCol value. In addition, a pre-join DataFrame
     * selection can be done by setting sel1 and sel2 parameters.
    **/
    def join_select(DatFr1:DataFrame, DatFr2:DataFrame, joinCol:String, sel1:Seq[String]=null, sel2:Seq[String]=null):DataFrame = {
        (if(sel1 != null) DatFr1.selectExpr(sel1:_*) else DatFr1)
           .join(if(sel2 != null) DatFr2.selectExpr(sel2:_*) else DatFr2, joinCol)
    }

    /**
     * This function will select the DataFrame column on the basis of the passed user index
     **/
    def selector(idx:Int): Seq[String] = {
        idx match {
            case 0 => Seq("COMPOUND_ID", "DRUG_NAME")
            case 1 => Seq("COMPOUND_ID", "OTHER_NAME")
            case 2 => Seq("COMPOUND_ID", "CHEMICAL_DATA")
            case 3 => Seq("_c0 as COMPOUND_ID", "_c2 as Unip_NAME")
        }
    }
}
