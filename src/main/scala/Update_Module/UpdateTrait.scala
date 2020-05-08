package Update_Module

import org.apache.spark.sql.SparkSession
import scala.io.Source
import scala.xml.{Elem, Node}

trait UpdateTrait {

    def addTuple(key: String, value: Node):(String, String) = key -> value.child(3).text.replaceAll("[\\s+|\n]","")


    /**
      * This function has been implemented to get all parameters that will be used during the  Pubmed new pmids, and
      * consequently title and abstract, downloading phase. In addition, these new data will be stored in a hdf directory
      * specified under the newDocuments_path xml element.
      **/
    def getPubmedANDpaths(conf_xml:Elem):Map[String,String] = {
        val last_upd_path = System.getProperty("user.dir") + "/src/main/configuration/last_update.txt"
        var pubmed_params: Map[String, String] = Map()
        conf_xml.child.foreach(elem_0 =>
            elem_0.label match {
                case "pubmed_parameters" =>
                     elem_0.child.foreach(elem_1 => elem_1.label match {
                         case "url4pmids"        => pubmed_params = pubmed_params + addTuple("url4pmids", elem_1)
                         case "url4abstract"     => pubmed_params = pubmed_params + addTuple("url4abstract", elem_1)
                         case "end_update_date"  => pubmed_params = pubmed_params + addTuple("end_update_date", elem_1)
                         case "retmax"           => pubmed_params = pubmed_params + addTuple("retmax", elem_1)
                         case "retstart"         => pubmed_params = pubmed_params + addTuple("retstart", elem_1)
                         case _                  => null
                     })
                case "hdfs_paths" =>
                     elem_0.child.foreach(elem_1  => elem_1.label match {
                         case "newDocuments_path" => pubmed_params = pubmed_params + addTuple("newDocuments_path", elem_1)
                         case _                   => null
                     })
                case _ => null
            }
        )

        val text_pointer = Source.fromFile(last_upd_path)
        pubmed_params    = pubmed_params + ("last_update_date" -> text_pointer.getLines().next())
        text_pointer.close()
        pubmed_params
    }

  /**
    * Main Upload procedure
    **/
    def mainUpdater(conf_xml_obj: Elem, spark: SparkSession)

}
