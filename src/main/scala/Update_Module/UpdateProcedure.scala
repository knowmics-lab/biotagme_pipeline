package Update_Module

import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.apache.log4j.Logger
import scala.xml.Elem
import UpdateUtils._
import org.json4s._
import org.json4s.jackson.JsonMethods._

object UpdateProcedure extends UpdateTrait {

     /**
       * Sicne the articles into the Pubmed database increase every days, it is important add all the new
       * documents title and abstract into the local one. To reach this purpose we implement the mainUpdater
       * function based on NCBI restfull API. We decided to use POST method so that many documents data can
       * be download through a single request.
       * NOTE: The requests to do are the following:
       *   - the first one (selected by the zero option of the Pubmed_request function) is used to download
       *     all the new pmids published over the [last_update_date, actual date] period
       *   - the second one (selected by the option 1) allows to download the documents information having a
       *     pmid belong to the pmids list previously obtained.
       **/
     override def mainUpdater(conf_xml_obj: Elem, spark: SparkSession): Unit = {
         import spark.implicits._
         implicit val formats: DefaultFormats.type = DefaultFormats

         val pubMed_info: Map[String, String]  = getPubmedANDpaths(conf_xml_obj)
         var n_articles_todownload: Long       = 0l
         var n_articles_downloaded: Long       = pubMed_info("retstart").toLong
         val logger                            = Logger.getLogger("PubMed update procedure")
         val timestamp                         = Calendar.getInstance().getTime
         val dateFormat                        = new SimpleDateFormat("yyyy/MM/dd")
         var response                          = ""

         if(pubMed_info("newDocuments_path") == "") {
            logger.error("You are defined a bad path!!! check the newDocuments_pat tag within the file biotagme.xml")
            return
         }

         do{
             response = Pubmed_request(0, pubMed_info, n_articles_downloaded.toString)
             if(response != ""){
                 val pmids: Pmids = parse(response).extract[Pmids]
                 if(pmids.esearchresult.idlist.nonEmpty){
                     val pmid_list: Seq[String] = pmids.esearchresult.idlist
                     n_articles_todownload = pmids.esearchresult.count.toLong
                     n_articles_downloaded += pmid_list.length
                     logger.info("number of pmids to download: " + n_articles_todownload)
                     logger.info("number of pmids downloaded: "  + n_articles_downloaded)
                     logger.info("The title and abstract of the following pmids will be downloaded: " + pmid_list)
                     response = Pubmed_request(1, pubMed_info, pmid_list.mkString(","))
                     if(response != "") {
                         val articles = articles_xml_parser(response)
                         articles.toDS().write.mode("append").json(pubMed_info("newDocuments_path"))
                         logger.info("pmids abstract and title have been downloaded...")
                     }
                     else logger.error("the response related to title and abstract is empty")
                 }
             }
             Thread.sleep(1000)
         }
         while(n_articles_downloaded < n_articles_todownload)
         val last_upd_path = System.getProperty("user.dir") + "/src/main/configuration/last_update.txt"
         Files.write(Paths.get(last_upd_path), dateFormat.format(timestamp).getBytes)
     }
}
