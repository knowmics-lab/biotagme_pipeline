package Update_Module

import java.io.IOException
import java.nio.charset.{Charset, CodingErrorAction}
import java.util
import scala.io.Source
import com.lucidchart.open.xtract.XmlReader
import org.apache.http.{HttpException, NameValuePair, ProtocolException}
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicNameValuePair
import org.apache.log4j.Logger
import scala.xml.XML

object UpdateUtils {
  /**
   * Main xml parser function
   **/
  def articles_xml_parser(response: String):Seq[Article_final] = {
      val logger = Logger.getLogger("articles_xml_parser")
      var articles_list: Seq[Article_final] = Seq()
      try {
          val parsed_articles = XmlReader.of[Articles].read(XML.loadString(response))
          if (parsed_articles.isSuccessful) {
              articles_list = parsed_articles.toOption.get.articles.map(article => {
                  val article_data = Article_final(article.PMID.toLong, "", "")
                  article.ArticleTitle match {case Some(text) => article_data.Title = text}
                  article.AbstractText match {case Some(text_lst) => article_data.Abstract = text_lst.mkString(" ")}
                  article_data
              })
          }
      }
      catch {
         case ee: Exception =>
              logger.error(ee.getClass.toString + " => " + ee.getMessage)
      }
      articles_list
  }


  /**
   * The Pubmed_request function has been implemented to download from Pubmed server all the interested information.
   * The NCBI rest API have been used to do this. In addition, an opt option has been defined to to allow the
   * following two actions:
   *   - 0: download the pmids list that occur into a used defined temporal interval [mindate, maxdate]
   *   - 1: download document's title and abstract
   **/
  def Pubmed_request(opt:Int, map_inf: Map[String,String], param: String, start_data:String = ""): String = {
      var http_response: CloseableHttpResponse = null
      val http_client = HttpClientBuilder.create.build
      val http_post   = new HttpPost(if(opt==0) map_inf("url4pmids") else map_inf("url4abstract"))
      var content     = ""
      val logger      = Logger.getLogger("PubMed_equest logger")
      val entityPost  = new util.ArrayList[NameValuePair]

      try {
          entityPost.add(new BasicNameValuePair("db", "pubmed"))
          entityPost.add(new BasicNameValuePair("retmode", if(opt==0) "json" else "xml"))
          if (opt == 0) {
             entityPost.add(new BasicNameValuePair("mindate",  start_data))
             entityPost.add(new BasicNameValuePair("maxdate",  map_inf("end_update_data")))
             entityPost.add(new BasicNameValuePair("retmax" ,  map_inf("retmax")))
             entityPost.add(new BasicNameValuePair("retstart", param))
          }
          else
             entityPost.add(new BasicNameValuePair("id", param))

          http_post.setEntity(new UrlEncodedFormEntity(entityPost, "UTF-8"))
          http_response = http_client.execute(http_post)
          val status_code = http_response.getStatusLine.getStatusCode
          if (status_code != 200) {
              logger.error("Status code obtained is: " + status_code.toString)
              return content
          }

          val entity = http_response.getEntity
          if (entity != null) {
              val inputStream = entity.getContent
              val decoder = Charset.forName("UTF-8").newDecoder()
              decoder.onMalformedInput(CodingErrorAction.IGNORE)
              content = Source.fromInputStream(inputStream)(decoder).getLines.mkString("\n")
              inputStream.close()
          }
      }
      catch{
          case ioe:IOException         => logger.error(ioe.getClass.toString  + " => " + ioe.getMessage)
          case pre: ProtocolException  => logger.error(pre.getClass.toString + " => " + pre.getMessage)
          case hte: HttpException      => logger.error(hte.getClass.toString + " => " + hte.getMessage)
          case e:Exception             => logger.error("Generic: " + e.getClass.toString + " => " + e.getMessage)
      }

      http_client.close()
      content
  }
}
