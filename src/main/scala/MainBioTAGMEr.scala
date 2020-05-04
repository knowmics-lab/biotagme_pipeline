import java.nio.charset.{Charset, CodingErrorAction}
import java.util

import Update_Module.UpdateUtils._
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicNameValuePair

import scala.io.Source

object MainBioTAGMEr {
  def main(args: Array[String]): Unit = {

    val URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

    val entityPost = new util.ArrayList[NameValuePair]
    entityPost.add(new BasicNameValuePair("db","pubmed"))
    entityPost.add(new BasicNameValuePair("retmode","json"))
    entityPost.add(new BasicNameValuePair("mindate","2020/01/01"))
    entityPost.add(new BasicNameValuePair("maxdate","3000"))
    entityPost.add(new BasicNameValuePair("retmax","1000"))

    val http_client    = HttpClientBuilder.create().build()
    val http_post      = new HttpPost(URL)
    http_post.setEntity(new UrlEncodedFormEntity(entityPost, "UTF-8"))
    val response       = http_client.execute(http_post)

    val entity = response.getEntity
    val inputStream = entity.getContent
    val decoder = Charset.forName("UTF-8").newDecoder()
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    val content = Source.fromInputStream(inputStream)(decoder).getLines.mkString("\n")

    print(content)


    /*
    val ids = 15000000 to 15000010
    val URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    val entityPost = new util.ArrayList[NameValuePair]

    entityPost.add(new BasicNameValuePair("db","pubmed"))
    entityPost.add(new BasicNameValuePair("id", ids.mkString(",")))
    entityPost.add(new BasicNameValuePair("retmode","xml"))

    val http_client    = HttpClientBuilder.create().build()
    val http_post      = new HttpPost(URL)
    http_post.setEntity(new UrlEncodedFormEntity(entityPost, "UTF-8"))
    val response       = http_client.execute(http_post)

    val entity = response.getEntity
    val inputStream = entity.getContent
    val decoder = Charset.forName("UTF-8").newDecoder()
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    val content = Source.fromInputStream(inputStream)(decoder).getLines.mkString("\n")
    inputStream.close()

    print(articles_xml_parser(content))


    http_client.close()
  */
  }
}
