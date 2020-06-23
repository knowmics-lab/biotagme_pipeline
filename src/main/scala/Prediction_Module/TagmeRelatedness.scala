package Prediction_Module

import java.io.IOException
import org.apache.http.{HttpException, NameValuePair, ProtocolException}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.JsonMethods._


object TagmeRelatedness {
    var config_par:Broadcast[Map[String,String]] = _

    /**
     * The httpPost_relatedness user define function is used to get the relatedness of each ti-tj couples.
     * Note that:
     *   - We use a single POST request to get the relatedness of 100 tuples.
     **/
    val httpPost_relatedness: UserDefinedFunction = udf((tuples: Seq[String]) => {
        val logger     = Logger.getLogger("Tagme relatedness procedure")

        // POST body configuration
        val entityPost = new java.util.ArrayList[NameValuePair]
        entityPost.add(new BasicNameValuePair("gcube-token", config_par.value("token")))
        entityPost.add(new BasicNameValuePair("lang",        config_par.value("language")))
        tuples.map(tuple => entityPost.add(new BasicNameValuePair("id", tuple)))

        var relatedness_list: Seq[Relatedness] = Seq()
        try{
            logger.info("relatedness computation of the following tuple: " + tuples.mkString(";"))
            val request_config = RequestConfig.custom()
                .setConnectTimeout(60 * 1000)
                .setConnectionRequestTimeout(60 * 1000)
                .setSocketTimeout(60 * 1000)
            val httpClient  = HttpClientBuilder.create.build
            val httpPost    = new HttpPost(config_par.value("url_rel"))
            httpPost.setEntity(new UrlEncodedFormEntity(entityPost, "UTF-8"))
            httpPost.setConfig(request_config.build())
            val response    = httpClient.execute(httpPost)

            // JSON to Scala Object parser
            implicit val formats: DefaultFormats.type = DefaultFormats
            val json_resp = parse(EntityUtils.toString(response.getEntity))
            relatedness_list = json_resp.extract[Tuples_Relatedness].result
            logger.info("relatedness of the considered tupled has been completed..")
            httpClient.close()
        }
        catch{
            case ioe: IOException        => logger.error(ioe.getClass.toString  + " => " + ioe.getMessage)
            case pre: ProtocolException  => logger.error(pre.getClass.toString  + " => " + pre.getMessage)
            case hte: HttpException      => logger.error(hte.getClass.toString  + " => " + hte.getMessage)
            case e: Exception            => logger.error("description of the generic error: " + e.toString)
        }
        relatedness_list
    })


}
