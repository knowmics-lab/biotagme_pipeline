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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable


object TagmeRelatedness {

    /**
     * The httpPost_relatedness user define function is used to get the relatedness of each ti-tj couples.
     * Note that:
     *   - We use a single POST request to get the relatedness of 100 tuples.
     **/
    private[this] val httpPost_relatedness: UserDefinedFunction = udf((tuples: Seq[String], token:String, url:String) => {
        val logger     = Logger.getLogger("Tagme relatedness procedure")

        // POST body configuration
        val entityPost = new java.util.ArrayList[NameValuePair]
        entityPost.add(new BasicNameValuePair("gcube-token", token))
        entityPost.add(new BasicNameValuePair("lang",  "en"))
        tuples.map(tuple => entityPost.add(new BasicNameValuePair("id", tuple)))

        var relatedness_list: Seq[Relatedness] = Seq()
        try{
            logger.info("relatedness computation of the following tuple: " + tuples.mkString(";"))
            val request_config = RequestConfig.custom()
                .setConnectTimeout(60 * 1000)
                .setConnectionRequestTimeout(60 * 1000)
                .setSocketTimeout(60 * 1000)
            val httpClient  = HttpClientBuilder.create.build
            val httpPost    = new HttpPost(url)
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


    /**
      * main_tagme_relatedness allows the user to get the relatedness of each wid_i-wid_j couple obtained
      * during the documents annotations. Since TagME can work with a block of 100 couples at most and at the
      * same time, we have used the Spark windows concept to build such groups.
      * Note that:
      *   - the first withColumn add a group index usinf the Spark row_number and floor function
      **/
    def main_tagme_relatedness(spark: SparkSession, map_conf: mutable.Map[String, Any], opt: Int): Unit = {
        import spark.implicits._

        val paths       = map_conf("hdfs_paths").asInstanceOf[mutable.Map[String, String]]
        val tagme_param = map_conf("tagme_info").asInstanceOf[mutable.Map[String, String]]

        var wid1_wid2_dt = spark.read.parquet(paths("wid1_wid2_path") + "/*")
        if(opt == 0){
            val prev_tagme_rel = spark.read.parquet(paths("tagme_relatedness") + "/*")
            wid1_wid2_dt = wid1_wid2_dt.join(prev_tagme_rel, Seq("wid1", "wid2"), "leftanti")

            if(wid1_wid2_dt.head(1).isEmpty) return
        }

        val window_idx = Window.partitionBy("index").orderBy("index")
        wid1_wid2_dt = wid1_wid2_dt.where($"wid1" =!= $"wid2")
           .select(concat_ws(" ", $"wid1", $"wid2") as "tuple", lit(0) as "index")
           .withColumn("index", floor(row_number().over(window_idx) / lit(100.0)))
           .groupBy("index").agg(collect_list($"tuple") as "tuples")
           .select(httpPost_relatedness($"tuples", lit(tagme_param("token")), lit(tagme_param("url_rel"))) as "relatedness")
           .withColumn("relatedness", explode($"relatedness"))
           .select(
               split($"relatedness".getItem("couple"), " ") as "couple",
               $"relatedness".getItem("rel") as "tag_relatedness"
           )
           .select($"couple"(0) as "wid1", $"couple"(1) as "wid2", $"tag_relatedness")

        wid1_wid2_dt.show(200)

        if(opt == 0) wid1_wid2_dt.write.mode("append").parquet(paths("tagme_relatedness"))
        else         wid1_wid2_dt.write.mode("overwrite").parquet(paths("tagme_relatedness"))
    }

}
