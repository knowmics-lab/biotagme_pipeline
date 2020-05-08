package Annotation_Module

import java.util
import java.io.IOException
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.http.{HttpException, NameValuePair, ProtocolException}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder, StandardHttpRequestRetryHandler}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.json4s._
import org.json4s.jackson.JsonMethods._

object AnnotationUtils {
  /**
   * The following set of user defined spark function are used to transform an array of string or string
   * to an array of bytes array or byte array. Basically:
   *   - arrayString2arrayBytes: convert an array of string to an array of bytes array
   *   - arrayBytes2String: convert an array of bytes to a string
   *   - string2arrayBytes: convert a string to an array of bites
   *   - when_other: return the transformed value of each row of the table
   * Since the parquet file only support the utf8 format, a transformation from string to array of bites
   * is necessary to avoid data format exception
   **/
  val arrayString2arrayBytes: UserDefinedFunction = udf((str_vet: Seq[String])   =>  str_vet.map(str => str.getBytes()))
  val arrayBytes2String: UserDefinedFunction      = udf((arrayByte:Array[Byte])  =>  new String(arrayByte))
  val string2arrayByte_udf: UserDefinedFunction   = udf((str:String)             =>  str.getBytes)

  def when_other(_col: Column, opt:Int) = {
      when(_col.isNull, null).otherwise(
          opt match {
                 case 0 => string2arrayByte_udf(_col)             // string to array of bites conversion
                 case 1 => arrayString2arrayBytes(_col)           // array of string to array of bites array conversion
          })
  }

  /**
   * The getNumPartition has been defined to get the appropriate number of partition to our documents DataFrame
   * on the basis of the following parameters:
   *   - n_row: number of documents
   *   - n_bytes_doc: average number of bytes per document. This value was set to 11571 thanks to a statistic analysis
   *   - executor_mem: memory percentage assigned to each spark executor
   *   - total_cores: CPUs assigned to each spark executor
   **/
   def getNumPartition(df:DataFrame, spark:SparkSession):Int = {
       // DataFrame and document information
       val n_row:       Long = df.count()
       val n_bytes_doc: Int  = 11571
       val tot_bytes:   Long = n_bytes_doc * n_row
       // spark executor information
       val executor_mem = spark.conf.get("spark.executor.memory")
       val total_cores  = spark.conf.get("spark.cores.max").toInt
       val molt_factor  = executor_mem.last.toLower match {
           case 'k' => scala.math.pow(10,3)
           case 'm' => scala.math.pow(10,6)
           case 'g' => scala.math.pow(10,9)
       }
       // final equation
       scala.math.ceil(tot_bytes/(executor_mem.dropRight(1).toDouble * molt_factor * total_cores)).toInt * total_cores
   }

  /**
   * The sendAnnotationRequest udf has been defined so that each joined document title-abstract can be sent to TAGME
   * server by a http post request. To achieve this purpose, we need to pass the TAGME token, language and epsilon
   * value as well as the URL and document information. Since the TAGME server could not return a response, a
   * connect timeout has been configured through apache http "setConnectTimeout" function to avoid endless wait.
   * In addition, a retransmission has been managed by the "RetryHandler" function to deal with temporary server
   * congestion or failure. Since the returned annotations are in a json format, the json4s api have been used to
   * do a json-scala object transformation.
   **/
   val sendAnnotationRequest: UserDefinedFunction = udf((parameters:Map[String,String], text: String, pmid:Long) => {
       val logger       = Logger.getLogger("Annotation procedure")
       val tagme_config = parameters
       // HTTP request body configuration
       val entityPost = new util.ArrayList[NameValuePair]
       entityPost.add(new BasicNameValuePair("gcube-token",tagme_config("token")))
       entityPost.add(new BasicNameValuePair("text", text))
       entityPost.add(new BasicNameValuePair("lang",tagme_config("language")))
       entityPost.add(new BasicNameValuePair("epsilon",tagme_config("epsilon")))
       entityPost.add(new BasicNameValuePair("include_categories",tagme_config("include_wikicategory")))

       // HTTP request building and setting
       var annotations: Annotations = Annotations(List():List[Annotation])
       try{
           logger.info("The annotation of the pmid: " + pmid.toString + " is just started !!!")
           val request_config: RequestConfig.Builder = RequestConfig.custom()
              .setConnectTimeout(60 * 1000)
              .setConnectionRequestTimeout(60 * 1000)
              .setSocketTimeout(60 * 1000)
           val httpClient: CloseableHttpClient = HttpClientBuilder.create()
              .setRetryHandler(new StandardHttpRequestRetryHandler(4, true))
              .build()

           /* Request/Response handler */
           val httpPost: HttpPost = new HttpPost(tagme_config("url"))
           httpPost.setEntity(new UrlEncodedFormEntity(entityPost, "UTF-8"))
           httpPost.setConfig(request_config.build())
           val response = httpClient.execute(httpPost)

           /* JSON-SCALA OBJECT parser */
           implicit val formats: DefaultFormats.type = DefaultFormats
           val doc_json = parse(EntityUtils.toString(response.getEntity))
           annotations  = doc_json.extract[Annotations]
           httpPost.clone()
           logger.info("annotation operation successfully completed !!")
       }
       catch {
           case ioe: IOException        => logger.error(ioe.getClass.toString  + " => " + ioe.getMessage)
           case pre: ProtocolException  => logger.error(pre.getClass.toString  + " => " + pre.getMessage)
           case hte: HttpException      => logger.error(hte.getClass.toString  + " => " + hte.getMessage)
       }

       /* return value */
       annotations
   })
}
