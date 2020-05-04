package Update_Module

import com.lucidchart.open.xtract.{XmlReader,__}
import com.lucidchart.open.xtract.XmlReader.seq
import cats.syntax.all._

/**
 * The Article class is necessary to parse the xml part related to a Pubmed article in a corresponding scala
 * object. This operation is implemented using the lucidchart api
 **/
case class Article(
     PMID:String,
     ArticleTitle:Option[String],
     AbstractText:Option[Seq[String]]
)
object Article{
  implicit val reader: XmlReader[Article] = (
    (__ \ "MedlineCitation" \ "PMID").read[String],
    (__ \ "MedlineCitation" \ "Article" \ "ArticleTitle").read[String].optional,
    (__ \ "MedlineCitation" \ "Article" \ "Abstract" \ "AbstractText").read(seq[String]).default(Nil).optional
  ).mapN(apply)
}

case class Article_final(var PMID:Long, var Title:String, var Abstract:String)

/**
 * The Articles class instead contains all the parsed articles
 **/
case class Articles(
     articles: Seq[Article]
)
object Articles {
  implicit val reader: XmlReader[Articles] =
    (__ \ "PubmedArticle").read(seq[Article]).default(Nil).map(apply)
}

/**
 * The following two classes are defined to extract the pmids list received by a get response.
 **/
case class EsearchResult(var count:String, var idlist:Seq[String])
case class Pmids(var esearchresult : EsearchResult)


