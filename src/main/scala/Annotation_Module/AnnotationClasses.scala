package Annotation_Module

/**
 * The Annotations class contains all the document's Annotation. Each Annotation stores the following
 * information: wikipedia id, spot, start, end, rho, dbpedia categories, title and so on.
 **/
case class Annotation
(
    spot              :String,
    start             :Option[Int],
    link_probability  :Option[Double],
    rho               :Option[Double],
    dbpedia_categories:Option[List[String]],
    end               :Option[Int],
    id                :Option[Long],
    title             :Option[String]
)

case class Annotations
(
    annotations:List[Annotation]
)