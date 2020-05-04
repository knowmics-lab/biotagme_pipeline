package Annotation_Module

import org.apache.spark.sql.SparkSession

import scala.xml.{Elem, Node}

/**
 * Such trait defines the AnnotationProcedure model. Therefore, all the necessary functions related to
 * annotation procedure are defined here. Since the trait component is similar to Java abstract class,
 * some methods are also implemented.
 **/

trait AnnotationTrait {

  /* key-value association creation */
  def addTuple(key: String, value: Node):(String, String) = key -> value.child(3).text.replaceAll("[\\s+|\n]","")

  /**
   * The getTagme_info function is defined to get all the TAGME configuration parameters from the xml
   * file. These information are: annotation url, relatedness url, token and so on. In addition, the
   * paths containing the documents to annotate are also obtained.
   **/
  def getTagme_info(conf_xml:Elem):Map[String, String] = {
      var tagme_params: Map[String, String] = Map()
      conf_xml.child.foreach(elem_0 =>
          elem_0.label match {
              case "tagme_info" =>
                  elem_0.child.foreach(elem_1 => elem_1.label match {
                      case "url"                  => tagme_params = tagme_params + addTuple("url",      elem_1)
                      case "url_rel"              => tagme_params = tagme_params + addTuple("url_rel",  elem_1)
                      case "token"                => tagme_params = tagme_params + addTuple("token",    elem_1)
                      case "language"             => tagme_params = tagme_params + addTuple("language", elem_1)
                      case "epsilon"              => tagme_params = tagme_params + addTuple("epsilon",  elem_1)
                      case "include_wikicategory" => tagme_params = tagme_params + addTuple("include_wikicategory", elem_1)
                      case _ => null
                  })
              case "hdfs_paths" =>
                  elem_0.child.foreach(elem_1  => elem_1.label match {
                      case "allDocuments_path" => tagme_params = tagme_params + addTuple("allDocuments_path", elem_1)
                      case "newDocuments_path" => tagme_params = tagme_params + addTuple("newDocuments_path", elem_1)
                      case "categories_path"   => tagme_params = tagme_params + addTuple("categories_path",   elem_1)
                      case "DocumentsAnnotations_path" => tagme_params = tagme_params + addTuple("DocumentsAnnotations_path",elem_1)
                      case _ => null
              })
              case _ => null
          })
      tagme_params
  }

  /**
   * Main function to Annotation procedure
   **/
  def getDataFrame_annotations(spark:SparkSession, option: Int, conf_xml_obj: Elem)

  /**
   * Categories Filtering operation
   **/
  def makeCategoriesFiltering(spark:SparkSession, option: Int, conf_xml_obj: Elem)
}
