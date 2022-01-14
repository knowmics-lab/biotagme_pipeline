import scala.collection.mutable
import scala.xml.Node

trait MainBioTAGMErTrait {
    var conf_params: mutable.Map[String, Any] = _


    /**
      * xml2scalaSeq_parser has been implemented to convert the xml file (containing all the configuration parameters)
      * in a scala set of string.
      *   Note that:
      *      <tag1>
      *         <tag2>
      *            value of tag 2
      *         </tag2>
      *      <tag1>
      *   is converted as follows:
      *      tag1;tag2;value of tag 2
      **/
    def xml2scalaSeq_parser(node_root: Node): Seq[String] = {
        val child_list = node_root.child
        var xml2Vect:Seq[String] = Seq.empty

        if( child_list.nonEmpty)
            child_list.foreach(node => {
                if(node.label.toLowerCase != "description" && node.label.toLowerCase != "descrizione") {
                   val tmp = xml2scalaSeq_parser(node)
                   tmp.foreach(str => {
                      if(str != "") {
                         val label_and_text = node_root.label + ";" + str
                         xml2Vect = xml2Vect :+ label_and_text
                      }
                   })
                }
            })
        else
            xml2Vect = Seq(node_root.text.replaceAll("[\\s+|\n]",""))

        xml2Vect
    }


    /**
      * xml_parser_main converts the string vector containing the configuration parameters into a Map[String, Any].
      * The second Map parameter is an Any type because the tags related to databases have much more nested subtags
      * compared with the base Biotagme configuration parameters. So we can get:
      *   1. Map(String, Map(String, String))
      *   2. Map(String, Map(String, Map(String, String)))
      **/
    def xml_parser_main(configs:Seq[String]): Unit = {
        var config_params: mutable.Map[String, Any] = mutable.Map.empty

        configs.foreach(row => {
            val tmp = row.replaceAll("configuration;|value;", "").split(";")
            if( !config_params.contains(tmp(0)))
                 config_params += (tmp(0) -> mutable.Map.empty[String, Any])

            if(tmp.length == 3){
               var tmp_map = config_params(tmp(0)).asInstanceOf[mutable.Map[String, String]]
               tmp_map += (tmp(1) -> tmp(2))
            }
            else if(tmp.length == 5){
                var tmp_map = config_params(tmp(0)).asInstanceOf[mutable.Map[String, mutable.Map[String, mutable.Map[String, String]]]]
                if( !tmp_map.contains(tmp(1)))
                     tmp_map += (tmp(1) -> mutable.Map.empty[String, mutable.Map[String, String]])
                var  tmp_map_2 = tmp_map(tmp(1))
                if( !tmp_map_2.contains(tmp(2)))
                     tmp_map_2 += (tmp(2) -> mutable.Map.empty[String,String])

                tmp_map_2(tmp(2)) += (tmp(3) -> tmp(4))
            }
        })

        conf_params = config_params
    }

}
