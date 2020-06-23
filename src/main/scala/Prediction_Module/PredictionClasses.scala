package Prediction_Module

/**
 * The following two classes are used to parse the json file containing the tuples relatedness in scala objects
 **/
case class Relatedness(couple:String, rel:Option[Double])
case class Tuples_Relatedness(result: List[Relatedness])