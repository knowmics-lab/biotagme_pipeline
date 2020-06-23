package Networks

import org.apache.spark.sql.functions.udf

trait NetworksUtils {
    /**
     *  regex_term is a user defined function that applies some regular expression operations in order to convert a
     *  spot or wikipedia title in a more appropriate form for the associations with literature data
     **/
    var regex_term = udf((term: String) =>{
        var n_term  = if(term.contains("hsa") || term.contains("lnc")) term.split("-").drop(1).mkString("-") else term
        var regex   =  "\\s+".r
        n_term      =  regex.replaceAllIn(n_term," ")
        regex       =  "\\s*-\\s*".r
        n_term      =  regex.replaceAllIn(n_term,"")
        regex       =  "[^\\w\\s+]+".r
        n_term      =  regex.replaceAllIn(n_term," ")
        regex       =  "\\s+".r
        n_term      =  regex.replaceAllIn(n_term,"")

        n_term
    })
}
