package Databases_Module.LNC

import Databases_Module.DatabasesUtilsTrait
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import LNCpediaUtils._
import mirCodeUtils._
import scala.xml.Elem

object LNCMain extends LNCMainTrait with DatabasesUtilsTrait {
  def get_LNC_dataframes(spark: SparkSession, conf_xml: Elem): Unit = {
      val paths = get_element_path(conf_xml, "hdfs_paths", "lnc")


      /** LNCBook **/
      //val LNCB_h38 = getLNCbook(root + "/LNCbook/lncRNA_LncBook_GRCh38_9.28.gtf",spark,0)
      //val LNCB_h37 = getLNCbook(root + "/LNCbook/lncRNA_LncBook_GRCh37_9.28.gtf",spark,0)
      //val LNCB_dis = getLNCbook(root + "/LNCbook/lncbook_disease_lncrna.txt",spark,1)
      //val LNCB_miR = getLNCbook(root + "/LNCbook/lncRNA_miRNA_interaction.txt",spark,2, "true")

      /** LNCpedia **/
      val lnc_pedia_paths  = paths("lnc_pedia")
      val lnc_pedia_rpath  = lnc_pedia_paths("root_path")
      val LNCP_h38         = get_LNCpedia(lnc_pedia_rpath + "/" + lnc_pedia_paths("hg38_path"),  spark, 0)
      val LNCP_h37         = get_LNCpedia(lnc_pedia_rpath + "/" + lnc_pedia_paths("hg19_path"),  spark, 0)
      //val LNCP_gene_Ensemb = get_LNCpedia(root + "/LNCpedia/lncipedia_5_2_ensembl_92_genes.txt", spark, 1)
      //val LNCP_tran_Ensemb = get_LNCpedia(root + "/LNCpedia/lncipedia_5_2_ensembl_92.txt", spark, 2)
      //val LNCP_tran_RefSeq = get_LNCpedia(root + "/LNCpedia/lncipedia_5_2_refseq.txt", spark, 3)
      val LNCP4Index       = get_LNCPedia4Indexing(LNCP_h38, LNCP_h37).persist


      /** mirCode **/
      val mir_rpath       = paths("mirCode_path")("root_path")
      val LNCmC           = getmirCode(mir_rpath + "/*", spark)
      val LNCmC4filtering = get_mirCode4indexing(LNCmC).persist


      /** Indexing **/
      val saving_rpath = "/" + mir_rpath.split("/")(1)
      LNC_indexing     = create_element_indexing("LNC_name", "LNC", LNCP4Index, LNCmC4filtering).persist
      LNC_indexing.write.mode("overwrite").save(saving_rpath + "/LNC_indexing")


      /** Relationships **/
      create_LCPedia_rel(create_relationships, LNC_indexing, LNCP4Index, LNCP_h37, LNCP_h38)
         .union(create_mirCode_rel(create_relationships, LNC_indexing, LNCmC4filtering , LNCmC))
         .groupBy("NAME1","IDX1","NAME2","IDX2","TYPE").agg(collect_set(col("REFERENCE")).as("REFERENCE"))
         .write.mode("overwrite").save(saving_rpath + "/LNC_relationships")

  }
}
