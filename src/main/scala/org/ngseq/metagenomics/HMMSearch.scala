package org.ngseq.metagenomics

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.FileStatus
//
import org.apache.spark.{SparkConf, SparkContext}
//
import scala.collection.mutable.ArrayBuffer
import org.rogach.scallop._
import scala.sys.process._

/**
  * Created by davbzh on 2017-05-26.
  */

/*
/srv/hops/spark/bin/spark-submit --master yarn --deploy-mode client --conf spark.scheduler.mode=FAIR --conf spark.shuffle.service.enabled=true --executor-memory 20g  --conf spark.dynamicAllocation.enabled=true --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --class se.ki.ngs.metagenomics.sparkncbiblast.hmm.HMMSearch metagenomics-0.9-jar-with-dependencies.jar --inputDir /Projects/NGS_Projects/RNA_aggregated/RNA_aggregated_assembly_cdhit --outputDir /Projects/NGS_Projects/RNA_aggregated/min60_ --tempDir /mnt/hdfs/1/hmm_tmp_dir --db /mnt/hdfs/1/vFam_A_2014/vFam-A_2014.hmm --minlength 60
*/

object HMMSearch {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val inputDir = opt[String]("inputDir", required = true)
    val outputDir = opt[String]("outputDir", required = true)
    val tempDir = opt[String]("tempDir", required = true)
    val database = opt[String]("db", required = true)
    verify()
  }


  def main(args: Array[String]) {

    val argconf = new Conf(args)
    val inputDir = argconf.inputDir()
    val outputDir = argconf.outputDir()
    val tempDir = argconf.tempDir()
    val db = argconf.database()

    //spark configuration
    val conf = new SparkConf().setAppName("HMMSearch")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    //hdfs configuration
    val fs = FileSystem.get(new Configuration())
    //list files in the hdfs directroty
    val st: Array[FileStatus] = fs.listStatus(new Path( inputDir ))
    //and add to array
    var splitFileList = ArrayBuffer[String]()
    st.map(file_st => {
      if (!file_st.equals("_SUCCESS")){
        splitFileList.append(file_st.getPath.getName)
      }
    })

    //now parallelise as spark context
    val fastaFilesRDD = sc.parallelize(splitFileList, splitFileList.length)

    //Create output directory
    fs.mkdirs(new Path( outputDir ))
    //and perfomrn hmmsearch
    val hmmRDD = fastaFilesRDD.map(fname => {

        //construct command strings
        val rm_tmp_dir = "if [ -d "+tempDir+" ]; then rm -r "+tempDir+"; fi"
        val mk_tmp_dir = "if [ ! -d "+tempDir+" ]; then mkdir "+tempDir+"; fi"
        val cp_2_loc = "hdfs dfs -get " + inputDir + "/" + fname +  " "+tempDir+"/" + fname
        val hmm = "hmmsearch --noali --cpu 10 -o "+tempDir+"/lsu."+fname+".txt --tblout "+tempDir+"/lsu."+fname+".table.txt db" + " "+tempDir+"/" + fname + " 2> "+tempDir+"/error.log"
        val cp_2_dfs = "hdfs dfs -put "+tempDir+"/lsu."+fname+".table.txt" + " " + outputDir+"/"+fname+".table.txt";
        //run commands
        val rm_status = Seq("/bin/sh", "-c", rm_tmp_dir).!
        val mk_status = Seq("/bin/sh", "-c", mk_tmp_dir).!
        val cp_2_loc_status = Seq("/bin/sh", "-c", cp_2_loc).!
        val hmm_status = Seq("/bin/sh", "-c", hmm).!
        val cp_2_dfs_status = Seq("/bin/sh", "-c", cp_2_dfs).!
        val clean_up_status = Seq("/bin/sh", "-c", rm_tmp_dir).!

    }).collect()

    val clean_hmmRDD = fastaFilesRDD.map(fname => {

      val rm_tmp_dir = "if [ -d /mnt/hdfs/1/hmm_tmp_dir ]; then rm -r /mnt/hdfs/1/hmm_tmp_dir; fi"
      val clean_up_status = Seq("/bin/sh", "-c", rm_tmp_dir).!

    }).collect()
  }
}
