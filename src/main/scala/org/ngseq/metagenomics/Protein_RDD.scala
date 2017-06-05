package org.ngseq.metagenomics

/**
  * Created by davbzh on 2017-05-25.
  */

/*
/srv/hops/spark/bin/spark-submit --master yarn --deploy-mode client --conf spark.scheduler.mode=FAIR --conf spark.shuffle.service.enabled=true --executor-memory 20g  --conf spark.dynamicAllocation.enabled=true --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --class se.ki.ngs.metagenomics.sparkncbiblast.hmm.Protein_RDD metagenomics-0.9-jar-with-dependencies.jar --inputFile /Projects/NGS_Projects/RNA_aggregated/RNA_aggregated_assembly_cdhit --outputFile /Projects/NGS_Projects/RNA_aggregated/min60_ --minlength 60
*/

import org.apache.spark.{SparkConf, SparkContext}
//
import ORF.dnaOrfGenerator
//
import org.rogach.scallop._

object Protein_RDD {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val inputFile = opt[String]("inputFile", required = true)
    val outputFile = opt[String]("outputFile", required = true)
    val minlength = opt[Int]("minlength", required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {

    val argconf = new Conf(args)
    val inputFile = argconf.inputFile()
    val outputFile = argconf.outputFile()
    val minlength = argconf.minlength()

    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("Protein_RDD")
    val sc = new SparkContext(conf)

    // each sequeence in Fasta format starts with >seqid followded by nucleotide seqiences
    // and sequence part might be split with \n chactater: >seqid\nACTG\nACTT\nACTG
    // So we need to set custom spliter ">" to properly RDD
    sc.hadoopConfiguration.set("textinputformat.record.delimiter",">")
    //sc.conf.set("textinputformat.record.delimiter",">")

    // Load  input fasta.
    //val input =  sc.textFile(inputFile,1200)
    val input =  sc.textFile(inputFile)

    //
    val seqs = input.map(fquery => fquery.trim().split("\n"))
      .filter(line => line.length >= 2)

    //count lines
    //val nseqs = seqs.count().toInt

    //Genearte ORFs and calculate
    //val ORFs = seqs.repartition(nseqs).map(fquery => dnaOrfGenerator(fquery(0), fquery.slice(1, fquery.length).mkString(""), minlength))
    val ORFs = seqs.map(fquery => dnaOrfGenerator(fquery(0), fquery.slice(1, fquery.length).mkString(""), minlength))

    //Generate forward and reverse RSCU values
    val result = ORFs.map( ORF =>
      s"${ORF._2(0)(0).trim()}\n" +
      s"${ORF._2(1)(0).trim()}\n" +
      s"${ORF._2(2)(0).trim()}\n" +
      s"${ORF._2(3)(0).trim()}\n" +
      s"${ORF._2(4)(0).trim()}\n" +
      s"${ORF._2(5)(0)}"
    )

    //save RSCU values
    result.saveAsTextFile(outputFile + "Protein")

    sc.stop()
  }
}
