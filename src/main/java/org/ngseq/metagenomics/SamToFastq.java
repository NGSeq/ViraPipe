package org.ngseq.metagenomics;


import htsjdk.samtools.SAMRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.IOException;

/**Usage
 *  spark-submit --master local[4] --class fi.aalto.ngs.metagenomics.Interleave target/metagenomics-0.9-jar-with-dependencies.jar -fastq /user/root/fw.fq -fastq2 /user/root/rw.fq -splitsize 10000 -splitout /user/root/fqsplits -interleave
   **/


public class SamToFastq {

  private static JavaSparkContext sc;

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("SamToFastq");
    sc = new JavaSparkContext(conf);

    String in = args[0];
    String out = args[1];

    JavaPairRDD<LongWritable, SAMRecordWritable> bamPairRDD = sc.newAPIHadoopFile(in, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class, sc.hadoopConfiguration());
    //Map to SAMRecord RDD
    JavaRDD<SAMRecord> samRDD = bamPairRDD.map(v1 -> v1._2().get());

    JavaPairRDD<Text, SequencedFragment> fastqrdd = mapSAMRecordsToFastq(samRDD);

    fastqrdd.saveAsNewAPIHadoopFile(out, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());

    sc.stop();

  }

  private static JavaPairRDD<Text, SequencedFragment> mapSAMRecordsToFastq(JavaRDD<SAMRecord> bamRDD) {

    JavaPairRDD<Text, SequencedFragment> fastqRDD = bamRDD.mapToPair(read -> {

      String name = read.getReadName();
      if(read.getReadPairedFlag()){
        if(read.getFirstOfPairFlag())
          name = name+"/1";
        if(read.getSecondOfPairFlag())
          name = name+"/2";
      }

      //TODO: check values
      Text t = new Text(name);
      SequencedFragment sf = new SequencedFragment();
      sf.setSequence(new Text(read.getReadString()));
      sf.setQuality(new Text(read.getBaseQualityString()));

      return new Tuple2<Text, SequencedFragment>(t, sf);
    });
    return fastqRDD;
  }

}