package org.ngseq.metagenomics;


import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;

/**Usage
 * Apply different quality filters to FASTQ reads
 spark-submit --master local[20] --class org.ngseq.metagenomics.FastqFilter target/metagenomics-0.9-jar-with-dependencies.jar -fastq /user/root/fqsplits -avgc 0.5 -lowqc 10 -lowqt 50

 spark-submit --master yarn --deploy-mode client --conf spark.dynamicAllocation.enabled=true --executor-memory 10g  --conf spark.yarn.executor.memoryOverhead=3000 --conf spark.task.maxFailures=40 --conf spark.yarn.max.executor.failures=100 --conf spark.hadoop.validateOutputSpecs=true --class org.ngseq.metagenomics.FastqFilter target/metagenomics-0.9-jar-with-dependencies.jar -fastq interleaved -ref /index/hg38.fa -bwaout bam -align -unmapped

 **/


public class FastqFilter {

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("FastqFilter");
    JavaSparkContext sc = new JavaSparkContext(conf);


    Options options = new Options();
    Option pathOpt = new Option( "fastq", true, "Path to fastq file in hdfs." );
    Option path2Opt = new Option( "fastq2", true, "Path to fastq file in hdfs." );

    //gmOpt.setRequired(true);
    Option refOpt = new Option( "ref", true, "Path to fasta reference file." );
    Option opOpt = new Option( "out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
    Option pairedOpt = new Option( "interleaved", "Use interleaved paired end reads" );

    Option formatOpt = new Option( "format", true, "bam or sam, bam is default" );

    options.addOption(new Option( "avgq",true, "Minimum value for average quality score" ));
    options.addOption(new Option( "lowqc", true, "Maximum count for low quality score under threshold -lowqt "));
    options.addOption(new Option( "lowqt", true, "Threshold for low quality" ));
    options.addOption(new Option( "dedupid", "remove reads with duplicate ids, can not be used with other filters" ));
    options.addOption(new Option( "split", "Split paired-end interleaved reads to two directories named by -out parameters + 1 and 2" ));

    options.addOption( pathOpt );
    options.addOption( path2Opt );
    options.addOption( refOpt );
    options.addOption( opOpt );
    options.addOption( pairedOpt );
    options.addOption( formatOpt );

    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "spark-submit <spark specific args>", options, true );

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse( options, args );
    }
    catch( ParseException exp ) {
      System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
    }
    String fastq = cmd.getOptionValue("fastq");
    String fastqOutDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;

    int minAvgQuality = (cmd.hasOption("avgq")==true)? Integer.parseInt(cmd.getOptionValue("avgq")):0;
    int maxLowQualityCount = (cmd.hasOption("lowqc")==true)? Integer.parseInt(cmd.getOptionValue("lowqc")):0;
    int qualityThreshold = (cmd.hasOption("lowqt")==true)? Integer.parseInt(cmd.getOptionValue("lowqt")):0;
    boolean dedupid = cmd.hasOption("dedupid");
    boolean splitinterleaved = cmd.hasOption("split");


    if(splitinterleaved){
      JavaPairRDD<Text, SequencedFragment> rdd1 = sc.newAPIHadoopFile(fastq, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());
      JavaPairRDD<Text, SequencedFragment> fwd = rdd1.filter(r -> r._1.toString().contains(" 1:N:0:1") || r._1.toString().contains("/1"));
      JavaPairRDD<Text, SequencedFragment> rdd2 = sc.newAPIHadoopFile(fastq, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());
      JavaPairRDD<Text, SequencedFragment> rev = rdd2.filter(r -> r._1.toString().contains(" 2:N:0:1") || r._1.toString().contains("/2"));
      fwd.saveAsNewAPIHadoopFile(fastqOutDir+"1", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
      rev.saveAsNewAPIHadoopFile(fastqOutDir+"2", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
    }
    else if(dedupid){
      JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(fastq, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());
      JavaPairRDD<String,SequencedFragment> rdd = fastqRDD.mapToPair(a -> new Tuple2(a._1.toString(), a._2));
      JavaPairRDD<String,SequencedFragment> eredRDD = rdd.reduceByKey((a, b) -> copySequencedFragment(a, a.getSequence().toString(), a.getQuality().toString()));
      JavaPairRDD<Text, SequencedFragment> filteredRDD = eredRDD.mapToPair(a -> new Tuple2<Text, SequencedFragment>(new Text(a._1), copySequencedFragment(a._2, a._2.getSequence().toString(), a._2.getQuality().toString())));

      filteredRDD.saveAsNewAPIHadoopFile(fastqOutDir, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
    }
    else{
      JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(fastq, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

      JavaPairRDD<Text, SequencedFragment> filteredRDD = fastqRDD.mapPartitionsToPair(record -> {

        ArrayList<Tuple2<Text, SequencedFragment>> records = new ArrayList<Tuple2<Text, SequencedFragment>>();
        while (record.hasNext()) {
          Tuple2<Text, SequencedFragment> next = record.next();
          String key = next._1.toString();
          SequencedFragment sf = next._2;
          String quality = sf.getQuality().toString();
          String sequence = sf.getSequence().toString();

          if (maxLowQualityCount != 0) {
            if (!lowQCountTest(maxLowQualityCount, qualityThreshold, quality.getBytes())){
              record.next();
              continue;
            }
          }
          if (minAvgQuality != 0) {
            if (!avgQualityTest(minAvgQuality, quality.getBytes())){
              record.next();
              continue;
            }
          }

          if (record.hasNext()) {
            Tuple2<Text, SequencedFragment> next2 = record.next();
            String key2 = next2._1.toString();
            SequencedFragment sf2 = next2._2;
            String quality2 = sf2.getQuality().toString();
            String sequence2 = sf2.getSequence().toString();

            if (maxLowQualityCount != 0) {
              if (!lowQCountTest(maxLowQualityCount, qualityThreshold, quality2.getBytes())){
                continue;
              }
            }
            if (minAvgQuality != 0) {
              if (!avgQualityTest(minAvgQuality, quality2.getBytes())){
                continue;
              }
            }
            records.add(new Tuple2<>(new Text(key),  copySequencedFragment(sf, sequence, quality)));
            records.add(new Tuple2<>(new Text(key2),  copySequencedFragment(sf2, sequence2, quality2)));
          }
        }
        return records.iterator();

      });
      filteredRDD.saveAsNewAPIHadoopFile(fastqOutDir, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
    }


    sc.stop();

  }

  private static SequencedFragment copySequencedFragment(SequencedFragment sf, String sequence, String quality) {
    SequencedFragment copy = new SequencedFragment();

    copy.setControlNumber(sf.getControlNumber());
    copy.setFilterPassed(sf.getFilterPassed());
    copy.setFlowcellId(sf.getFlowcellId());
    copy.setIndexSequence(sf.getIndexSequence());
    copy.setInstrument(sf.getInstrument());
    copy.setLane(sf.getLane());
    copy.setQuality(new Text(quality));
    copy.setRead(sf.getRead());
    copy.setRunNumber(sf.getRunNumber());
    copy.setSequence(new Text(sequence));
    copy.setTile(sf.getTile());
    copy.setXpos(sf.getXpos());
    copy.setYpos(sf.getYpos());

    return copy;
  }

  private static boolean avgQualityTest(double minAvgQuality, byte[] bytes) {
    int qSum = 0;
    for(byte b : bytes){
      qSum+=b;
    }
    int q = qSum/bytes.length;
    if(q > minAvgQuality)
      return true;

    return false;
  }

  private static boolean lowQCountTest(int maxLowQualityCount, int qualityThreshold, byte[] bytes) {
    int lowqCount = 0;
    for(byte b : bytes){
      if(b<qualityThreshold)
        lowqCount++;
    }
    if(lowqCount<maxLowQualityCount)
      return true;

    return false;
  }

}
