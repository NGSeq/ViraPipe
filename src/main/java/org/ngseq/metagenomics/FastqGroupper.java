package org.ngseq.metagenomics;


import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

/**Usage
  Groups Fastq reads by read name and saves to multiple output directories/files.
 **/


public class FastqGroupper {

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("FastqGroupper");
    JavaSparkContext sc = new JavaSparkContext(conf);

    Options options = new Options();
    Option pathOpt = new Option( "in", true, "Path to fastq file in hdfs." );
    Option opOpt = new Option( "out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
    options.addOption(  new Option( "subdirs", "Divide to subdirectories" ) );
    options.addOption( pathOpt );
    options.addOption( opOpt );

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
    String fastq = cmd.getOptionValue("in");

    String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
    boolean subdirs = cmd.hasOption("subdirs");

    JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(fastq, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

    //Group by keys and save partitions to different locations using record writer
      JavaPairRDD<String, Tuple2<Text, SequencedFragment>> pairRDD = fastqRDD.mapToPair(kv -> {
          String readname = kv._1.toString();
          String key = readname.split(":")[0];
          return new Tuple2<String, Tuple2<Text, SequencedFragment>>(key, new Tuple2<Text, SequencedFragment>(new Text(readname), kv._2));
      });

      JavaPairRDD<String, Iterable<Tuple2<Text, SequencedFragment>>> groupped = pairRDD.groupByKey();

      groupped.foreach(group -> {
          System.out.println("GROUP NAME: "+group._1());
          ByteArrayOutputStream os = new ByteArrayOutputStream();

          FSDataOutputStream dataOutput = null;
          Configuration config = new Configuration();
          try {
              FileSystem fs = FileSystem.get(config);
              dataOutput = new FSDataOutputStream(os);
              if(subdirs)
                dataOutput = fs.create(new Path(outDir+"/"+group._1+"/out.fq"));
              else dataOutput = fs.create(new Path(outDir+"/"+group._1+".fq"));
          } catch (IOException e) {
              e.printStackTrace();
          }

          FastqOutputFormat.FastqRecordWriter writer = new FastqOutputFormat.FastqRecordWriter(config, dataOutput);
          Iterator<Tuple2<Text, SequencedFragment>> it = group._2().iterator();
          try {

              while (it.hasNext()) {
                  Tuple2<Text, SequencedFragment> next = it.next();
                  String key = next._1.toString();

                  SequencedFragment sf = new SequencedFragment();
                  sf.setQuality(new Text(next._2.getQuality().toString()));
                  sf.setSequence(new Text(next._2.getSequence().toString()));

                  writer.write(new Text(key), sf);

              }

              dataOutput.close();
              os.close();
          }catch(IOException e) {
              e.printStackTrace();
          }

      });

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

}
