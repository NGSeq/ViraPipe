package org.ngseq.metagenomics;

import htsjdk.samtools.SAMRecord;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.BAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Usage
 spark-submit --master local[40] --driver-memory 4g --executor-memory 4g --class fi.aalto.ngs.metagenomics.SQLQueryBAM target/metagenomics-0.9-jar-with-dependencies.jar -in aligned -out unmapped -query "SELECT * from records WHERE readUnmapped = TRUE"

 **/


public class SQLQueryBAM {

  private static String tablename = "records";

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("SQLQueryBAM");

    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new HiveContext(sc.sc());

    Options options = new Options();
    Option opOpt = new Option( "out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
    Option queryOpt = new Option( "query", true, "SQL query string." );
    Option baminOpt = new Option( "in", true, "" );

    options.addOption( opOpt );
    options.addOption( queryOpt );
    options.addOption( baminOpt );
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse( options, args );

    }
    catch( ParseException exp ) {
      System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
    }

    String bwaOutDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
    String query = (cmd.hasOption("query")==true)? cmd.getOptionValue("query"):null;
    String bamin = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;

    sc.hadoopConfiguration().setBoolean(BAMInputFormat.KEEP_PAIRED_READS_TOGETHER_PROPERTY, true);

    //Read BAM/SAM from HDFS
    JavaPairRDD<LongWritable, SAMRecordWritable> bamPairRDD = sc.newAPIHadoopFile(bamin, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class, sc.hadoopConfiguration());
    //Map to SAMRecord RDD
    JavaRDD<SAMRecord> samRDD = bamPairRDD.map(v1 -> v1._2().get());
    JavaRDD<MyAlignment> rdd = samRDD.map(bam -> new MyAlignment(bam.getReadName(), bam.getStart(), bam.getReferenceName(), bam.getReadLength(), new String(bam.getReadBases(), StandardCharsets.UTF_8), bam.getCigarString(), bam.getReadUnmappedFlag(), bam.getDuplicateReadFlag()));

    Dataset<Row> samDF = sqlContext.createDataFrame(rdd, MyAlignment.class);
    samDF.registerTempTable(tablename);
    if(query!=null) {

      //Save as parquet file
      Dataset df2 = sqlContext.sql(query);
      df2.show(100,false);

      if(bwaOutDir!=null)
        df2.write().parquet(bwaOutDir);

    }else{
      if(bwaOutDir!=null)
        samDF.write().parquet(bwaOutDir);
    }

    sc.stop();

  }


}