package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.seqdoop.hadoop_bam.BAMInputFormat;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.IOException;

/**
 * Usage
 spark-submit  --master local[40] --driver-memory 4g --executor-memory 4g --class org.ngseq.metagenomics.SQLQueryFastq target/metagenomics-0.9-jar-with-dependencies.jar -in normrdd.fq -out results -query "SELECT * from records ORDER BY key ASC"

 **/


public class SQLQueryFastq {

  private static String tablename = "records";

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("SQLQueryFastq");

    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    Options options = new Options();

    Option opOpt = new Option( "out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
    Option queryOpt = new Option( "query", true, "SQL query string." );
    Option samOpt = new Option( "format", true, "parquet or fastq" );
    Option baminOpt = new Option( "in", true, "" );
    options.addOption( new Option( "tablename", true, "Default sql table name is 'records'"));

    options.addOption( opOpt );
    options.addOption( queryOpt );
    options.addOption( samOpt );
    options.addOption( baminOpt );
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      // parse the command line arguments
      cmd = parser.parse( options, args );

    }
    catch( ParseException exp ) {
      // oops, something went wrong
      System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
    }

    String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
    String query = (cmd.hasOption("query")==true)? cmd.getOptionValue("query"):null;
    String format = (cmd.hasOption("format")==true)? cmd.getOptionValue("format"):"fastq";
    String in = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
    tablename = (cmd.hasOption("tablename")==true)? cmd.getOptionValue("tablename"):"records";

    sc.hadoopConfiguration().setBoolean(BAMInputFormat.KEEP_PAIRED_READS_TOGETHER_PROPERTY, true);

    JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(in, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

    JavaRDD<MyRead> rdd = fastqRDD.map(record -> {
      MyRead read = new MyRead();
      read.setKey(record._1.toString());
      read.setSequence(record._2.getSequence().toString());
      read.setRead(record._2.getRead());
      read.setQuality(record._2.getQuality().toString());

      read.setTile(record._2.getTile());
      read.setXpos(record._2.getXpos());
      read.setYpos(record._2.getYpos());
      read.setRunNumber(record._2.getRunNumber());
      read.setInstrument(record._2.getInstrument());
      read.setFlowcellId(record._2.getFlowcellId());
      read.setLane(record._2.getLane());
      read.setControlNumber(record._2.getControlNumber());
      read.setFilterPassed(record._2.getFilterPassed());

      return read;
    });

    Dataset df = sqlContext.createDataFrame(rdd, MyRead.class);
    df.registerTempTable(tablename);
    //eq. count duplicates "SELECT count(DISTINCT(sequence)) FROM records"
    //"SELECT key,LEN(sequence) as l FROM records where l<100;"
    if(query!=null) {

      //JavaRDD<MyAlignment> rdd = samRDD.map(bam -> new MyAlignment(bam.getReadName(), bam.getStart(), bam.getReferenceName(), bam.getReadLength(), new String(bam.getReadBases(), StandardCharsets.UTF_8), bam.getCigarString(), bam.getReadUnmappedFlag(), bam.getDuplicateReadFlag(), bam));
      //Save as parquet file
      Dataset<Row> resultDF = sqlContext.sql(query);
      resultDF.show(100, false);

      if(outDir!=null){
        if(format.equals("fastq")){
          JavaPairRDD<Text, SequencedFragment> resultRDD = dfToFastqRDD(resultDF);
          resultRDD.saveAsNewAPIHadoopFile(outDir, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
        }
        else
          resultDF.write().parquet(outDir);
      }
    }
    sc.stop();

  }

  private static JavaPairRDD<Text, SequencedFragment> dfToFastqRDD(Dataset<Row> df) {
    return df.toJavaRDD().mapToPair(row ->  {
      Text t = new Text((String) ("key"));
      SequencedFragment sf = new SequencedFragment();
      sf.setSequence(new Text((String) row.getAs("sequence")));
      sf.setRead((Integer) row.getAs("read"));
      sf.setQuality(new Text((String) row.getAs("quality")));

      return new Tuple2<Text, SequencedFragment>(t, sf);
    });
  }

}