package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;

/**
 * Usage
 spark-submit  --master local[40] --driver-memory 4g --executor-memory 4g --class fi.aalto.ngs.metagenomics.SQLQueryBlast target/metagenomics-0.9-jar-with-dependencies.jar -in normrdd.fq -out blast_result -query "SELECT * from records ORDER BY sseqid ASC"

 **/


public class SQLQueryBlast {

  private static String tablename = "records";

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("SQLQueryBlast");

    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    //String query = args[2];


    Options options = new Options();
    options.addOption(new Option( "temp", "Temporary output"));
    options.addOption(new Option( "out", true, "" ));
    options.addOption(new Option( "in", true, "" ));
    options.addOption(new Option( "partitions", true, "Number of partitions" ));

    options.addOption(new Option( "word_size", ""));
    options.addOption(new Option( "gapopen", true, "" ));
    options.addOption(new Option( "gapextend", true, "" ));
    options.addOption(new Option( "penalty", true, "" ));
    options.addOption(new Option( "reward", true, "" ));
    options.addOption(new Option( "max_target_seqs", true, "" ));
    options.addOption(new Option( "evalue", true, "" ));
    options.addOption(new Option( "show_gis", "" ));
    options.addOption(new Option( "outfmt", true, "" ));
    options.addOption(new Option( "db", true, "" ));
    options.addOption(new Option( "query", true, "SQL query string." ));
    options.addOption(new Option( "format", true, "parquet or fastq" ));
    options.addOption(new Option( "lines", true, "" ));
    options.addOption( new Option( "tablename", true, "Default sql table name is 'records'"));

    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "spark-submit <spark specific args>", options, true );

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse( options, args );
    }
    catch( ParseException exp ) {
      System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
      System.exit(1);
    }

    String input = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
    String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
    String query = (cmd.hasOption("query")==true)? cmd.getOptionValue("query"):null;
    String format = (cmd.hasOption("format")==true)? cmd.getOptionValue("format"):"fastq";
    int lines = (cmd.hasOption("lines")==true)? Integer.valueOf(cmd.getOptionValue("lines")):100;
    tablename = (cmd.hasOption("tablename")==true)? cmd.getOptionValue("tablename"):"records";

    JavaRDD<String> rdd = sc.textFile(input);

    JavaRDD<BlastRecord> blastRDD = rdd.map(f -> {

        //qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore
        String[] fields = f.split("\\t");
        BlastRecord record = new BlastRecord();
        record.setQseqid(fields[0]!=null?fields[0]:null);
        record.setSseqid(fields[1]!=null?fields[1]:null);
        record.setPident(fields[2]!=null?Double.valueOf(fields[2]):null);
        record.setLength(fields[3]!=null?Integer.valueOf(fields[3]):null);
        record.setMismatch(fields[4]!=null?Integer.valueOf(fields[4]):null);
        record.setGapopen(fields[5]!=null?Integer.valueOf(fields[5]):null);
        record.setQstart(fields[6]!=null?Long.valueOf(fields[6]):null);
        record.setQend(fields[7]!=null?Long.valueOf(fields[7]):null);
        record.setSstart(fields[8]!=null?Long.valueOf(fields[8]):null);
        record.setSend(fields[9]!=null?Long.valueOf(fields[9]):null);
        record.setEvalue(fields[10]!=null?Double.valueOf(fields[10]):null);
        record.setBitscore(fields[11]!=null?Double.valueOf(fields[11]):null);

        System.out.println(f);

      return record;
    });

    Dataset df = sqlContext.createDataFrame(blastRDD, BlastRecord.class);
    df.registerTempTable(tablename);
    //eq. count duplicates "SELECT count(DISTINCT(sequence)) FROM reads"
    //"SELECT key,LEN(sequence) as l FROM reads where l<100;"

    if(query!=null) {

      Dataset<Row> resultDF = sqlContext.sql(query);
      resultDF.show(lines, false);

      if(outDir!=null){
        if(format.equals("parquet")){
          resultDF.write().parquet(outDir);
        }
        else if(format.equals("csv")){
          JavaRDD<String> resultRDD = dfToCSV(resultDF);
          //resultDF.write().csv(outDir); //prints columns in wrong order
          resultRDD.saveAsTextFile(outDir);
        }
        else{
          JavaRDD<String> resultRDD = dfToTabDelimited(resultDF);
          resultRDD.saveAsTextFile(outDir);
        }

      }
    }
    sc.stop();

  }

  private static JavaRDD<String> dfToTabDelimited(Dataset<Row> df) {
    return df.toJavaRDD().map(row ->  {
      //qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore

      String output = row.getAs("qseqid")+"\t"+row.getAs("sseqid")+"\t"+row.getAs("pident")+"\t"
              +row.getAs("length") +"\t"+row.getAs("mismatch")+"\t"+row.getAs("gapopen")
              +"\t"+row.getAs("qstart")+"\t"+row.getAs("qend")+"\t"+row.getAs("sstart")
              +"\t"+row.getAs("send")+"\t"+row.getAs("evalue")+"\t"+row.getAs("bitscore");

      return output;
    });
  }

  private static JavaRDD<String> dfToCSV(Dataset<Row> df) {
    return df.toJavaRDD().map(row ->  {
      //qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore

      String output = row.getAs("qseqid")+","+row.getAs("sseqid")+","+row.getAs("pident")+","
              +row.getAs("length") +","+row.getAs("mismatch")+","+row.getAs("gapopen")
              +","+row.getAs("qstart")+","+row.getAs("qend")+","+row.getAs("sstart")
              +","+row.getAs("send")+","+row.getAs("evalue")+","+row.getAs("bitscore");

      return output;
    });
  }

}