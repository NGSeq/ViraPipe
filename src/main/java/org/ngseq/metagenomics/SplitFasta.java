package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Created by davbzh on 2017-04-14.
 */
public class SplitFasta {

    public static void main(String[] args) throws IOException {
        Options options = new Options();
        Option pathOpt = new Option( "in", true, "Path to fastq file in hdfs." );
        Option opOpt = new Option( "out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
        options.addOption(  new Option( "partitions", "Divide or merge to n partitions" ) );
        options.addOption( pathOpt );
        options.addOption( opOpt );

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

        String out = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
        String in = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
        String partitions = (cmd.hasOption("partitions")==true)? cmd.getOptionValue("partitions"):null;

        SparkConf conf = new SparkConf().setAppName("SplitFasta");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("textinputformat.record.delimiter", ">");

        JavaRDD<String> rdd = sc.textFile(in);
        JavaRDD<String> crdd = rdd.map(v->">"+v.trim()).repartition(Integer.valueOf(partitions));

        crdd.saveAsTextFile(out);
        sc.stop();
    }
}
