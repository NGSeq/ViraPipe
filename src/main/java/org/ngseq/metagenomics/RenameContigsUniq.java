package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * Created by ilamaa on 2017-04-14.
 */
public class RenameContigsUniq {

    public static void main(String[] args) throws IOException {
        Options options = new Options();
        Option pathOpt = new Option( "in", true, "Path to fastq file in hdfs." );
        Option opOpt = new Option( "out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
        options.addOption(  new Option( "partitions", true,"Divide or merge to n partitions" ) );
        options.addOption(new Option( "fa", true, "Include only files with extension given " ));
        options.addOption( pathOpt );
        options.addOption( opOpt );

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse( options, args );
        }
        catch( ParseException exp ) {
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }

        String out = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
        String in = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
        String fastaonly = (cmd.hasOption("fa")==true)? cmd.getOptionValue("fa"):null;
        String partitions = (cmd.hasOption("partitions")==true)? cmd.getOptionValue("partitions"):null;

        SparkConf conf = new SparkConf().setAppName("RenameContigsUniq");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("textinputformat.record.delimiter", ">");

        JavaRDD<String> rdd;
        if(fastaonly!=null)
            rdd = sc.textFile(in+"/*."+fastaonly);
        else
            rdd = sc.textFile(in); //take whole directory as input

        JavaRDD<String> crdd = rdd.filter(f -> f.trim().split("\n")[0].length()!=0).map(fasta->{

            String[] fseq = fasta.trim().split("\n");
            String id = fseq[0].split(" ")[0];

            //Give unique id for sequence
            String seq_id = id+"_"+UUID.randomUUID().toString();
            String seq = Arrays.toString(Arrays.copyOfRange(fseq, 1, fseq.length)).replace(", ","").replace("[","").replace("]","");

            return ">"+seq_id+"\n"+seq;
        });

        if(partitions!=null)
            crdd.repartition(Integer.valueOf(partitions)).saveAsTextFile(out);
        else
            crdd.saveAsTextFile(out);

        sc.stop();
    }
}
