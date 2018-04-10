package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

/**
 Filter
 Usage:

 spark-submit --master local[${NUM_EXECUTORS}] --executor-memory 10g --class org.ngseq.metagenomics.FastaFilter virapipe-0.9-jar-with-dependencies.jar -fasta ${OUTPUT_PATH}/${PROJECT_NAME}_blast_nonhuman -blast ${OUTPUT_PATH}/${PROJECT_NAME}_blast_final -out ${OUTPUT_PATH}/${PROJECT_NAME}_nonhuman_unknown

 spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 10g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.FastaFilter virapipe-0.9-jar-with-dependencies.jar -fasta ${OUTPUT_PATH}/${PROJECT_NAME}_blast_nonhuman -blast ${OUTPUT_PATH}/${PROJECT_NAME}_blast_final -out ${OUTPUT_PATH}/${PROJECT_NAME}_nonhuman_unknown
 */
public class FastaFilter {

    public static void main(String[] args) throws IOException {

        Options options = new Options();
        options.addOption(new Option( "out", true, "" ));
        options.addOption(new Option( "fasta", true, "Path to fasta sequences" ));
        options.addOption(new Option( "filter", true, "Blast format supported (one record per line, first column must be the corresponding sequence ID, columns delimited by tab)" ));

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

        String input = (cmd.hasOption("fasta")==true)? cmd.getOptionValue("fasta"):null;
        String input2 = (cmd.hasOption("filter")==true)? cmd.getOptionValue("filter"):null;
        String output = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;

        SparkConf conf = new SparkConf().setAppName("FastaFilter");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> blastRDD = sc.textFile(input2).map(blast -> {
            String[] s = blast.trim().split("\t");
            return s[0];
        });
        List<String> ids = blastRDD.collect();
        sc.hadoopConfiguration().set("textinputformat.record.delimiter", ">");
        //JavaPairRDD<Text, ReferenceFragment> inRDD = sc.newAPIHadoopFile(input, FastaInputFormat.class, Text.class, ReferenceFragment.class, sc.hadoopConfiguration());

        JavaPairRDD<String, String> fastaRDD = sc.textFile(input).mapToPair(f ->
        {
            String[] split = f.trim().split(System.lineSeparator());
            if(split.length>1) {
                return new Tuple2<String, String>( split[0], split[1]);
            }
            else return new Tuple2<String, String>("","");
        }).filter(fasta -> {
            System.out.println(fasta);
            return !ids.contains(fasta._1.toString()) && !fasta._1.isEmpty();
        });

        fastaRDD.map(z-> ">"+z._1+"\n"+z._2()).saveAsTextFile(output);
        sc.stop();
    }
}