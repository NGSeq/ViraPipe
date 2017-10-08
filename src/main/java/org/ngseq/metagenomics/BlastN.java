package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 spark-submit  --master local[${NUM_EXECUTORS}] --executor-memory 10g  --class org.ngseq.metagenomics.BlastN metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_blast_nonhuman -out ${OUTPUT_PATH}/${PROJECT_NAME}_blast_final -db ${BLAST_DATABASE} -outfmt 6 -num_threads ${BLAST_THREADS}

 spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 10g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.BlastN metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_blast_nonhuman -out ${OUTPUT_PATH}/${PROJECT_NAME}_blast_final -db ${BLAST_DATABASE} -outfmt 6 -num_threads ${BLAST_THREADS}

 */
public class BlastN {

    public static void main(String[] args) throws IOException {

        Options options = new Options();
        options.addOption(new Option( "temp", "Temporary output"));
        options.addOption(new Option( "out", true, "" ));
        options.addOption(new Option( "in", true, "" ));

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
        options.addOption(new Option( "task", true, "" ));
        options.addOption(new Option( "num_threads", true, "" ));
        options.addOption(new Option( "taxname", true, "Use Blast taxonomy names for filtering e.g. viruses, bacteria, archaea" ));

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
        String output = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
        int word_size = (cmd.hasOption("word_size")==true)? Integer.valueOf(cmd.getOptionValue("word_size")):11;
        int gapopen = (cmd.hasOption("gapopen")==true)? Integer.valueOf(cmd.getOptionValue("gapopen")):0;
        int gapextend = (cmd.hasOption("gapextend")==true)? Integer.valueOf(cmd.getOptionValue("gapextend")):2;
        int penalty = (cmd.hasOption("penalty")==true)? Integer.valueOf(cmd.getOptionValue("penalty")):-1;
        int reward = (cmd.hasOption("reward")==true)? Integer.valueOf(cmd.getOptionValue("reward")):1;
        int max_target_seqs = (cmd.hasOption("max_target_seqs")==true)? Integer.valueOf(cmd.getOptionValue("max_target_seqs")):10;
        double evalue = (cmd.hasOption("evalue")==true)? Double.valueOf(cmd.getOptionValue("evalue")):0.001;
        boolean show_gis = cmd.hasOption("show_gis");
        String outfmt = (cmd.hasOption("outfmt")==true)? cmd.getOptionValue("outfmt"): "6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore sscinames sskingdoms";
        String db = (cmd.hasOption("db")==true)? cmd.getOptionValue("db"):"/mnt/hdfs/1/Index_blastn/nt";
        String task = (cmd.hasOption("task")==true)? cmd.getOptionValue("task"):"blastn";
        int num_threads = (cmd.hasOption("num_threads")==true)? Integer.valueOf(cmd.getOptionValue("num_threads")):1;
        String taxname = (cmd.hasOption("taxname")==true)? cmd.getOptionValue("taxname"):"";

        SparkConf conf = new SparkConf().setAppName("BlastN");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("textinputformat.record.delimiter", ">");

        FileSystem fs = FileSystem.get(new Configuration());

        FileStatus[] st = fs.listStatus(new Path(input));
        ArrayList<String> splitFileList = new ArrayList<>();
        for (int i=0;i<st.length;i++){
            if(!st[i].isDirectory()){
                if(st[i].getLen()>1){
                    splitFileList.add(st[i].getPath().toString());
                    System.out.println(st[i].getPath().toString());
                }
            }
        }

        JavaRDD<String> fastaFilesRDD = sc.parallelize(splitFileList, splitFileList.size());
        JavaRDD<String> outRDD = fastaFilesRDD.mapPartitions(f -> {
            Process process;
            String fname = f.next();
            System.out.println("fname: " + fname);
            String blastn_cmd;
            if(task.equalsIgnoreCase("megablast"))
                blastn_cmd = "hdfs dfs -text "+fname+" | blastn -db "+db+" -num_threads "+num_threads+" -task megablast -word_size "+word_size+" -max_target_seqs "+max_target_seqs+" -evalue "+evalue+" " + ((show_gis == true) ? "-show_gis " : "") + " -outfmt "+outfmt;
            else
                blastn_cmd = "hdfs dfs -text "+fname+" | blastn -db "+db+" -num_threads "+num_threads+" -word_size "+word_size+" -gapopen "+gapopen+" -gapextend "+gapextend+" -penalty "+penalty+" -reward "+reward+" -max_target_seqs "+max_target_seqs+" -evalue "+evalue+" " + ((show_gis == true) ? "-show_gis " : "") + " -outfmt "+outfmt;

            System.out.println(blastn_cmd);

            ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", blastn_cmd);
            process = pb.start();

            BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            ArrayList<String> out = new ArrayList<String>();
            while ((line = in.readLine()) != null) {
                out.add(line);
            }

            /*
            BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String e;
            while ((e = err.readLine()) != null) {
                out.add(e);
            }
            */

            process.waitFor();
            in.close();
            return out.iterator();
        });

        if(taxname!="")
            outRDD.filter(res ->{
                String[] fields = res.split("\t");
                String taxonomy = fields[res.length()];
                return taxonomy.equalsIgnoreCase(taxname);
            }).saveAsTextFile(output);
        else
            outRDD.saveAsTextFile(output);

        sc.stop();
    }
}
