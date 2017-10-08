package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**THIS IS IN MEMORY IMPLEMENTATION OF PARALLEL BWA AND READ FILTERING, NO READ SPLIT FILES ARE WRITTEN
 * Usage
 spark-submit --master local[${NUM_EXECUTORS}] --executor-memory 20g --class org.ngseq.metagenomics.NormalizeRDD metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_aligned -out ${OUTPUT_PATH}/${PROJECT_NAME}_normalized -k ${NORMALIZATION_KMER_LEN} -C ${NORMALIZATION_CUTOFF}

 spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 20g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.NormalizeRDD metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_aligned -out ${OUTPUT_PATH}/${PROJECT_NAME}_normalized -k ${NORMALIZATION_KMER_LEN} -C ${NORMALIZATION_CUTOFF}

 **/


public class NormalizeRDD {

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("NormalizeRDD");

    JavaSparkContext sc = new JavaSparkContext(conf);

    Options options = new Options();

    Option opOpt = new Option( "out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
    Option baminOpt = new Option( "in", true, " To read data from HDFS subdirectories use regex /path/**/*" );
    options.addOption( opOpt );
    options.addOption( baminOpt );
    options.addOption(new Option( "k", true, "kmer size" ));
    options.addOption(new Option( "C", true, "minimum coverage" ));
    options.addOption(new Option( "maxc", true, "maximum coverage" ));

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
    String in = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
    int k = (cmd.hasOption("k")==true)? Integer.parseInt(cmd.getOptionValue("k")):16;
    int maxc = (cmd.hasOption("C")==true)? Integer.parseInt(cmd.getOptionValue("C")):20;
    int minc = (cmd.hasOption("minc")==true)? Integer.parseInt(cmd.getOptionValue("minc")):0;

    JavaPairRDD<Text, SequencedFragment> fqRDD = sc.newAPIHadoopFile(in, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

    JavaPairRDD<String, Tuple3<Text, SequencedFragment, Integer>> kmers = fqRDD.mapPartitionsToPair(part -> {
      List<Tuple2<String, Tuple3<Text, SequencedFragment, Integer>>> kmerls = new ArrayList<>();
      while (part.hasNext()) {
        Tuple2<Text, SequencedFragment> fastq = part.next();

        Text readname = new Text(fastq._1.toString());
        String seq = fastq._2.getSequence().toString();

        SequencedFragment sf = new SequencedFragment();
        sf.setQuality(new Text(fastq._2.getQuality().toString()));
        sf.setSequence(new Text(seq));

        for (int i = 0; i < seq.length() - k - 1; i++) {
          String kmer = seq.substring(i, i + k);
          kmerls.add(new Tuple2<String, Tuple3<Text, SequencedFragment, Integer>>(kmer, new Tuple3(readname, sf, 1)));
        }
      }

      return kmerls.iterator();
    });

    JavaPairRDD<String, Tuple3<Text, SequencedFragment, Integer>> reduced = kmers.reduceByKey((a, b) -> {
      SequencedFragment sf = new SequencedFragment();
      sf.setQuality(new Text(a._2().getQuality().toString()));
      sf.setSequence(new Text(a._2().getSequence().toString()));
      return new Tuple3(new Text(a._1().toString()), sf, a._3() + b._3());
    });

    JavaPairRDD<Text, SequencedFragment> filteredRDD = reduced.filter(v -> (v._2._3() < maxc && v._2._3() > minc)).mapToPair(s -> {
      SequencedFragment sf = new SequencedFragment();
      sf.setQuality(new Text(s._2._2().getQuality().toString()));
      sf.setSequence(new Text(s._2._2().getSequence().toString()));
      return new Tuple2<>(new Text(s._2()._1().toString()), sf);
    });

    System.out.println("FILTERED KMERS:"+filteredRDD.count());

    filteredRDD.distinct().saveAsNewAPIHadoopFile(outDir, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());

    sc.stop();

  }

  private static JavaRDD<String> getUniqueKmers(JavaPairRDD<Text, SequencedFragment> fastqRDD, int k) {
    JavaRDD<String> rdd = fastqRDD.mapPartitions(records -> {

      HashSet<String> umer_set = new HashSet<String>();
      while (records.hasNext()) {
        Tuple2<Text, SequencedFragment> fastq = records.next();
        String seq = fastq._2.getSequence().toString();
        //HashSet<String> umer_in_seq = new HashSet<String>();
        for (int i = 0; i < seq.length() - k - 1; i++) {
          String kmer = seq.substring(i, i + k);
          umer_set.add(kmer);
        }
      }
      return umer_set.iterator();
    });

    JavaRDD<String> umersRDD = rdd.distinct();
    //umersRDD.sortBy(s -> s, true, 4);
    return umersRDD;
  }

}