package org.ngseq.metagenomics;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;

import java.io.IOException;

/**
 * Created by ilamaa on 2017-04-14.
 */
public class RepartitionFastq {

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.err.println("Usage: RepartitionFastq <input path> <output path> <number of partitions>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("RepartitionFastq");
        //conf.set("spark.default.parallelism", String.valueOf(args[2]));
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(args[0], FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

        JavaPairRDD<Text, SequencedFragment> repartitioned = fastqRDD.repartition(Integer.valueOf(args[2]));

        repartitioned.saveAsNewAPIHadoopFile(args[1], Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());

        sc.stop();
    }
}
