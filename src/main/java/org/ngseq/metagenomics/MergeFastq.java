package org.ngseq.metagenomics;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;

import java.io.IOException;

/**
 * Created by davbzh on 2017-04-14.
 */
public class MergeFastq {

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.err.println("Usage: MergeFastq <input path> <output path> <number of partitions>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("MergeFastq");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(args[0], FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

        JavaPairRDD<Text, SequencedFragment> coalesced = fastqRDD.coalesce(Integer.valueOf(args[2]));

        coalesced.saveAsNewAPIHadoopFile(args[1], Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());

        sc.stop();
    }
}
