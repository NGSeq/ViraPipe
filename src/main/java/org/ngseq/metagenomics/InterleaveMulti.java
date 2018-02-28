package org.ngseq.metagenomics;


import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.FastqInputFormat.FastqRecordReader;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**Usage
 * spark-submit --master local[15] --class org.ngseq.metagenomics.Interleave target/metagenomics-0.9-jar-with-dependencies.jar -fastq /user/root/reads/reads/forward.fastq -fastq2 /user/root/reads/reads/reverse.fastq -splitsize 100000 -splitout /user/root/interleaved2 -interleave
 **/


public class InterleaveMulti {

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("DecompressInterleave");
    //conf.set("spark.scheduler.mode", "FAIR");
    //conf.set("spark.scheduler.allocation.file", "/opt/cloudera/parcels/CDH-5.10.0-1.cdh5.10.0.p0.41/etc/hadoop/conf.dist/pools.xml");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //sc.setLocalProperty("spark.scheduler.pool", "production");

    Options options = new Options();

    Option splitDirOpt = new Option( "out", true, "Path to output directory in hdfs." );
    Option numsplitsOpt = new Option( "splitsize", true, "Number of reads in split, depends on the size of read file, number of cores and available memory." );
    options.addOption( new Option( "decompress", "" ) );
    options.addOption( new Option( "temp", true, "" ) );
    options.addOption( new Option( "in", true, "" ) );
    options.addOption( new Option( "remtemp", "" ) );
    options.addOption( new Option( "merge", "" ) );

    options.addOption( numsplitsOpt );
    options.addOption( splitDirOpt );
    options.addOption(new Option( "help", "print this message" ));

    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "spark-submit <spark specific args>", options, true );

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
    String input = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
    int splitsize = (cmd.hasOption("splitsize")==true)? Integer.parseInt(cmd.getOptionValue("splitsize")):0;
    boolean merge = cmd.hasOption("merge");
    String outpath = cmd.getOptionValue("out");

    FileSystem fs = FileSystem.get(new Configuration());
        int splitlen = splitsize*4; //FASTQ read is expressed by 4 lines
        FileStatus[] dirs = fs.listStatus(new Path(input));

        Arrays.asList(dirs).forEach(dir ->{
          if(dir.isDirectory()){
            try {
              FileStatus fst = fs.getFileStatus(new Path(input+"/"+dir.getPath().getName()+"/1.fq"));
              FileStatus fst2 = fs.getFileStatus(new Path(input+"/"+dir.getPath().getName()+"/2.fq"));

              if(merge)
                interleaveSplitFastq(fst, fst2, outpath, splitlen, sc);
              else //SAVES SEPARATE HDFS DIRECTORIES
                interleaveSplitFastq(fst, fst2, outpath+"/"+dir.getPath().getName(), splitlen, sc);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
          });

    sc.stop();

  }

  private static void decompress(FileSystem fs, String in, String outpath) throws IOException {

    Configuration conf = new Configuration();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(new Path(in));
    //Decompressing zip file.
    InputStream is = codec.createInputStream(fs.open(new Path(in)));
    OutputStream out = fs.create(new Path(outpath));
    //Write decompressed out
    IOUtils.copyBytes(is, out, conf);
    is.close();
    out.close();
  }


  private static void splitFastq(FileStatus fst, String fqPath, String splitDir, int splitlen, JavaSparkContext sc) throws IOException {
    Path fqpath = new Path(fqPath);
    String fqname = fqpath.getName();
    String[] ns = fqname.split("\\.");
    //TODO: Handle also compressed files
    List<FileSplit> nlif = NLineInputFormat.getSplitsForFile(fst, sc.hadoopConfiguration(), splitlen);

    JavaRDD<FileSplit> splitRDD = sc.parallelize(nlif);

    splitRDD.foreach( split ->  {

      FastqRecordReader fqreader = new FastqRecordReader(new Configuration(), split);
      writeFastqFile(fqreader, new Configuration(), splitDir + "/split_" + split.getStart() + "." + ns[1]);

     });
  }

  private static void writeInterleavedSplits(FastqRecordReader fqreader, FastqRecordReader fqreader2, Configuration config, String fileName){

    ByteArrayOutputStream os = new ByteArrayOutputStream();

    FSDataOutputStream dataOutput = null;
    try {
      FileSystem fs = FileSystem.get(config);
      dataOutput = fs.create(new Path(fileName));
    } catch (IOException e) {
      e.printStackTrace();
    }

    FastqOutputFormat.FastqRecordWriter writer = new FastqOutputFormat.FastqRecordWriter(config, dataOutput);

    try {
      boolean next = true;
      while(next) {
        Text t = fqreader.getCurrentKey();
        SequencedFragment sf = fqreader.getCurrentValue();
        next = fqreader.next(t, sf);

        if(next) writer.write(fqreader.getCurrentKey(), fqreader.getCurrentValue());

        Text t2 = fqreader2.getCurrentKey();
        SequencedFragment sf2 = fqreader2.getCurrentValue();
        fqreader2.next(t2, sf2);

        if(next) writer.write(fqreader2.getCurrentKey(), fqreader2.getCurrentValue());

      }

      dataOutput.close();
      os.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void writeFastqFile(FastqRecordReader fqreader, Configuration config, String fileName){

    ByteArrayOutputStream os = new ByteArrayOutputStream();

    FSDataOutputStream dataOutput = null;
    try {
      FileSystem fs = FileSystem.get(config);
      dataOutput = new FSDataOutputStream(os);
      dataOutput = fs.create(new Path(fileName));
    } catch (IOException e) {
      e.printStackTrace();
    }

    FastqOutputFormat.FastqRecordWriter writer = new FastqOutputFormat.FastqRecordWriter(config, dataOutput);

    try {
      boolean next = true;
      while(next) {
        Text t = fqreader.getCurrentKey();
        SequencedFragment sf = fqreader.getCurrentValue();
        next = fqreader.next(t, sf);
        writer.write(fqreader.getCurrentKey(), fqreader.getCurrentValue());
      }

      dataOutput.close();
      os.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void interleaveSplitFastq(FileStatus fst, FileStatus fst2, String splitDir, int splitlen, JavaSparkContext sc) throws IOException {

    List<FileSplit> nlif = NLineInputFormat.getSplitsForFile(fst, sc.hadoopConfiguration(), splitlen);
    List<FileSplit> nlif2 = NLineInputFormat.getSplitsForFile(fst2, sc.hadoopConfiguration(), splitlen);

    JavaRDD<FileSplit> splitRDD = sc.parallelize(nlif);
    JavaRDD<FileSplit> splitRDD2 = sc.parallelize(nlif2);
    JavaPairRDD<FileSplit, FileSplit> zips = splitRDD.zip(splitRDD2);

    zips.foreach( splits ->  {
      Path path = splits._1.getPath();
      FastqRecordReader fqreader = new FastqRecordReader(new Configuration(), splits._1);
      FastqRecordReader fqreader2 = new FastqRecordReader(new Configuration(), splits._2);
      writeInterleavedSplits(fqreader, fqreader2, new Configuration(), splitDir+"/"+path.getParent().getName()+"_"+splits._1.getStart()+".fq");
    });
  }

 private static FastqRecordReader readFastqSplits(String fastqfile, Configuration config, long start, long length) throws IOException {

    FileSplit split = new FileSplit(new Path(fastqfile), start, length, null);

    return new FastqRecordReader(config, split);
  }

}