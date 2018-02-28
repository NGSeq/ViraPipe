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
import scala.Tuple2;
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
 spark-submit  --master local[${NUM_EXECUTORS}] --executor-memory 10g --class org.ngseq.metagenomics.DecompressInterleave metagenomics-0.9-jar-with-dependencies.jar -in ${INPUT_PATH} -temp ${TEMP_PATH} -out ${OUTPUT_PATH}/${PROJECT_NAME}_interleaved -remtemp
 *
 spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 10g --conf spark.yarn.executor.memoryOverhead=5000   --class org.ngseq.metagenomics.DecompressInterleave metagenomics-0.9-jar-with-dependencies.jar -in ${INPUT_PATH} -temp ${TEMP_PATH} -out ${OUTPUT_PATH}/${PROJECT_NAME}_interleaved -remtemp
 **/


public class DecompressInterleave {

  public DecompressInterleave() {
  }

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("DecompressInterleave");
    //conf.set("spark.scheduler.mode", "FAIR");
    //conf.set("spark.scheduler.allocation.file", "/opt/cloudera/parcels/CDH-5.10.0-1.cdh5.10.0.p0.41/etc/hadoop/conf.dist/pools.xml");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //sc.setLocalProperty("spark.scheduler.pool", "production");

    Options options = new Options();
    options.addOption( new Option( "in", true, "" ) );
    options.addOption( new Option( "out", true, "Path to output directory in hdfs." ) );
    options.addOption( new Option( "remtemp", "" ) );
    options.addOption( new Option( "temp", true, "" ) );
    options.addOption(new Option( "help", "print this message" ));

    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "spark-submit <spark specific args>", options, true );

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse( options, args );
    }
    catch( ParseException exp ) {
      System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
    }
    String input = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
    String temppath = cmd.getOptionValue("temp");
    String outpath = cmd.getOptionValue("out");

    boolean remtemp = cmd.hasOption("remtemp");

    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus[] st = fs.listStatus(new Path(input));
    ArrayList<Tuple3<String, String, Integer>> splitFileList = new ArrayList<>();

    ArrayList<FileStatus> compressedfiles = new ArrayList<>();
    for (FileStatus f : Arrays.asList(st)) {
      if (f.getPath().getName().endsWith(".gz") || f.getPath().getName().endsWith(".zip") || f.getPath().getName().endsWith(".tar") || f.getPath().getName().endsWith(".bz"))
        compressedfiles.add(f);
    }

    Iterator<FileStatus> it = compressedfiles.iterator();
    int count = 1;
    while (it.hasNext()) {
      String fn1 = it.next().getPath().getName();
      if (it.hasNext()) {
        String fn2 = it.next().getPath().getName();
        splitFileList.add(new Tuple3<>(fn1, fn2, count));
        count++;
      }
    }

    JavaRDD<Tuple3<String, String, Integer>> filesRDD = sc.parallelize(splitFileList, splitFileList.size());
    filesRDD.foreachPartition(files -> {
        Tuple3<String, String, Integer> f = files.next();
        FileStatus f1 = decompress(FileSystem.get(new Configuration()), input + "/" + f._1(), temppath + "/" + f._3() + "/"+f._1().split("\\.")[0]+"#1.fq");
        FileStatus f2 = decompress(FileSystem.get(new Configuration()), input + "/" + f._2(), temppath + "/" + f._3() + "/"+f._2().split("\\.")[0]+"#2.fq");

        FileSplit split1 = new FileSplit(f1.getPath(), 0, f1.getLen(), null);
        FileSplit split2 = new FileSplit(f2.getPath(), 0, f2.getLen(), null);

        FastqRecordReader fqreader = new FastqRecordReader(new Configuration(), split1);
        FastqRecordReader fqreader2 = new FastqRecordReader(new Configuration(), split2);

        writeInterleavedSplits(fqreader, fqreader2, new Configuration(), outpath, split1.getPath().getName());

        //Store to subdirectories
        //writeInterleavedSplits(fqreader, fqreader2, new Configuration(), outpath+"/"+split1.getPath().getParent().getName(), split1.getPath().getName());

    });

    if(remtemp){
      fs.delete(new Path(temppath), true);
    }

    sc.stop();

  }

  private static FileStatus decompress(FileSystem fs, String in, String outpath) throws IOException {
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
    return fs.getFileStatus(new Path(outpath));
  }

  public static void interleaveSplitFastq(FileStatus fst, FileStatus fst2, String splitDir, int splitlen, JavaSparkContext sc) throws IOException {

    String[] ns = fst.getPath().getName().split("\\.");
    //TODO: Handle also compressed files
    List<FileSplit> nlif = NLineInputFormat.getSplitsForFile(fst, sc.hadoopConfiguration(), splitlen);
    List<FileSplit> nlif2 = NLineInputFormat.getSplitsForFile(fst2, sc.hadoopConfiguration(), splitlen);

    JavaRDD<FileSplit> splitRDD = sc.parallelize(nlif);
    JavaRDD<FileSplit> splitRDD2 = sc.parallelize(nlif2);
    JavaPairRDD<FileSplit, FileSplit> zips = splitRDD.zip(splitRDD2);

    zips.foreach( splits ->  {
      Path path = splits._1.getPath();
      FastqRecordReader fqreader = new FastqRecordReader(new Configuration(), splits._1);
      FastqRecordReader fqreader2 = new FastqRecordReader(new Configuration(), splits._2);
      writeInterleavedSplits(fqreader, fqreader2, new Configuration(), splitDir, path.getParent().getName()+"_"+splits._1.getStart()+".fq");
    });
  }

  private static void splitFastq(FileStatus fst, String splitDir, int splitlen, JavaSparkContext sc) throws IOException {

    //TODO: Handle also compressed files
    List<FileSplit> nlif = NLineInputFormat.getSplitsForFile(fst, new Configuration(), splitlen);

    JavaRDD<FileSplit> splitRDD = sc.parallelize(nlif);

    splitRDD.foreach( split ->  {

      FastqRecordReader fqreader = new FastqRecordReader(new Configuration(), split);
      writeFastqFile(fqreader, new Configuration(), splitDir + "/" + split.getPath().getName()+"_"+split.getStart() + ".fq");

     });
  }

  private static void writeInterleavedSplits(FastqRecordReader fqreader, FastqRecordReader fqreader2, Configuration config, String path, String fileName){

    ByteArrayOutputStream os = new ByteArrayOutputStream();

    FSDataOutputStream dataOutput = null;
    try {
      FileSystem fs = FileSystem.get(config);
      dataOutput = new FSDataOutputStream(os);
      dataOutput = fs.create(new Path(path+"/"+fileName));
    } catch (IOException e) {
      e.printStackTrace();
    }

    FastqOutputFormat.FastqRecordWriter writer = new FastqOutputFormat.FastqRecordWriter(config, dataOutput);

    String sampleid = fileName.split("#")[0];

    try {
      boolean next = true;
      while(next) {
        Text t = fqreader.getCurrentKey();
        SequencedFragment sf = fqreader.getCurrentValue();
        next = fqreader.next(t, sf);
        //TODO: create new read names for grouping per sample later
        String newname = t.toString().replace(t.toString().split(":")[0], sampleid);

        if(next) {

          Text t2 = fqreader2.getCurrentKey();
          SequencedFragment sf2 = fqreader2.getCurrentValue();
          next = fqreader2.next(t2, sf2);
          String newname2 = t2.toString().replace(t2.toString().split(":")[0], sampleid);

          if (next){
            writer.write(new Text(newname), fqreader.getCurrentValue());
            writer.write(new Text(newname2), fqreader2.getCurrentValue());
          }
        }
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

 private static FastqRecordReader readFastqSplits(String fastqfile, Configuration config, long start, long length) throws IOException {

    FileSplit split = new FileSplit(new Path(fastqfile), start, length, null);

    return new FastqRecordReader(config, split);
  }


}