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
 spark-submit  --master local[${NUM_EXECUTORS}] --executor-memory 10g --class fi.aalto.ngs.metagenomics.Decompress metagenomics-0.9-jar-with-dependencies.jar -in ${INPUT_PATH} -temp ${TEMP_PATH} -out ${OUTPUT_PATH}/${PROJECT_NAME}_interleaved -remtemp
 *
 spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 10g --conf spark.yarn.executor.memoryOverhead=5000   --class fi.aalto.ngs.metagenomics.Decompress metagenomics-0.9-jar-with-dependencies.jar -in ${INPUT_PATH} -temp ${TEMP_PATH} -out ${OUTPUT_PATH}/${PROJECT_NAME}_interleaved -remtemp
 **/

public class Decompress {

  public Decompress() {
  }

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("Decompress");
    //conf.set("spark.scheduler.mode", "FAIR");
    //conf.set("spark.scheduler.allocation.file", "/opt/cloudera/parcels/CDH-5.10.0-1.cdh5.10.0.p0.41/etc/hadoop/conf.dist/pools.xml");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //sc.setLocalProperty("spark.scheduler.pool", "production");

    Options options = new Options();

    Option splitDirOpt = new Option( "out", true, "Path to output directory in hdfs." );
    options.addOption( new Option( "temp", true, "" ) );
    options.addOption( new Option( "in", true, "" ) );
    options.addOption( new Option( "remtemp", "" ) );

    options.addOption( splitDirOpt );
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
    String outpath = cmd.getOptionValue("out");

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
            decompress(FileSystem.get(new Configuration()), input + "/" + f._1(), outpath + "/" + f._3() + "/1.fq");
            decompress(FileSystem.get(new Configuration()), input + "/" + f._2(), outpath + "/" + f._3() + "/2.fq");

          });


    sc.stop();

  }

  private static void decompress(FileSystem fs, String in, String outpath) throws IOException {
    Configuration conf = new Configuration();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    // the correct codec will be discovered by the extension of the file

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