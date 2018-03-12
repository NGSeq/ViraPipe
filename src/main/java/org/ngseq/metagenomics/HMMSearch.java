package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;


public class HMMSearch {

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("Assemble");
    JavaSparkContext sc = new JavaSparkContext(conf);

    Options options = new Options();

    Option inOpt = new Option( "in", true, "" );
    Option cOpt = new Option( "t", true, "Threads" );
    Option ouOpt = new Option( "out", true, "" );

    options.addOption(new Option( "localdir", true, "Absolute path to local temp dir"));
    options.addOption(  new Option( "debug", "saves error log" ) );
    options.addOption(  new Option( "bin", true,"Path to megahit binary, defaults calls 'megahit'" ) );
    options.addOption(new Option( "db", true, "Path to local BlastNT database (database must be available on every node under the same path)" ));

    options.addOption( inOpt );
    options.addOption( cOpt );
    options.addOption( ouOpt );

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
    String inputPath = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
    String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
    String localdir = cmd.getOptionValue("localdir");
    boolean debug = cmd.hasOption("debug");
    String db = (cmd.hasOption("db")==true)? cmd.getOptionValue("db"):"Path to local hmmer database (database must be available on every node under the same path)"; //We want to filter out human matches, so use human db as default
    String bin = (cmd.hasOption("bin")==true)? cmd.getOptionValue("bin"):"hmmsearch";

    int t = (cmd.hasOption("t")==true)? Integer.valueOf(cmd.getOptionValue("t")):1;

    FileSystem fs = FileSystem.get(new Configuration());
    fs.mkdirs(fs,new Path(outDir),new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));

    ArrayList<String> splitFileList = new ArrayList<>();

      FileStatus[] st = fs.listStatus(new Path(inputPath));
      for (int i=0;i<st.length;i++){
        String fn = st[i].getPath().getName().toString();
        if(!fn.equalsIgnoreCase("_SUCCESS"))
          splitFileList.add(st[i].getPath().toUri().getRawPath().toString());
      }


    JavaRDD<String> splitFilesRDD = sc.parallelize(splitFileList, splitFileList.size());
    Broadcast<String> bs = sc.broadcast(fs.getUri().toString());

    JavaRDD<String> outRDD = splitFilesRDD.mapPartitions(f -> {
      String path = f.next();
      System.out.println(path);
      String fname;
      if(path.lastIndexOf(".")<path.lastIndexOf("/"))
        fname = path.substring(path.lastIndexOf("/")+1);
      else fname = path.substring(path.lastIndexOf("/")+1, path.lastIndexOf("."));

     // String tempName = String.valueOf((new Date()).getTime());

      DFSClient client = new DFSClient(URI.create(bs.getValue()), new Configuration());
      DFSInputStream hdfsstream = client.open(path);

      String ass_cmd = bin+" --noali --cpu "+t+" -o "+localdir+"/lsu."+fname+".txt --tblout "+localdir+"/lsu."+fname+".table.txt " +db+ " /dev/stdin";
      System.out.println(ass_cmd);

      ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", ass_cmd);
      Process process = pb.start();

      BufferedReader hdfsinput = new BufferedReader(new InputStreamReader(hdfsstream));
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
      String line;
      while ((line = hdfsinput.readLine()) != null) {
        writer.write(line);
        writer.newLine();
      }
      writer.close();

      ArrayList<String> out = new ArrayList<String>();

      BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      String e;
      while ((e = err.readLine()) != null) {
        System.out.println(e);
        out.add(e);
      }

/*      String mkdir = System.getenv("HADOOP_HOME")+"/bin/hdfs dfs -mkdir "+outDir;
      String chown = System.getenv("HADOOP_HOME")+"/bin/hdfs dfs -chown yarn "+outDir;

      Process mkpb = new ProcessBuilder("/bin/sh", "-c", mkdir).start();
      Process chpb = new ProcessBuilder("/bin/sh", "-c", chown).start();
      
      BufferedReader err1 = new BufferedReader(new InputStreamReader(dprocess.getErrorStream()));
      String e1;
      while ((e1 = err1.readLine()) != null) {
        System.out.println(e1);
        out.add(e1);
      }
      dprocess.waitFor();	*/

      String copy_cmd = System.getenv("HADOOP_HOME")+"/bin/hdfs dfs -put "+localdir+"/lsu."+fname+"* " +outDir;

      ProcessBuilder pb2 = new ProcessBuilder("/bin/sh", "-c", copy_cmd);
      Process process2 = pb2.start();

      BufferedReader err2 = new BufferedReader(new InputStreamReader(process2.getErrorStream()));
      String e2;
      while ((e2 = err2.readLine()) != null) {
        System.out.println(e2);
        out.add(e2);
      }
      process2.waitFor();

      String delete_cmd = "rm -rf "+localdir+"/lsu."+fname+"*";

      ProcessBuilder pb3 = new ProcessBuilder("/bin/sh", "-c", delete_cmd);
      Process process3 = pb3.start();
      BufferedReader err3 = new BufferedReader(new InputStreamReader(process3.getErrorStream()));
      String e3;
      while ((e3 = err3.readLine()) != null) {
        System.out.println(e3);
        out.add(e3);
      }
      process3.waitFor();

      out.add(ass_cmd);
      out.add(copy_cmd);
      out.add(delete_cmd);

      return out.iterator();
    });
    if(debug) outRDD.saveAsTextFile("hmm_errorlog/"+String.valueOf(new Date().getTime()));
    else outRDD.foreach(err -> System.out.println(err));
        
    sc.stop();

  }

}
