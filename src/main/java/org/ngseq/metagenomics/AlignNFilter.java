package org.ngseq.metagenomics;


import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.ShortRead;
import htsjdk.samtools.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**Usage
 *  Usage example:
 //ON SINGLE NODE
 spark-submit --master local[${NUM_EXECUTORS}] --executor-memory 30g --class fi.aalto.ngs.metagenomics.AlignNFilter metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_interleaved -out ${OUTPUT_PATH}/${PROJECT_NAME}_aligned -ref ${REF_INDEX_IN_LOCAL_FS} -unmapped -avgq 50 -lowqc 50 -lowqt 64
 //ON YARN CLUSTER
 spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 30g --conf spark.yarn.executor.memoryOverhead=10000  --class fi.aalto.ngs.metagenomics.AlignNFilter metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_interleaved -out ${OUTPUT_PATH}/${PROJECT_NAME}_aligned -ref ${REF_INDEX_IN_LOCAL_FS} -unmapped -avgq 50 -lowqc 50 -lowqt 64

 **/

public class AlignNFilter {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("AlignNFilter");
        //conf.set("spark.scheduler.mode", "FAIR");
        //conf.set("spark.scheduler.allocation.file", "/opt/cloudera/parcels/CDH-5.10.0-1.cdh5.10.0.p0.41/etc/hadoop/conf.dist/pools.xml");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.setLocalProperty("spark.scheduler.pool", "production");

        Options options = new Options();
        Option pathOpt = new Option( "in", true, "Path to fastq file in hdfs." );

        Option refOpt = new Option( "ref", true, "Path to fasta reference file in local FS." );

        Option fqoutOpt = new Option( "out", true, "" );
        Option formatOpt = new Option( "format", true, "bam or sam, fastq is default" );
        Option umOpt = new Option( "unmapped", "keep unmapped, true or false. If not any given, all alignments/reads are persisted" );
        Option mappedOpt = new Option( "mapped", "keep mapped, true or false. If not any given, all alignments/reads are persisted" );

        options.addOption(new Option( "avgq",true, "Minimum value for quality average score" ));
        options.addOption(new Option( "lowqc", true, "Maximum count for low quality bases under threshold, lowqt must be given also."));
        options.addOption(new Option( "lowqt", true, "Threshold for low quality." ));

        options.addOption( pathOpt );
        options.addOption( refOpt );
        options.addOption( formatOpt );
        options.addOption( fqoutOpt );
        options.addOption( umOpt );
        options.addOption( mappedOpt );

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
        String fastq = cmd.getOptionValue("fastq");
        String ref = (cmd.hasOption("ref")==true)? cmd.getOptionValue("ref"):null;
        boolean unmapped = cmd.hasOption("unmapped");
        boolean mapped = cmd.hasOption("mapped");
        String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
        String format = (cmd.hasOption("format") == true) ? cmd.getOptionValue("format") : "bam";

        int minAvgQuality = (cmd.hasOption("avgq")==true)? Integer.parseInt(cmd.getOptionValue("avgq")):0;
        int maxLowQualityCount = (cmd.hasOption("lowqc")==true)? Integer.parseInt(cmd.getOptionValue("lowqc")):0;
        int qualityThreshold = (cmd.hasOption("lowqt")==true)? Integer.parseInt(cmd.getOptionValue("lowqt")):0;

        JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(fastq, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

        JavaRDD<String> alignmentRDD = fastqRDD.mapPartitions(split -> {
            System.loadLibrary("bwajni");
            BwaIndex index = new BwaIndex(new File(ref));
            BwaMem mem = new BwaMem(index);

            List<ShortRead> L1 = new ArrayList<ShortRead>();
            List<ShortRead> L2 = new ArrayList<ShortRead>();

            while (split.hasNext()) {
                Tuple2<Text, SequencedFragment> next = split.next();
                String key = next._1.toString();
                String[] keysplit=key.split(" ");
                key = keysplit[0];

                SequencedFragment origsf = next._2;
                String quality = origsf.getQuality().toString();
                String sequence = origsf.getSequence().toString();
                SequencedFragment sf = copySequencedFragment(origsf, sequence, quality);

                //Apply quality filters if given
                if (maxLowQualityCount != 0) {
                    if (!lowQCountTest(maxLowQualityCount, qualityThreshold, sf.getQuality().toString().getBytes())){
                        split.next(); //We skip over read pair
                        continue;
                    }
                }
                if (minAvgQuality != 0) {
                    if (!avgQualityTest(minAvgQuality, sf.getQuality().toString().getBytes())) {
                        split.next(); //We skip over read pair
                        continue;
                    }
                }

                if(split.hasNext()){

                    Tuple2<Text, SequencedFragment> next2 = split.next();
                    String key2 = next2._1.toString();
                    String[] keysplit2=key2.split(" ");
                    key2 = keysplit2[0];

                    SequencedFragment origsf2 = next2._2;
                    String quality2 = origsf2.getQuality().toString();
                    String sequence2 = origsf2.getSequence().toString();
                    SequencedFragment sf2 = copySequencedFragment(origsf2, sequence2, quality2);

                    //Apply quality filters if given
                    if (maxLowQualityCount != 0) {
                        if (!lowQCountTest(maxLowQualityCount, qualityThreshold, sf2.getQuality().toString().getBytes()))
                            continue;
                    }
                    if (minAvgQuality != 0) {
                        if (!avgQualityTest(minAvgQuality, sf2.getQuality().toString().getBytes()))
                            continue;
                    }

                    if(key.equalsIgnoreCase(key2)){
                        L1.add(new ShortRead(key, sf.getSequence().toString().getBytes(), sf.getQuality().toString().getBytes()));
                        L2.add(new ShortRead(key2, sf2.getSequence().toString().getBytes(), sf2.getQuality().toString().getBytes()));
                    }else
                        split.next();

                }
            }

            String[] aligns = mem.align(L1, L2);

            if (aligns != null) {

                ArrayList<String> filtered = new ArrayList<String>();
                if(mapped==true){
                    final SAMLineParser samLP = new SAMLineParser(new DefaultSAMRecordFactory(), ValidationStringency.SILENT,  new SAMFileHeader(), null, null);
                    Arrays.asList(aligns).forEach(aln -> {
                       //String flag = fields[1];
                        SAMRecord record = samLP.parseLine(aln);
                        //We want only mapped reads
                        if(!record.getReadUnmappedFlag()){
                            filtered.add(aln);
                        }

                    });
                    return filtered.iterator();
                }
                else if(unmapped==true){
                    final SAMLineParser samLP = new SAMLineParser(new DefaultSAMRecordFactory(), ValidationStringency.SILENT,  new SAMFileHeader(), null, null);
                    Arrays.asList(aligns).forEach(aln -> {
                        //String flag = fields[1];
                        SAMRecord record = samLP.parseLine(aln);
                        //We want only unmapped reads
                        if(record.getReadUnmappedFlag()){
                            filtered.add(aln);
                        }
                    });
                    return filtered.iterator();
                }
                else{
                    return Arrays.asList(aligns).iterator(); //NO MAPPING FILTER
                }
            } else
                return (new ArrayList<String>()).iterator(); //NULL ALIGNMENTS

        });

        if(format!=null)
            new HDFSWriter(alignmentRDD, outDir, format, sc);
        else
            alignmentRDD.saveAsTextFile(outDir);

        sc.stop();

    }

    private static boolean avgQualityTest(double minAvgQuality, byte[] bytes) {
        int qSum = 0;
        for(byte b : bytes){
            qSum+=b;
        }
        if((qSum/bytes.length) > minAvgQuality)
            return true;

        return false;
    }

    private static boolean lowQCountTest(int maxLowQualityCount, int qualityThreshold, byte[] bytes) {
        int lowqCount = 0;
        for(byte b : bytes){
            if(b<qualityThreshold)
                lowqCount++;
        }
        if(lowqCount < maxLowQualityCount)
            return true;

        return false;
    }

    private static SequencedFragment copySequencedFragment(SequencedFragment sf, String sequence, String quality) {
        SequencedFragment copy = new SequencedFragment();

        copy.setControlNumber(sf.getControlNumber());
        copy.setFilterPassed(sf.getFilterPassed());
        copy.setFlowcellId(sf.getFlowcellId());
        copy.setIndexSequence(sf.getIndexSequence());
        copy.setInstrument(sf.getInstrument());
        copy.setLane(sf.getLane());
        copy.setQuality(new Text(quality));
        copy.setRead(sf.getRead());
        copy.setRunNumber(sf.getRunNumber());
        copy.setSequence(new Text(sequence));
        copy.setTile(sf.getTile());
        copy.setXpos(sf.getXpos());
        copy.setYpos(sf.getYpos());

        return copy;
    }


}