#!/usr/bin/env bash

#See below to set yarn and executor memory options right
#http://stackoverflow.com/questions/38331502/spark-on-yarn-resource-manager-relation-between-yarn-containers-and-spark-execu
#example parameters (data must be in /data/input/example in compressed fastq format and /data/output directory must exist)
#./virapipe.sh /data/input /data/output example

TAXONOMY=viruses #Use some top level Blast taxonomy name from in https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi
DEPLOY_MODE=client
SCHEDULER_MODE=FIFO
CLASSPATH=virapipe-0.9-jar-with-dependencies.jar
INPUT_PATH=${1} #path to HDFS input directory
OUTPUT_PATH=${2} #path to HDFS output directory
PROJECT_NAME=${3} #Used for directory name suffix
REF_INDEX=/index #path to reference index file in local filesystem of every node
ASSEMBLER_THREADS=10
NORMALIZATION_KMER_LEN=16
NORMALIZATION_CUTOFF=15
TEMP_PATH=${OUTPUT_PATH}/temp  #path to temp directory in HDFS
LOCAL_TEMP_PATH=/tmp #path to temp directory in local filesystem of every node
BLAST_TASK=megablast #other option is blastn which is default
BLAST_DATABASE=/database/blast/nt #path to local fs
BLAST_HUMAN_DATABASE=/database/blast/hg
BLAST_PARTITIONS=100 #repartition contigs to this amount (default is same as number of samples)
BLAST_THREADS=12
HMMER_THREADS=12
HMMER_DB=/database/hmmer/vFam-B_2014.hmm
HADOOP_LIB_NATIVE=${!HADOOP_HOME}/lib/native/ #include external libraries e.g. libbwajni.so must exist here
MEGAHIT_BIN=/srv/non_hdfs/megahit/megahit
BLAST_BIN=/usr/bin/blastn
HMMER_BIN=hmmsearch

#EX_MEM=${4}
#NUM_EX=${7}
#EX_CORES=${8}

#Decompress and interleave all data from HDFS input path
spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.driver.extraJavaOptions="-Djava.library.path=${HADOOP_LIB_NATIVE}" --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 10g --conf spark.yarn.executor.memoryOverhead=5000   --class org.ngseq.metagenomics.DecompressInterleave ${CLASSPATH} -in ${INPUT_PATH} -temp ${TEMP_PATH} -out ${OUTPUT_PATH}/${PROJECT_NAME}_interleaved -remtemp

#Align and filter unmapped reads from interleaved reads diretory
#per sample
spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.driver.extraJavaOptions="-Djava.library.path=${HADOOP_LIB_NATIVE}" --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 30g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.AlignInterleavedMulti ${CLASSPATH} -in ${OUTPUT_PATH}/${PROJECT_NAME}_interleaved -out ${OUTPUT_PATH}/${PROJECT_NAME}_aligned -ref ${REF_INDEX}
#all in one
#spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 30g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.AlignInterleaved ${CLASSPATH} -in ${OUTPUT_PATH}/${PROJECT_NAME}_interleaved -out ${OUTPUT_PATH}/${PROJECT_NAME}_aligned -ref ${REF_INDEX}

#Normalize unmapped reads
spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.driver.extraJavaOptions="-Djava.library.path=${HADOOP_LIB_NATIVE}" --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 20g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.NormalizeRDD ${CLASSPATH} -in ${OUTPUT_PATH}/${PROJECT_NAME}_aligned/**/* -out ${OUTPUT_PATH}/${PROJECT_NAME}_normalized -k ${NORMALIZATION_KMER_LEN} -C ${NORMALIZATION_CUTOFF}

#Group output by samples
spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.driver.extraJavaOptions="-Djava.library.path=${HADOOP_LIB_NATIVE}" --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 10g --conf spark.yarn.executor.memoryOverhead=5000   --class org.ngseq.metagenomics.FastqGroupper ${CLASSPATH} -in ${OUTPUT_PATH}/${PROJECT_NAME}_normalized -out ${OUTPUT_PATH}/${PROJECT_NAME}_groupped

#Assembly
spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.driver.extraJavaOptions="-Djava.library.path=${HADOOP_LIB_NATIVE}" --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 20g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.Assemble ${CLASSPATH} -in ${OUTPUT_PATH}/${PROJECT_NAME}_groupped -out ${OUTPUT_PATH}/${PROJECT_NAME}_assembled -localdir ${LOCAL_TEMP_PATH} -single -merge -t ${ASSEMBLER_THREADS}

#rename assembled contigs uniquely
spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.driver.extraJavaOptions="-Djava.library.path=${HADOOP_LIB_NATIVE}" --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 10g --conf spark.yarn.executor.memoryOverhead=5000 --class org.ngseq.metagenomics.RenameContigsUniq ${CLASSPATH} -in ${OUTPUT_PATH}/${PROJECT_NAME}_assembled -out ${OUTPUT_PATH}/${PROJECT_NAME}_contigs -fa fa -partitions ${BLAST_PARTITIONS}

#Blast against human db and filter out human matches
spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.driver.extraJavaOptions="-Djava.library.path=${HADOOP_LIB_NATIVE}" --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 50g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.BlastNFilter ${CLASSPATH} -in ${OUTPUT_PATH}/${PROJECT_NAME}_contigs -out ${OUTPUT_PATH}/${PROJECT_NAME}_blast_nonhuman -db ${BLAST_HUMAN_DATABASE} -task megablast -outfmt 6 -threshold 70 -num_threads ${BLAST_THREADS}
#Blast non human contigs in parallel
spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.driver.extraJavaOptions="-Djava.library.path=${HADOOP_LIB_NATIVE}" --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 50g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.BlastN ${CLASSPATH} -in ${OUTPUT_PATH}/${PROJECT_NAME}_blast_nonhuman -out ${OUTPUT_PATH}/${PROJECT_NAME}_blast_final -db ${BLAST_DATABASE} -num_threads ${BLAST_THREADS} -taxname ${TAXONOMY}
#HMMSearch viral contigs in parallel
spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.driver.extraJavaOptions="-Djava.library.path=${HADOOP_LIB_NATIVE}" --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 50g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.HMMSearch ${CLASSPATH} -in ${INPUT_PATH}/${PROJECT_NAME}_blast_nonhuman -out ${OUTPUT_PATH}/${PROJECT_NAME}_hmm -localdir ${LOCAL_TEMP_PATH} -db ${HMMER_DB} -t ${HMMER_THREADS} -bin ${HMMER_BIN}
