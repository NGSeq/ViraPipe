#!/usr/bin/env bash

#See below to set yarn and executor memory options right
#http://stackoverflow.com/questions/38331502/spark-on-yarn-resource-manager-relation-between-yarn-containers-and-spark-execu
#example parameters (data must be in /path/to/contigs in fasta format directory must exist)
#./blast_search.sh /path/to/contigs /data/output example

TAXONOMY=viruses #Use some top level Blast taxonomy name from in https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi
DEPLOY_MODE=client
SCHEDULER_MODE=FIFO
CLASSPATH=virapipe-0.9-jar-with-dependencies.jar
INPUT_PATH=${1} #path to HDFS input directory
OUTPUT_PATH=${2} #path to HDFS output directory
TEMP_PATH=${OUTPUT_PATH}/temp  #path to temp directory in HDFS
LOCAL_TEMP_PATH=/temp #path to temp directory in local filesystem of every node
BLAST_TASK=megablast #other option is blastn which is default
BLAST_DATABASE=/database/blast/nt #path to local fs
BLAST_HUMAN_DATABASE=/database/blast/hg
BLAST_THREADS=12
#EX_MEM=${4}
#NUM_EX=${7}
#EX_CORES=${8}
#export BLASTDB=$BLASTDB:$BLAST_DATABASE;$BLAST_HUMAN_DATABASE

#Blast against human db and filter out human matches
spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 50g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.BlastNFilter ${CLASSPATH} -in ${INPUT_PATH} -out ${OUTPUT_PATH}/blast_nonhuman -db ${BLAST_HUMAN_DATABASE} -task megablast -outfmt 6 -threshold 70 -num_threads ${BLAST_THREADS}
#Blast non human contigs per file in parallel
spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 50g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.BlastN ${CLASSPATH} -in ${OUTPUT_PATH}/blast_nonhuman -out ${OUTPUT_PATH}/blast_final -num_threads ${BLAST_THREADS} -taxname ${TAXONOMY}
