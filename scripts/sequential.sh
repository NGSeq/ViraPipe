#!/usr/bin/env bash
#This is a script for running sequential metagenomics analysis pipeline

SAMPLES=13

for i in {1..$SAMPLES}
  do
                #Align
				bwa mem -t 56 /db/index/hg19/hg19_unmasked /data/fastq/forward$i.fq.gz /data/fastq/reverse$i.fq.gz > out/aligned$i.sam
                samtools sort -@ 56 out/aligned$i.sam | samtools view -@ 56 -f 4 > out/unmapped$i.fq
				#Preprocess and filter
                awk '{print "@"$1}' out/unmapped$i.fq > out/unmapped_ids$i
                zcat /data/fastq/f$i.fq.gz | paste - - - - | awk -F" " '{print $1,$2,$3,$4}' > out/fwd_1l$i.fq
                zcat /data/fastq/r$i.fq.gz | paste - - - - | awk -F" " '{print $1,$2,$3,$4}' > out/rev_1l$i.fq
                awk -F" " 'NR==FNR{a[$1];next} ($1 in a) {print $1"/1",$2,$3,$4}' out/unmapped_ids$i out/fwd_1l$i.fq > out/ufwd_1liner$i.fq
                awk -F" " 'NR==FNR{a[$1];next} ($1 in a) {print $1"/2",$2,$3,$4}' out/unmapped_ids$i out/rev_1l$i.fq > out/urev_1liner$i.fq
                paste -d '\n' out/ufwd_1liner$i.fq out/urev_1liner$i.fq | tr ' ' '\n' > out/unmapped_dn$i.fq
				#normalization
                normalize-by-median.py -C 15 -k 16 -N 4 -x 20e9 -p out/unmapped_dn$i.fq
                mv unmapped_dn$i.fq.keep out/unmapped_dn$i.fq.keep
				#assembly
                megahit -t 56 -m 0.9 --12 out/unmapped_dn$i.fq.keep -o out/assembled$i
				#Blast search human db
                blastn -db /db/blast/human_genomic -num_threads 56 -task megablast -word_size 11 -max_target_seqs 10 -evalue 0.001 -outfmt 6 -query out/assembled$i/final.contigs.fa -out out/humans$i
                cat out/assembled$i/final.contigs.fa | paste - - | awk -F" " '{print $1,$5}' > out/contigs_1l$i.fa
				#Filter non-human contigs
                python filter.py out/contigs_1l$i.fa out/humans$i > out/nonhumans$i.fa
				#Blast search nt db
                blastn -db /db/blast/nt -num_threads 56 -word_size 11 -max_target_seqs 10 -evalue 0.001 -outfmt "6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore sscinames sskingdoms" -query out/nonhumans$i.fa -out out/nonhumans$i.nt
				grep viruses out/nonhumans$i.nt > out/viruses$i.nt
				#HMMER
                hmmsearch --noali --cpu 56 -o out/lsu$i.txt --tblout out/lsu$i.table.txt /db/vfam/vFam-A_2014.hmm out/nonhumans$i.fa
done
