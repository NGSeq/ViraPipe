package org.ngseq.metagenomics

/**
  * Created by davbzh on 2017-04-26.
  */

object Codondict {

  def count_codons ( ORF1: String, ORF2: String, ORF3: String ) : collection.mutable.Map[String,Int] =  {

    //codonsdict
    val  codonsmap = collection.mutable.Map(
      "TTT" -> 0, "TTC" -> 0, "TTA" -> 0, "TTG" -> 0, "CTT" -> 0,
      "CTC" -> 0, "CTA" -> 0, "CTG" -> 0, "ATT" -> 0, "ATC" -> 0,
      "ATA" -> 0, "ATG" -> 0, "GTT" -> 0, "GTC" -> 0, "GTA" -> 0,
      "GTG" -> 0, "TAT" -> 0, "TAC" -> 0, "TAA" -> 0, "TAG" -> 0,
      "CAT" -> 0, "CAC" -> 0, "CAA" -> 0, "CAG" -> 0, "AAT" -> 0,
      "AAC" -> 0, "AAA" -> 0, "AAG" -> 0, "GAT" -> 0, "GAC" -> 0,
      "GAA" -> 0, "GAG" -> 0, "TCT" -> 0, "TCC" -> 0, "TCA" -> 0,
      "TCG" -> 0, "CCT" -> 0, "CCC" -> 0, "CCA" -> 0, "CCG" -> 0,
      "ACT" -> 0, "ACC" -> 0, "ACA" -> 0, "ACG" -> 0, "GCT" -> 0,
      "GCC" -> 0, "GCA" -> 0, "GCG" -> 0, "TGT" -> 0, "TGC" -> 0,
      "TGA" -> 0, "TGG" -> 0, "CGT" -> 0, "CGC" -> 0, "CGA" -> 0,
      "CGG" -> 0, "AGT" -> 0, "AGC" -> 0, "AGA" -> 0, "AGG" -> 0,
      "GGT" -> 0, "GGC" -> 0, "GGA" -> 0, "GGG" -> 0
    )

    //Create list of
    val ORFs = Array(ORF1, ORF2, ORF3)
    //and loop through them as we need to count codonds in all of them
    ORFs.map(ORF => {
      //println(ORF)
      for (dna_sequence_with_id <- ORF.trim().split(">")) {
        val dna_seq_array = dna_sequence_with_id.trim().split("\n")
        if (dna_seq_array.length > 1){
          //extract Change to upper case
          val dna_sequence = dna_seq_array(1).toUpperCase
          //println(dna_sequence)
          //Catch if codon is not present in the dictionary and continue silently
          scala.util.Try {
            for (i <- 0 to dna_sequence.length by 3) {
              //substring
              var codon = dna_sequence.substring(i, i + 3)
              codonsmap(codon) += 1
            }
          }
        } else {
          //println(dna_seq_array(0))
        }
      }
    }
    )

    return codonsmap
  }

}

