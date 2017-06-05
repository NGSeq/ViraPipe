package org.ngseq.metagenomics

import Codondict._
/**
  * Created by davbzh on 2017-05-01.
  */

object ORF {

  //Reverse complement dna string
  def reverscomplement(seq: String): String = {


    val reversecomp = new StringBuilder
    val result = new StringBuilder

    /*
    for (nuceotide <- seq ) {
      if (nuceotide == 'A') reversecomp.append('T')
      else if (nuceotide == 'T') reversecomp.append('A')
      else if (nuceotide == 'C') reversecomp.append('G')
      else if (nuceotide == 'G') reversecomp.append('C')
    }
    */

    seq.map(nuceotide => {
      if (nuceotide == 'A') reversecomp.append('T')
      else if (nuceotide == 'T') reversecomp.append('A')
      else if (nuceotide == 'C') reversecomp.append('G')
      else if (nuceotide == 'G') reversecomp.append('C')
      else if (nuceotide == 'N') reversecomp.append('N')
    }
    )

    val reverse_comp = reversecomp.toString
    for (i <- (reverse_comp.length - 1) to 1 by -1) {
      result.append(reverse_comp(i))
    }
    return  result.toString
  }

  //DNA to proteing translator
  def dna2orf (id: String, dnaseq: String, framestart: Int, dir: String, minlength: Int): Array[String] = {

    //Initiate necessary variables
    val dna_sequence =  dnaseq.toUpperCase
    //Start end variables
    var start = -1
    var end = -1
    var tmp_end = -1
    var nrorf = 0
    //result variables
    val protein_orf = new StringBuilder
    val dna_orf = new StringBuilder
    val result = new Array[String](2)

    //Codon dictionnary
    val codon2aa = Map (
      "ATA"->"I", "ATC"->"I", "ATT"->"I", "ATG"->"M",
      "ACA"->"T", "ACC"->"T", "ACG"->"T", "ACT"->"T",
      "AAC"->"N", "AAT"->"N", "AAA"->"K", "AAG"->"K",
      "AGC"->"S", "AGT"->"S", "AGA"->"R", "AGG"->"R",
      "CTA"->"L", "CTC"->"L", "CTG"->"L", "CTT"->"L",
      "CCA"->"P", "CCC"->"P", "CCG"->"P", "CCT"->"P",
      "CAC"->"H", "CAT"->"H", "CAA"->"Q", "CAG"->"Q",
      "CGA"->"R", "CGC"->"R", "CGG"->"R", "CGT"->"R",
      "GTA"->"V", "GTC"->"V", "GTG"->"V", "GTT"->"V",
      "GCA"->"A", "GCC"->"A", "GCG"->"A", "GCT"->"A",
      "GAC"->"D", "GAT"->"D", "GAA"->"E", "GAG"->"E",
      "GGA"->"G", "GGC"->"G", "GGG"->"G", "GGT"->"G",
      "TCA"->"S", "TCC"->"S", "TCG"->"S", "TCT"->"S",
      "TTC"->"F", "TTT"->"F", "TTA"->"L", "TTG"->"L",
      "TAC"->"Y", "TAT"->"Y", "TAA"->"*", "TAG"->"*",
      "TGC"->"C", "TGT"->"C", "TGA"->"*", "TGG"->"W"
    )

    //Loop throuhg dna sequence by window of 3
    for (i <- 0 to dna_sequence.length by 3) {

      //Catch if codon is not present in the dictionary and continue silently
      scala.util.Try {
        //If start codon
        val codon = dna_sequence.substring(i, i + 3)
        //Using all alternative start codons
        if (codon.equals("ATG") || codon.equals("TTG") || codon.equals("GTG") || codon.equals("CTG")){
          start = i
          var protein = new StringBuilder

          for (from_start_codon <- start to dna_sequence.length by 3) {
            protein.append(codon2aa(dna_sequence.substring(from_start_codon, from_start_codon + 3)))
            if (codon2aa(dna_sequence.substring(from_start_codon, from_start_codon + 3)).equals("*")){
              if ((from_start_codon + 3) - start >= minlength && start >= 0) {
                if (nrorf == 0) {
                  end = from_start_codon + 3
                  tmp_end = end
                  nrorf += 1
                  protein_orf.append(s">${id}_${dir}_${framestart}_${nrorf}\n${protein.dropRight(1)}\n")
                  dna_orf.append(s">${id}_${dir}_${framestart}_${nrorf}_${start}-${end}\n${dna_sequence.substring(start, end)}\n")
                  protein.clear()
                } else if (nrorf > 0 && start > tmp_end) {
                  end = from_start_codon + 3
                  tmp_end = end
                  protein_orf.append(s">${id}_${dir}_${framestart}_${nrorf}\n${protein.dropRight(1)}\n")
                  dna_orf.append(s">${id}_${dir}_${framestart}_${nrorf}_${start}-${end}\n${dna_sequence.substring(start, end)}\n")
                  end = -1
                  nrorf += 1
                  protein.clear()
                }
              } else {
                start = -1
                protein.clear()
              }
            }
          }
        }
      }
    }

    //Add to result
    result(0) = protein_orf.toString().trim()
    result(1) = dna_orf.toString().trim()
    return result
  }

  def dnaOrfGenerator (id: String, dnaseq: String, minlength: Int ): Tuple2[String, Array[Array[String]]] = {

    //Initiate variables:
    var orfStart: Int = -1
    var orfend: Int = -1
    var orfNR: Int = 0

    //There will be 6 frames:
    val all_orfs = new Array[Array[String]](6)

    //upper case
    val dna_sequence =  dnaseq.toUpperCase
    //reverse complement
    var reverse_comp = reverscomplement(dna_sequence)

    //rcsu result
    val forw_rcsu_result = new StringBuilder
    val rev_rcsu_result = new StringBuilder

    //Construct dna frames
    val frame1 = dna_sequence
    val frame2 = dna_sequence.substring(1, dnaseq.length-1)
    val frame3 = dna_sequence.substring(2, dnaseq.length-2)
    val revframe1 = reverse_comp
    val revframe2 = reverse_comp.substring(1, reverse_comp.length-1)
    val revframe3 = reverse_comp.substring(2, reverse_comp.length-2)

    //Construct and Add all ORFs in a list
    all_orfs(0) = dna2orf(id, frame1, 1, "forw", minlength)
    all_orfs(1) = dna2orf(id, frame2, 2, "forw", minlength)
    all_orfs(2) = dna2orf(id, frame3, 3, "forw", minlength)
    all_orfs(3) = dna2orf(id, revframe1, 1, "rev", minlength)
    all_orfs(4) = dna2orf(id, revframe2, 2, "rev", minlength)
    all_orfs(5) = dna2orf(id, revframe3, 3, "rev", minlength)

    //Construct result value
    val result: Tuple2[String, Array[Array[String]]] = (id, all_orfs)

    //and return
    return result
  }
}