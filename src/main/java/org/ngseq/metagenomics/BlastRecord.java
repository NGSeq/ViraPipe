package org.ngseq.metagenomics;

import java.io.Serializable;

/**
 * Created by ilamaa on 11/3/16.
 */


    public class BlastRecord implements Serializable {

    //qseqid    sseqid     pident length mismatch gapopen qstart    qend sstart     send        evalue bitscore
    //k141_53	CM000261.1	91.429	35	    1	    1	    238	    270	21459480	21459446	0.001	47.3
    private String qseqid;
    private String sseqid;
    private Double pident;
    private Integer length;
    private Integer mismatch;
    private Integer gapopen;
    private Long qstart;
    private Long qend;
    private Long sstart;
    private Long send;
    private Double evalue;
    private Double bitscore;

    public String getQseqid() {
        return qseqid;
    }

    public void setQseqid(String qseqid) {
        this.qseqid = qseqid;
    }

    public String getSseqid() {
        return sseqid;
    }

    public void setSseqid(String sseqid) {
        this.sseqid = sseqid;
    }

    public Double getPident() {
        return pident;
    }

    public void setPident(Double pident) {
        this.pident = pident;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Integer getMismatch() {
        return mismatch;
    }

    public void setMismatch(Integer mismatch) {
        this.mismatch = mismatch;
    }

    public Integer getGapopen() {
        return gapopen;
    }

    public void setGapopen(Integer gapopen) {
        this.gapopen = gapopen;
    }

    public Long getQstart() {
        return qstart;
    }

    public void setQstart(Long qstart) {
        this.qstart = qstart;
    }

    public Long getQend() {
        return qend;
    }

    public void setQend(Long qend) {
        this.qend = qend;
    }

    public Long getSstart() {
        return sstart;
    }

    public void setSstart(Long sstart) {
        this.sstart = sstart;
    }

    public Long getSend() {
        return send;
    }

    public void setSend(Long send) {
        this.send = send;
    }

    public Double getEvalue() {
        return evalue;
    }

    public void setEvalue(Double evalue) {
        this.evalue = evalue;
    }

    public Double getBitscore() {
        return bitscore;
    }

    public void setBitscore(Double bitscore) {
        this.bitscore = bitscore;
    }
}

