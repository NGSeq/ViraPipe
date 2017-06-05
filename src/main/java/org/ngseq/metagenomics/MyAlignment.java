package org.ngseq.metagenomics;

import java.io.Serializable;

/**
 * Created by root on 11/3/16.
 */


    public class MyAlignment implements Serializable {

    private boolean duplicateRead;
    private String readName;
        private Integer start;
        private String referenceName;
        private Integer length;
        private String bases;
        private String cigar;
        private boolean readUnmapped;

    public MyAlignment(){
        
    }

    public MyAlignment(String readName, Integer start, String referenceName, Integer length, String bases, String cigar, boolean readUnmapped) {
        this.readName = readName;
        this.start = start;
        this.referenceName = referenceName;
        this.length = length;
        this.bases = bases;
        this.cigar = cigar;
        this.readUnmapped = readUnmapped;
    }

    public MyAlignment(String readName, Integer start, String referenceName, Integer length, String bases, String cigar, boolean readUnmapped, boolean duplicateRead) {
        this.readName = readName;
        this.start = start;
        this.referenceName = referenceName;
        this.length = length;
        this.bases = bases;
        this.cigar = cigar;
        this.readUnmapped = readUnmapped;
        this.duplicateRead = duplicateRead;
    }

    public boolean isReadUnmapped() {
        return readUnmapped;
    }

    public String getReadName() {
        return readName;
    }

    public String getReferenceName() {
        return referenceName;
    }


        public Integer getStart() {
            return start;
        }

        public void setStart(Integer start) {
            this.start = start;
        }

        public Integer getLength() {
            return length;
        }

        public void setLength(Integer length) {
            this.length = length;
        }

        public String getBases() {
            return bases;
        }

        public void setBases(String bases) {
            this.bases = bases;
        }

        public String getCigar() {
            return cigar;
        }

        public void setCigar(String cigar) {
            this.cigar = cigar;
        }

    public boolean isDuplicateRead() {
        return duplicateRead;
    }

    public void setDuplicateRead(boolean duplicateRead) {
        this.duplicateRead = duplicateRead;
    }

    public void setReadName(String readName) {
        this.readName = readName;
    }

    public void setReferenceName(String referenceName) {
        this.referenceName = referenceName;
    }

    public void setReadUnmapped(boolean readUnmapped) {
        this.readUnmapped = readUnmapped;
    }
}

