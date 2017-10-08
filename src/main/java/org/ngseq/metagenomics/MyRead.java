package org.ngseq.metagenomics;

import java.io.Serializable;

/**
 * Created by ilamaa on 11/3/16.
 */


    public class MyRead implements Serializable {

    private String key;
    private long start;
    private long end;
    private long pos;
    private String sequence;
    private String quality;
    private String instrument;
    private Integer runNumber;
    private String flowcellId;
    private Integer lane;
    private Integer tile;
    private Integer xpos;
    private Integer ypos;
    private Integer read;
    private Boolean filterPassed;
    private Integer controlNumber;
    private String indexSequence;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public String getSequence() {
        return sequence;
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }

    public String getQuality() {
        return quality;
    }

    public void setQuality(String quality) {
        this.quality = quality;
    }

    public String getInstrument() {
        return instrument;
    }

    public void setInstrument(String instrument) {
        this.instrument = instrument;
    }

    public Integer getRunNumber() {
        return runNumber;
    }

    public void setRunNumber(Integer runNumber) {
        this.runNumber = runNumber;
    }

    public String getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(String flowcellId) {
        this.flowcellId = flowcellId;
    }

    public Integer getLane() {
        return lane;
    }

    public void setLane(Integer lane) {
        this.lane = lane;
    }

    public Integer getTile() {
        return tile;
    }

    public void setTile(Integer tile) {
        this.tile = tile;
    }

    public Integer getXpos() {
        return xpos;
    }

    public void setXpos(Integer xpos) {
        this.xpos = xpos;
    }

    public Integer getYpos() {
        return ypos;
    }

    public void setYpos(Integer ypos) {
        this.ypos = ypos;
    }

    public Integer getRead() {
        return read;
    }

    public void setRead(Integer read) {
        this.read = read;
    }

    public Boolean getFilterPassed() {
        return filterPassed;
    }

    public void setFilterPassed(Boolean filterPassed) {
        this.filterPassed = filterPassed;
    }

    public Integer getControlNumber() {
        return controlNumber;
    }

    public void setControlNumber(Integer controlNumber) {
        this.controlNumber = controlNumber;
    }

    public String getIndexSequence() {
        return indexSequence;
    }

    public void setIndexSequence(String indexSequence) {
        this.indexSequence = indexSequence;
    }
}

