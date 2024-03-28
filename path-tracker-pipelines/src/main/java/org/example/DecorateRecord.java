package org.example;

public class DecorateRecord<T> {
    private long seqNum;
    private String pathInfo;

    private T value;

    public DecorateRecord(long SeqNum, String pathInfo, T value) {
        this.seqNum = SeqNum;
        this.pathInfo = pathInfo;
        this.value = value;
    }

    public void setSeqNum(long seqNum) {
        this.seqNum = seqNum;
    }

    public long getSeqNum() {
        return seqNum;
    }

    // TODO: use xor to compress path information?
    public void addAndSetPathInfo(String vertexID) {
        this.pathInfo = String.format("%s-%s", this.pathInfo, vertexID);
    }

    public String addPathInfo(String vertexID) {
        return String.format("%s-%s", this.pathInfo, vertexID);
    }

    public void setPathInfo(String pathInfo) {
        this.pathInfo = pathInfo;
    }

    public String getPathInfo() {
        return this.pathInfo;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format(
                "{SeqNumber=%d, PathInfo=(%s), Value=%s}",
                this.getSeqNum(),
                this.getPathInfo(),
                this.getValue());
    }
}
