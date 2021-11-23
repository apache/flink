package org.apache.flink.mongodb.streaming.sink;

import org.bson.Document;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@NotThreadSafe
public class DocumentBulk{

    private List<Document> bufferedDocuments;

    private final long maxSize;

    private static final int BUFFER_INIT_SIZE = Integer.MAX_VALUE;

    public DocumentBulk(long maxSize) {
        this.maxSize = maxSize;
        bufferedDocuments = new ArrayList<>(1024);
    }

    public DocumentBulk() {
        this(BUFFER_INIT_SIZE);
    }

    public int add(Document document) {
        if (bufferedDocuments.size() == maxSize) {
            throw new IllegalStateException("DocumentBulk is already full");
        }
        bufferedDocuments.add(document);
        return bufferedDocuments.size();
    }

    public int size() {
        return bufferedDocuments.size();
    }

    public boolean isFull() {
        return bufferedDocuments.size() >= maxSize;
    }

    public List<Document> getDocuments() {
        return bufferedDocuments;
    }

    @Override
    public String toString() {
        return "DocumentBulk{" +
                "bufferedDocuments=" + bufferedDocuments +
                ", maxSize=" + maxSize +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentBulk)) {
            return false;
        }
        DocumentBulk bulk = (DocumentBulk) o;
        return maxSize == bulk.maxSize &&
                Objects.equals(bufferedDocuments, bulk.bufferedDocuments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bufferedDocuments, maxSize);
    }
}
