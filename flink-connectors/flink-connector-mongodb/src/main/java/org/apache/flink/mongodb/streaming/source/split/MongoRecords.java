package org.apache.flink.mongodb.streaming.source.split;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import org.bson.Document;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public class MongoRecords implements RecordsWithSplitIds<Document> {

    private String splitId;

    private final Queue<Document> remaining = new LinkedList<>(); ;

    private final Set<String> finishedSplits;

    private boolean read = false;

    private MongoRecords(String splitId, List<Document> records, Set<String> finishedSplits) {
        this.splitId = splitId;
        this.finishedSplits = finishedSplits == null ? Collections.emptySet() : finishedSplits;
        if (records != null) {
            this.remaining.addAll(records);
        }
    }

    @Nullable
    @Override
    public String nextSplit() {
        if (read) {
            return null;
        } else {
            read = true;
            return splitId;
        }
    }

    @Nullable
    @Override
    public Document nextRecordFromSplit() {
        return remaining.poll();
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }

    @Override
    public void recycle() {
    }

    public static MongoRecords forRecords(String splitId, List<Document> results) {
        return new MongoRecords(splitId, results, Collections.emptySet());
    }

    public static MongoRecords finishedSplit(String splitId) {
        return new MongoRecords(null, null, Collections.singleton(splitId));
    }
}
