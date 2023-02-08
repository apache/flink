package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.table.data.RowData;

public interface JoinBatchProcessor {
    void process(RowData record) throws Exception;
}
