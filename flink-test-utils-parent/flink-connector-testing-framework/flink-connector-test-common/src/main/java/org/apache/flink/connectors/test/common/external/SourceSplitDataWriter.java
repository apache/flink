package org.apache.flink.connectors.test.common.external;

import org.apache.flink.table.data.RowData;

import java.util.Collection;

/**
 * Source split data writer.
 *
 * @param <T>
 */
public interface SourceSplitDataWriter<T> extends AutoCloseable {
    void writeRecords(Collection<T> records);

    void writeRowDataRecords(Collection<RowData> records);
}
