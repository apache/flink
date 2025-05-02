package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.table.data.RowData;

/**
 * Interface for state view handling records under a specific map key within the main join key's
 * context.
 */
public interface MultiJoinStateView {
    /** Adds the record to the state view under the given map key. */
    void addRecord(RowData mapKey, RowData record) throws Exception;

    /** Retract the record from the state view under the given map key. */
    void retractRecord(RowData mapKey, RowData record) throws Exception;

    /** Gets all the records under the given map key. */
    Iterable<RowData> getRecords(RowData mapKey) throws Exception;

    /** Removes all state associated with the given map key. */
    void cleanup(RowData mapKey) throws Exception;
}
