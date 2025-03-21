package org.apache.flink.table.runtime.operators.sort.utils;

import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.RecordComparator;

import java.util.List;

public abstract class ProcTimeSortHelper {

    private List<RowData> sortBuffer;
    private RecordComparator comparator;
    protected TimestampedCollector<RowData> collector;

    public ProcTimeSortHelper(
            List<RowData> sortBuffer,
            RecordComparator comparator,
            TimestampedCollector<RowData> collector) {
        this.sortBuffer = sortBuffer;
        this.comparator = comparator;
        this.collector = collector;
    }

    public void emitData(Iterable<RowData> inputs) {
        // sort the rows
        sortBuffer.sort(comparator);

        // Emit the rows in order
        sortBuffer.forEach((RowData row) -> collector.collect(row));

        clearDataState();
    }

    protected abstract void clearDataState();
}
