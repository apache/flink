package org.apache.flink.table.runtime.operators.join.temporal.utils;

import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.JoinCondition;

public class TemporalProcessTimeJoinHelper {
    private final boolean isLeftOuterJoin;
    private final JoinCondition joinCondition;
    private final JoinedRowData outRow;
    private final GenericRowData rightNullRow;
    private final TimestampedCollector<RowData> collector;

    public TemporalProcessTimeJoinHelper(
            boolean isLeftOuterJoin,
            JoinCondition joinCondition,
            JoinedRowData outRow,
            GenericRowData rightNullRow,
            TimestampedCollector<RowData> collector) {
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.joinCondition = joinCondition;
        this.outRow = outRow;
        this.rightNullRow = rightNullRow;
        this.collector = collector;
    }

    public void processElement1(RowData leftSideRow, RowData rightSideRow) throws Exception {
        if (rightSideRow == null) {
            if (isLeftOuterJoin) {
                collectJoinedRow(leftSideRow, rightNullRow);
            } else {
                return;
            }
        } else {
            if (joinCondition.apply(leftSideRow, rightSideRow)) {
                collectJoinedRow(leftSideRow, rightSideRow);
            } else {
                if (isLeftOuterJoin) {
                    collectJoinedRow(leftSideRow, rightNullRow);
                }
            }
        }
    }

    private void collectJoinedRow(RowData leftRow, RowData rightRow) {
        outRow.setRowKind(leftRow.getRowKind());
        outRow.replace(leftRow, rightRow);
        collector.collect(outRow);
    }
}
