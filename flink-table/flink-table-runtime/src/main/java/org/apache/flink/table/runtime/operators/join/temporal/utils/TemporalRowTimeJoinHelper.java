package org.apache.flink.table.runtime.operators.join.temporal.utils;

import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.JoinCondition;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class TemporalRowTimeJoinHelper {

    private final JoinCondition joinCondition;
    private final boolean isLeftOuterJoin;
    private final GenericRowData rightNullRow;
    private final JoinedRowData outRow;
    private final int leftTimeAttribute;
    private final int rightTimeAttribute;

    private TimestampedCollector<RowData> collector;

    public TemporalRowTimeJoinHelper(JoinCondition joinCondition,
                                     boolean isLeftOuterJoin,
                                     GenericRowData rightNullRow,
                                     TimestampedCollector<RowData> collector,
                                     JoinedRowData outRow,
                                     int leftTimeAttribute,
                                     int rightTimeAttribute
                                     ) {
        this.joinCondition = joinCondition;
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.rightNullRow = rightNullRow;
        this.collector = collector;
        this.outRow = outRow;
        this.leftTimeAttribute = leftTimeAttribute;
        this.rightTimeAttribute = rightTimeAttribute;
    }

    public void emitTriggeredLeftRecordsInOrder(Map<Long, RowData> orderedLeftRecords,
                                                List<RowData> rightRowsSorted) {
        // iterate the triggered left records in the ascending order of the sequence key, i.e. the
        // arrival order.
        orderedLeftRecords.forEach(
                (leftSeq, leftRow) -> {
                    long leftTime = getLeftTime(leftRow);
                    Optional<RowData> rightRow = latestRightRowToJoin(rightRowsSorted, leftTime);
                    if (rightRow.isPresent() && RowDataUtil.isAccumulateMsg(rightRow.get())) {
                        if (joinCondition.apply(leftRow, rightRow.get())) {
                            collectJoinedRow(leftRow, rightRow.get());
                        } else {
                            if (isLeftOuterJoin) {
                                collectJoinedRow(leftRow, rightNullRow);
                            }
                        }
                    } else {
                        if (isLeftOuterJoin) {
                            collectJoinedRow(leftRow, rightNullRow);
                        }
                    }
                });
        orderedLeftRecords.clear();
    }


    public void collectJoinedRow(RowData leftSideRow, RowData rightRow) {
        outRow.setRowKind(leftSideRow.getRowKind());
        outRow.replace(leftSideRow, rightRow);
        collector.collect(outRow);
    }



    public int firstIndexToKeep(long timerTimestamp, List<RowData> rightRowsSorted) {
        int firstIndexNewerThenTimer =
                indexOfFirstElementNewerThanTimer(timerTimestamp, rightRowsSorted);

        if (firstIndexNewerThenTimer < 0) {
            return rightRowsSorted.size() - 1;
        } else {
            return firstIndexNewerThenTimer - 1;
        }
    }

    public int indexOfFirstElementNewerThanTimer(long timerTimestamp, List<RowData> list) {
        ListIterator<RowData> iter = list.listIterator();
        while (iter.hasNext()) {
            if (getRightTime(iter.next()) > timerTimestamp) {
                return iter.previousIndex();
            }
        }
        return -1;
    }

    /**
     * Binary search {@code rightRowsSorted} to find the latest right row to join with {@code
     * leftTime}. Latest means a right row with largest time that is still smaller or equal to
     * {@code leftTime}. For example with: rightState = [1(+I), 4(+U), 7(+U), 9(-D), 12(I)],
     *
     * <p>If left time is 6, the valid period should be [4, 7), data 4(+U) should be joined.
     *
     * <p>If left time is 10, the valid period should be [9, 12), but data 9(-D) is a DELETE message
     * which means the correspond version has no data in period [9, 12), data 9(-D) should not be
     * correlated.
     *
     * @return found element or {@code Optional.empty} If such row was not found (either {@code
     *     rightRowsSorted} is empty or all {@code rightRowsSorted} are are newer).
     */
    public Optional<RowData> latestRightRowToJoin(List<RowData> rightRowsSorted, long leftTime) {
        return latestRightRowToJoin(rightRowsSorted, 0, rightRowsSorted.size() - 1, leftTime);
    }

    public Optional<RowData> latestRightRowToJoin(
            List<RowData> rightRowsSorted, int low, int high, long leftTime) {
        if (low > high) {
            // exact value not found, we are returning largest from the values smaller then leftTime
            if (low - 1 < 0) {
                return Optional.empty();
            } else {
                return Optional.of(rightRowsSorted.get(low - 1));
            }
        } else {
            int mid = (low + high) >>> 1;
            RowData midRow = rightRowsSorted.get(mid);
            long midTime = getRightTime(midRow);
            int cmp = Long.compare(midTime, leftTime);
            if (cmp < 0) {
                return latestRightRowToJoin(rightRowsSorted, mid + 1, high, leftTime);
            } else if (cmp > 0) {
                return latestRightRowToJoin(rightRowsSorted, low, mid - 1, leftTime);
            } else {
                return Optional.of(midRow);
            }
        }
    }

    public long getLeftTime(RowData leftRow) {
        return leftRow.getLong(leftTimeAttribute);
    }

    public long getRightTime(RowData rightRow) {
        return rightRow.getLong(rightTimeAttribute);
    }

}
