package org.apache.flink.table.runtime.operators.rank.utils;

import org.apache.flink.table.data.RowData;

public class RankRow {
    public final RowData row;
    public int innerRank;
    public boolean dirty;

    public RankRow(RowData row, int innerRank, boolean dirty) {
        this.row = row;
        this.innerRank = innerRank;
        this.dirty = dirty;
    }
}
