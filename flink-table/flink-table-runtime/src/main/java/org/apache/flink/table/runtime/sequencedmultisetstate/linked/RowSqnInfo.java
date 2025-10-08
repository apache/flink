package org.apache.flink.table.runtime.sequencedmultisetstate.linked;

import org.apache.flink.util.Preconditions;

import java.util.Objects;

/** Stores first and las SQN for a record. */
class RowSqnInfo {
    public final long firstSqn;
    public final long lastSqn;

    public RowSqnInfo(long firstSqn, long lastSqn) {
        Preconditions.checkArgument(firstSqn <= lastSqn);
        this.firstSqn = firstSqn;
        this.lastSqn = lastSqn;
    }

    public static RowSqnInfo ofSingle(long sqn) {
        return of(sqn, sqn);
    }

    public static RowSqnInfo of(long first, long last) {
        return new RowSqnInfo(first, last);
    }

    @Override
    public String toString() {
        return "RowSqnInfo{" + "firstSqn=" + firstSqn + ", lastSqn=" + lastSqn + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RowSqnInfo)) {
            return false;
        }
        RowSqnInfo that = (RowSqnInfo) o;
        return firstSqn == that.firstSqn && lastSqn == that.lastSqn;
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstSqn, lastSqn);
    }

    public RowSqnInfo withFirstSqn(long firstSqn) {
        return RowSqnInfo.of(firstSqn, lastSqn);
    }
}
