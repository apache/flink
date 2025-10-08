package org.apache.flink.table.runtime.sequencedmultisetstate.linked;

import org.apache.flink.util.Preconditions;

import java.util.Objects;

/** Stores first and las SQN for a record. */
class MetaSqnInfo {
    public final long highSqn;
    public final long size;

    public MetaSqnInfo(long highSqn, long size) {
        Preconditions.checkArgument(size >= 0);
        this.highSqn = highSqn;
        this.size = size;
    }

    public static MetaSqnInfo of(long first, long last) {
        return new MetaSqnInfo(first, last);
    }

    @Override
    public String toString() {
        return "MetaSqnInfo{" + "firstSqn=" + highSqn + ", lastSqn=" + size + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MetaSqnInfo)) {
            return false;
        }
        MetaSqnInfo that = (MetaSqnInfo) o;
        return highSqn == that.highSqn && size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(highSqn, size);
    }
}
