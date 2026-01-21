package org.apache.flink.table.connector.source.partitioning;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.TransformExpression;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * Key-grouped partitioning implementation for table sources.
 *
 * <p>TODO Consider relaxing this constraint in a future version
 * Preconditions:
 * 1. keys are ordered by the partition columns defined in the table schema.
 * 2. the partition values are ordered by the values in Row, comparing the values from 1st to last.
 * for example:
 * if a table is partitioned by (dt, bucket(128, user_id))
 * then the partition keys = [dt, bucket(128, user_id)]. It cannot be [bucket(128, user_id), dt].
 * the partition values can be ("2023-10-01", 0), ("2023-10-01", 1), ("2023-10-02", 0), ...
 * it cannot be ("2023-10-01", 1), ("2023-10-01", 0), ("2023-10-02", 0), ...
 */
public class KeyGroupedPartitioning implements Partitioning {
    private final TransformExpression[] keys;
    private final int numPartitions;
    private final Row[] partitionValues;

    public KeyGroupedPartitioning(TransformExpression[] keys, Row[] partitionValues, int numPartitions) {
        this.keys = keys;
        this.numPartitions = numPartitions;
        this.partitionValues = partitionValues;
    }

    /**
     * Returns the partition transform expressions for this partitioning.
     */
    public TransformExpression[] keys() {
        return keys;
    }

    public Row[] getPartitionValues() {
        return partitionValues;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    /**
     * Checks if this partitioning is compatible with another KeyGroupedPartitioning.
     * conditions:
     * 1. numPartitions is the same
     * 2. keys length is the same and for each key,keys are compatible
     * 3. RowData length is the same. values are the same.
     *
     * @param other the other KeyGroupedPartitioning to check compatibility with
     * @return true if compatible, false otherwise
     */
    public boolean isCompatible(KeyGroupedPartitioning other) {
        if (other == null) {
            return false;
        }

        // 1. Check numPartitions is the same
        if (this.numPartitions != other.numPartitions) {
            return false;
        }

        // 2. Check keys length is the same and each key is compatible
        if (this.keys.length != other.keys.length) {
            return false;
        }

        for (int i = 0; i < this.keys.length; i++) {
            if (!this.keys[i].isCompatible(other.keys[i])) {
                return false;
            }
        }

        // 3. Check RowData length and values are the same
        if (this.partitionValues.length != other.partitionValues.length) {
            return false;
        }

        for (int i = 0; i < this.partitionValues.length; i++) {
            Row thisRow = this.partitionValues[i];
            Row otherRow = other.partitionValues[i];

            if (thisRow.getArity() != otherRow.getArity()) {
                return false;
            }

            for (int j = 0; j < thisRow.getArity(); j++) {
                // filed in row cannot be null
                Preconditions.checkArgument(thisRow.getField(j) != null);
                if (!thisRow.getField(j).equals(otherRow.getField(j))) {
                    return false;
                }
            }
        }

        return true;
    }
}
