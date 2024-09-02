package org.apache.flink.table.utils;

import org.apache.flink.table.api.dataview.ListView;

import java.util.Objects;

/**
 * flink array accumulator,as T[]
 *
 * @author leochu
 * @date 2024/8/14
 */
public class ArrayAccumulator<T> {

    public ListView<T> values = new ListView<>();

    @Override
    public int hashCode() {
        return Objects.hashCode(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ArrayAccumulator<?> other = (ArrayAccumulator<?>) obj;
        return Objects.equals(values, other.values);
    }
}
