/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.dataview;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.Objects;

/**
 * A {@link DataView} that provides single-value functionality in state entries.
 *
 * <p>A {@link ValueView} can be backed by a Java heap object or can leverage Flink's state backends
 * depending on the context. In many unbounded data scenarios, the {@link ValueView} delegates all
 * calls to a {@link ValueState} instead of the heap object.
 *
 * <p>For process table functions, the view can be used as a top-level state entry. Data views in
 * PTFs are always backed by state.
 *
 * <p>In contrast to eager value state (i.e. a top-level {@code Row} or POJO state entry which
 * follows a Read-Modify-Write cycle on every call), a {@link ValueView} only deserializes the value
 * when {@link #getValue()} is accessed and only serializes it when {@link #setValue(Object)} or
 * {@link #clear()} is called. Thus, it should be preferred for state that is accessed
 * conditionally.
 *
 * <p>Note: {@link ValueView} is not supported in accumulators of aggregating functions.
 *
 * <p>Note: A {@link ValueView} does not support null values. Storing a null value via {@link
 * #setValue(Object)} is equal to calling {@link #clear()}.
 *
 * <p>The {@link DataType} of the view's value is reflectively extracted from the state definition.
 * This includes the generic argument {@code T} of this class. If reflective extraction is not
 * successful, it is possible to use a {@link DataTypeHint} on top of the state entry. In contrast
 * to {@link ListView} and {@link MapView}, the hint defines the value type {@code T} directly.
 *
 * <p>The following example shows how to specify a {@link ProcessTableFunction} with a {@link
 * ValueView}:
 *
 * <pre>{@code
 * public class CountingFunction extends ProcessTableFunction<String> {
 *   public void eval(@StateHint ValueView<Integer> count, @ArgumentHint(SET_SEMANTIC_TABLE) Row input) {
 *     Integer c = count.getValue();
 *     if (c == null) {
 *       c = 0;
 *     }
 *     count.setValue(c + 1);
 *     collect("Count: " + (c + 1));
 *   }
 * }
 * }</pre>
 *
 * <p>If reflective extraction is not possible - for example when a {@link Row} is used - a {@link
 * DataTypeHint} can define the value type directly:
 *
 * <pre>{@code
 * public class CountingFunction extends ProcessTableFunction<String> {
 *   public void eval(
 *       @StateHint(type = @DataTypeHint("ROW<count INT>")) ValueView<Row> count,
 *       @ArgumentHint(SET_SEMANTIC_TABLE) Row input) {
 *     Row v = count.getValue();
 *     Integer c = (v == null) ? 0 : v.getFieldAs("count");
 *     count.setValue(Row.of(c + 1));
 *     collect("Count: " + (c + 1));
 *   }
 * }
 * }</pre>
 *
 * @param <T> value type
 */
@PublicEvolving
public class ValueView<T> implements DataView {

    private T value;

    /**
     * Creates a value view.
     *
     * <p>The {@link DataType} of the contained value is reflectively extracted.
     */
    public ValueView() {
        // default constructor
    }

    /** Returns the current value of this view or {@code null} if no value has been set. */
    public T getValue() {
        return value;
    }

    /**
     * Updates the view to the given value.
     *
     * <p>Setting a null value is equal to calling {@link #clear()}.
     */
    public void setValue(T value) {
        this.value = value;
    }

    /** Returns {@code true} if this view holds no value (i.e. {@link #getValue()} is null). */
    public boolean isEmpty() {
        return getValue() == null;
    }

    /** Removes the value from this view. */
    @Override
    public void clear() {
        value = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ValueView)) {
            return false;
        }
        final ValueView<?> valueView = (ValueView<?>) o;
        return Objects.equals(getValue(), valueView.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getValue());
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    /** Utility method for creating a {@link DataType} of {@link ValueView} explicitly. */
    public static DataType newValueViewDataType(DataType valueDataType) {
        return DataTypes.STRUCTURED(ValueView.class, DataTypes.FIELD("value", valueDataType));
    }
}
