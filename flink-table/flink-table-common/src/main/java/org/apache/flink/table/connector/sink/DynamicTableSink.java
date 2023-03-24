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

package org.apache.flink.table.connector.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/**
 * Sink of a dynamic table to an external storage system.
 *
 * <p>Dynamic tables are the core concept of Flink's Table & SQL API for processing both bounded and
 * unbounded data in a unified fashion. By definition, a dynamic table can change over time.
 *
 * <p>When writing a dynamic table, the content can always be considered as a changelog (finite or
 * infinite) for which all changes are written out continuously until the changelog is exhausted.
 * The given {@link ChangelogMode} indicates the set of changes that the sink accepts during
 * runtime.
 *
 * <p>For regular batch scenarios, the sink can solely accept insert-only rows and write out bounded
 * streams.
 *
 * <p>For regular streaming scenarios, the sink can solely accept insert-only rows and can write out
 * unbounded streams.
 *
 * <p>For change data capture (CDC) scenarios, the sink can write out bounded or unbounded streams
 * with insert, update, and delete rows. See also {@link RowKind}.
 *
 * <p>Instances of {@link DynamicTableSink} can be seen as factories that eventually produce
 * concrete runtime implementation for writing the actual data.
 *
 * <p>Depending on the optionally declared abilities, the planner might apply changes to an instance
 * and thus mutate the produced runtime implementation.
 *
 * <p>A {@link DynamicTableSink} can implement the following abilities:
 *
 * <ul>
 *   <li>{@link SupportsPartitioning}
 *   <li>{@link SupportsOverwrite}
 *   <li>{@link SupportsWritingMetadata}
 * </ul>
 *
 * <p>In the last step, the planner will call {@link #getSinkRuntimeProvider(Context)} for obtaining
 * a provider of runtime implementation.
 */
@PublicEvolving
public interface DynamicTableSink {

    /**
     * Returns the set of changes that the sink accepts during runtime.
     *
     * <p>The planner can make suggestions but the sink has the final decision what it requires. If
     * the planner does not support this mode, it will throw an error. For example, the sink can
     * return that it only supports {@link ChangelogMode#insertOnly()}.
     *
     * @param requestedMode expected set of changes by the current plan
     */
    ChangelogMode getChangelogMode(ChangelogMode requestedMode);

    /**
     * Returns a provider of runtime implementation for writing the data.
     *
     * <p>There might exist different interfaces for runtime implementation which is why {@link
     * SinkRuntimeProvider} serves as the base interface. Concrete {@link SinkRuntimeProvider}
     * interfaces might be located in other Flink modules.
     *
     * <p>Independent of the provider interface, the table runtime expects that a sink
     * implementation accepts internal data structures (see {@link RowData} for more information).
     *
     * <p>The given {@link Context} offers utilities by the planner for creating runtime
     * implementation with minimal dependencies to internal data structures.
     *
     * <p>{@link SinkProvider} is the recommended core interface. {@code SinkFunctionProvider} in
     * {@code flink-table-api-java-bridge} and {@link OutputFormatProvider} are available for
     * backwards compatibility.
     *
     * @see SinkProvider
     */
    SinkRuntimeProvider getSinkRuntimeProvider(Context context);

    /**
     * Creates a copy of this instance during planning. The copy should be a deep copy of all
     * mutable members.
     */
    DynamicTableSink copy();

    /** Returns a string that summarizes this sink for printing to a console or log. */
    String asSummaryString();

    // --------------------------------------------------------------------------------------------
    // Helper interfaces
    // --------------------------------------------------------------------------------------------

    /**
     * Context for creating runtime implementation via a {@link SinkRuntimeProvider}.
     *
     * <p>It offers utilities by the planner for creating runtime implementation with minimal
     * dependencies to internal data structures.
     *
     * <p>Methods should be called in {@link #getSinkRuntimeProvider(Context)}. The returned
     * instances are {@link Serializable} and can be directly passed into the runtime implementation
     * class.
     */
    @PublicEvolving
    interface Context {

        /**
         * Returns whether a runtime implementation can expect a finite number of rows.
         *
         * <p>This information might be derived from the session's execution mode and/or kind of
         * query.
         */
        boolean isBounded();

        /**
         * Creates type information describing the internal data structures of the given {@link
         * DataType}.
         *
         * @see ResolvedSchema#toPhysicalRowDataType()
         */
        <T> TypeInformation<T> createTypeInformation(DataType consumedDataType);

        /**
         * Creates type information describing the internal data structures of the given {@link
         * LogicalType}.
         */
        <T> TypeInformation<T> createTypeInformation(LogicalType consumedLogicalType);

        /**
         * Creates a converter for mapping between Flink's internal data structures and objects
         * specified by the given {@link DataType} that can be passed into a runtime implementation.
         *
         * <p>For example, {@link RowData} and its fields can be converted into a {@link Row}, or
         * the internal representation for structured types can be converted back into the original
         * (possibly nested) POJO.
         *
         * @see LogicalType#supportsOutputConversion(Class)
         */
        DataStructureConverter createDataStructureConverter(DataType consumedDataType);

        /**
         * Returns an {@link Optional} array of column index paths related to user specified target
         * column list or {@link Optional#empty()} when not specified. The array indices are 0-based
         * and support composite columns within (possibly nested) structures.
         *
         * <p>This information comes from the column list of the DML clause, e.g., for a sink table
         * t1 which schema is: {@code a STRING, b ROW < b1 INT, b2 STRING>, c BIGINT}
         *
         * <ul>
         *   <li>insert: 'insert into t1(a, b.b2) ...', the column list will be 'a, b.b2', and will
         *       return {@code [[0], [1, 1]]}. The statement 'insert into t1 select ...' without
         *       specifying a column list will return {@link Optional#empty()}.
         *   <li>update: 'update t1 set a=1, b.b1=2 where ...', the column list will be 'a, b.b1',
         *       and will return {@code [[0], [1, 0]]}.
         * </ul>
         *
         * <p>Note: will always return empty for the delete statement because it has no column list.
         */
        Optional<int[][]> getTargetColumns();
    }

    /**
     * Converter for mapping between Flink's internal data structures and objects specified by the
     * given {@link DataType} that can be passed into a runtime implementation.
     *
     * <p>For example, {@link RowData} and its fields can be converted into a {@link Row}, or the
     * internal representation for structured types can be converted back into the original
     * (possibly nested) POJO.
     *
     * @see LogicalType#supportsOutputConversion(Class)
     */
    @PublicEvolving
    interface DataStructureConverter extends RuntimeConverter {

        /** Converts the given internal structure into an external object. */
        @Nullable
        Object toExternal(@Nullable Object internalStructure);
    }

    /**
     * Provides actual runtime implementation for writing the data.
     *
     * <p>There might exist different interfaces for runtime implementation which is why {@link
     * SinkRuntimeProvider} serves as the base interface. Concrete {@link SinkRuntimeProvider}
     * interfaces might be located in other Flink modules.
     *
     * <p>{@link SinkProvider} is the recommended core interface. {@code SinkFunctionProvider} in
     * {@code flink-table-api-java-bridge} and {@link OutputFormatProvider} are available for
     * backwards compatibility.
     *
     * @see SinkProvider
     */
    @PublicEvolving
    interface SinkRuntimeProvider {
        // marker interface
    }
}
