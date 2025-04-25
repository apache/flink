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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.extraction.TypeInferenceExtractor;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;

/**
 * Base class for a user-defined process table function (PTF).
 *
 * <p>PTFs are the most powerful function kind for Flink SQL and Table API. They enable implementing
 * user-defined operators that can be as feature-rich as built-in operations. PTFs can take
 * (partitioned) tables to produce a new table. They have access to Flink's managed state,
 * event-time and timer services, and underlying table changelogs.
 *
 * <p>A process table function (PTF) maps zero, one, or multiple tables to zero, one, or multiple
 * rows (or structured types). Scalar arguments are also supported. If the output record consists of
 * only one field, the wrapper can be omitted, and a scalar value can be emitted that will be
 * implicitly wrapped into a row by the runtime.
 *
 * <h1>Table Semantics and Virtual Processors</h1>
 *
 * <p>PTFs can produce a new table by consuming tables as arguments. For scalability, input tables
 * are distributed across so-called "virtual processors". A virtual processor, as defined by the SQL
 * standard, executes a PTF instance and has access only to a portion of the entire table. The
 * argument declaration decides about the size of the portion and co-location of data. Conceptually,
 * tables can be processed either "as row" (i.e. with row semantics) or "as set" (i.e. with set
 * semantics).
 *
 * <h2>Table Argument with Row Semantics</h2>
 *
 * <p>A PTF that takes a table with row semantics assumes that there is no correlation between rows
 * and each row can be processed independently. The framework is free in how to distribute rows
 * across virtual processors and each virtual processor has access only to the currently processed
 * row.
 *
 * <h2>Table Argument with Set Semantics</h2>
 *
 * <p>A PTF that takes a table with set semantics assumes that there is a correlation between rows.
 * When calling the function, the PARTITION BY clause defines the columns for correlation. The
 * framework ensures that all rows belonging to same set are co-located. A PTF instance is able to
 * access all rows belonging to the same set. In other words: The virtual processor is scoped by a
 * key context.
 *
 * <p>It is also possible not to provide a key ({@link ArgumentTrait#OPTIONAL_PARTITION_BY}), in
 * which case only one virtual processor handles the entire table, thereby losing scalability
 * benefits.
 *
 * <h1>Implementation</h1>
 *
 * <p>The behavior of a {@link ProcessTableFunction} can be defined by implementing a custom
 * evaluation method. The evaluation method must be declared publicly, not static, and named <code>
 * eval</code>. Overloading is not supported.
 *
 * <p>For storing a user-defined function in a catalog, the class must have a default constructor
 * and must be instantiable during runtime. Anonymous functions in Table API can only be persisted
 * if the function object is not stateful (i.e. containing only transient and static fields).
 *
 * <h2>Data Types</h2>
 *
 * <p>By default, input and output data types are automatically extracted using reflection. This
 * includes the generic argument {@code T} of the class for determining an output data type. Input
 * arguments are derived from the {@code eval()} method. If the reflective information is not
 * sufficient, it can be supported and enriched with {@link FunctionHint}, {@link ArgumentHint}, and
 * {@link DataTypeHint} annotations.
 *
 * <p>The following examples show how to specify data types:
 *
 * <pre>{@code
 * // Function that accepts two scalar INT arguments and emits them as an implicit ROW < INT >
 * class AdditionFunction extends ProcessTableFunction<Integer> {
 *   public void eval(Integer a, Integer b) {
 *     collect(a + b);
 *   }
 * }
 *
 * // Function that produces an explicit ROW < i INT, s STRING > from scalar arguments, the function hint helps in
 * // declaring the row's fields
 * @DataTypeHint("ROW< i INT, s STRING >")
 * class DuplicatorFunction extends ProcessTableFunction<Row> {
 *   public void eval(Integer i, String s) {
 *     collect(Row.of(i, s));
 *     collect(Row.of(i, s));
 *   }
 * }
 *
 * // Function that accepts a scalar DECIMAL(10, 4) and emits it as an explicit ROW < d DECIMAL(10, 4) >
 * @FunctionHint(output = @DataTypeHint("ROW< d DECIMAL(10, 4) >"))
 * class DuplicatorFunction extends ProcessTableFunction<Row> {
 *   public void eval(@DataTypeHint("DECIMAL(10, 4)") BigDecimal d) {
 *     collect(Row.of(d));
 *     collect(Row.of(d));
 *   }
 * }
 * }</pre>
 *
 * <h2>Arguments</h2>
 *
 * <p>The {@link ArgumentHint} annotation enables declaring the name, data type, and kind of each
 * argument (i.e. ArgumentTrait.SCALAR, ArgumentTrait.TABLE_AS_SET, or ArgumentTrait.TABLE_AS_ROW).
 * It allows specifying other traits for table arguments as well:
 *
 * <pre>{@code
 * // Function that has two arguments:
 * // "input_table" (a table with set semantics) and "threshold" (a scalar value)
 * class ThresholdFunction extends ProcessTableFunction<Integer> {
 *   public void eval(
 *       // For table arguments, a data type for Row is optional (leading to polymorphic behavior)
 *       @ArgumentHint(value = ArgumentTrait.TABLE_AS_SET, name = "input_table") Row t,
 *       // Scalar arguments require a data type either explicit or via reflection
 *       @ArgumentHint(value = ArgumentTrait.SCALAR, name = "threshold") Integer threshold) {
 *     int amount = t.getFieldAs("amount");
 *     if (amount >= threshold) {
 *       collect(amount);
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>Table arguments can declare a concrete data type (of either row or structured type) or accept
 * any type of row in a polymorphic fashion:
 *
 * <pre>{@code
 * // Function with explicit table argument type of row
 * class MyPTF extends ProcessTableFunction<String> {
 *   public void eval(Context ctx, @ArgumentHint(value = ArgumentTrait.TABLE_AS_SET, type = @DataTypeHint("ROW < s STRING >")) Row t) {
 *     TableSemantics semantics = ctx.tableSemanticsFor("t");
 *     // Always returns "ROW < s STRING >"
 *     semantics.dataType();
 *     ...
 *   }
 * }
 *
 * // Function with explicit table argument type of structured type "Customer"
 * class MyPTF extends ProcessTableFunction<String> {
 *   public void eval(Context ctx, @ArgumentHint(value = ArgumentTrait.TABLE_AS_SET) Customer c) {
 *     TableSemantics semantics = ctx.tableSemanticsFor("c");
 *     // Always returns structured type of "Customer"
 *     semantics.dataType();
 *     ...
 *   }
 * }
 *
 * // Function with polymorphic table argument
 * class MyPTF extends ProcessTableFunction<String> {
 *   public void eval(Context ctx, @ArgumentHint(value = ArgumentTrait.TABLE_AS_SET) Row t) {
 *     TableSemantics semantics = ctx.tableSemanticsFor("t");
 *     // Always returns "ROW" but content depends on the table that is passed into the call
 *     semantics.dataType();
 *     ...
 *   }
 * }
 * }</pre>
 *
 * <h2>Context</h2>
 *
 * <p>A {@link Context} can be added as a first argument to the eval() method for additional
 * information about the input tables and other services provided by the framework:
 *
 * <pre>{@code
 * // Function that accesses the Context for reading the PARTITION BY columns and
 * // excluding them when building a result string
 * class ConcatNonKeysFunction extends ProcessTableFunction<String> {
 *   public void eval(Context ctx, @ArgumentHint(ArgumentTrait.TABLE_AS_SET) Row inputTable) {
 *     TableSemantics semantics = ctx.tableSemanticsFor("inputTable");
 *     List<Integer> keys = Arrays.asList(semantics.partitionByColumns());
 *     return IntStream.range(0, inputTable.getArity())
 *       .filter(pos -> !keys.contains(pos))
 *       .mapToObj(inputTable::getField)
 *       .map(Object::toString)
 *       .collect(Collectors.joining(", "));
 *   }
 * }
 * }</pre>
 *
 * <h1>State</h1>
 *
 * <p>A PTF that takes set semantic tables can be stateful. Intermediate results can be buffered,
 * cached, aggregated, or simply stored for repeated access. A function can have one or more state
 * entries which are managed by the framework. Flink takes care of storing and restoring those
 * during failures or restarts (i.e. Flink managed state).
 *
 * <p>A state entry is partitioned by a key and cannot be accessed globally. The partitioning (or a
 * single partition in case of no partitioning) is defined by the corresponding function call. In
 * other words: Similar to how a virtual processor has access only to a portion of the entire table,
 * a PTF has access only to a portion of the entire state defined by the PARTITION BY clause. In
 * Flink, this concept is also known as keyed state.
 *
 * <p>State entries can be added as a mutable parameter to the eval() method. In order to
 * distinguish them from call arguments, they must be declared before any other argument, but after
 * an optional {@link Context} parameter. Furthermore, they must be annotated either via {@link
 * StateHint} or declared as part of {@link FunctionHint#state()}.
 *
 * <p>For read and write access, only row or structured types (i.e. POJOs with default constructor)
 * qualify as a data type. If no state is present, all fields are set to null (in case of a row
 * type) or fields are set to their default value (in case of a structured type). For state
 * efficiency, it is recommended to keep all fields nullable.
 *
 * <pre>{@code
 * // Function that counts and stores its intermediate result in the CountState object
 * // which will be persisted by Flink
 * class CountingFunction extends ProcessTableFunction<String> {
 *   public static class CountState {
 *     public long count = 0L;
 *   }
 *
 *   public void eval(@StateHint CountState memory, @ArgumentHint(TABLE_AS_SET) Row input) {
 *     memory.count++;
 *     collect("Seen rows: " + memory.count);
 *   }
 * }
 *
 * // Function that waits for a second event coming in
 * class CountingFunction extends ProcessTableFunction<String> {
 *   public static class SeenState {
 *     public String first;
 *   }
 *
 *   public void eval(@StateHint SeenState memory, @ArgumentHint(TABLE_AS_SET) Row input) {
 *     if (memory.first == null) {
 *       memory.first = input.toString();
 *     } else {
 *       collect("Event 1: " + memory.first + " and Event 2: " + input.toString());
 *     }
 *   }
 * }
 *
 * // Function that uses Row for state
 * class CountingFunction extends ProcessTableFunction<String> {
 *   public void eval(@StateHint(type = @DataTypeHint("ROW < count BIGINT >")) Row memory, @ArgumentHint(TABLE_AS_SET) Row input) {
 *     Long newCount = 1L;
 *     if (memory.getField("count") != null) {
 *       newCount += memory.getFieldAs("count");
 *     }
 *     memory.setField("count", newCount);
 *     collect("Seen rows: " + newCount);
 *   }
 * }
 * }</pre>
 *
 * <h2>Efficiency and Design Principles</h2>
 *
 * <p>A stateful function also means that data layout and data retention should be well thought
 * through. An ever-growing state can happen by an unlimited number of partitions (i.e. an open
 * keyspace) or even within a partition. Consider setting a {@link StateHint#ttl()} or call {@link
 * Context#clearAllState()} eventually:
 *
 * <pre>{@code
 * // Function that waits for a second event coming in BUT with better state efficiency
 * class CountingFunction extends ProcessTableFunction<String> {
 *   public static class SeenState {
 *     public String first;
 *   }
 *
 *   public void eval(Context ctx, @StateHint(ttl = "1 day") SeenState memory, @ArgumentHint(TABLE_AS_SET) Row input) {
 *     if (memory.first == null) {
 *       memory.first = input.toString();
 *     } else {
 *       collect("Event 1: " + memory.first + " and Event 2: " + input.toString());
 *       ctx.clearAllState();
 *     }
 *   }
 * }
 * }</pre>
 *
 * <h2>Large State</h2>
 *
 * <p>Flink's state backends provide different types of state to efficiently handle large state.
 *
 * <p>Currently, PTFs support three types of state:
 *
 * <ul>
 *   <li><b>Value state</b>: Represents a single value.
 *   <li><b>List state</b>: Represents a list of values, supporting operations like appending,
 *       removing, and iterating.
 *   <li><b>Map state</b>: Represents a map (key-value pair) for efficient lookups, modifications,
 *       and removal of individual entries.
 * </ul>
 *
 * <p>By default, state entries in a PTF are represented as value state. This means that every state
 * entry is fully read from the state backend when the evaluation method is called, and the value is
 * written back to the state backend once the evaluation method finishes.
 *
 * <p>To optimize state access and avoid unnecessary (de)serialization, state entries can be
 * declared as {@link ListView} or {@link MapView}. These provide direct views to the underlying
 * Flink state backend.
 *
 * <p>For example, when using a {@link MapView}, accessing a value via {@link MapView#get(Object)}
 * will only deserialize the value associated with the specified key. This allows for efficient
 * access to individual entries without needing to load the entire map. This approach is
 * particularly useful when the map does not fit entirely into memory.
 *
 * <p>State TTL is applied individually to each entry in a list or map, allowing for fine-grained
 * expiration control over state elements.
 *
 * <pre>{@code
 * // Function that uses a map view for storing a large map for an event history per user
 * class HistoryFunction extends ProcessTableFunction<String> {
 *   public void eval(@StateHint MapView<String, Integer> largeMemory, @ArgumentHint(TABLE_AS_SET) Row input) {
 *     String eventId = input.getFieldAs("eventId");
 *     Integer count = largeMemory.get(eventId);
 *     if (count == null) {
 *       largeMemory.put(eventId, 1);
 *     } else {
 *       if (count > 1000) {
 *         collect("Anomaly detected: " + eventId);
 *       }
 *       largeMemory.put(eventId, count + 1);
 *     }
 *   }
 * }
 * }</pre>
 *
 * <h1>Time and Timers</h1>
 *
 * <p>A PTF supports event time natively. Time-based services are available via {@link
 * Context#timeContext(Class)}.
 *
 * <h2>Time</h2>
 *
 * <p>Every PTF takes an optional {@code on_time} argument. The {@code on_time} argument in the
 * function call declares the time attribute column for which a watermark has been declared. When
 * processing a table's row, this timestamp can be accessed via {@link TimeContext#time()} and the
 * watermark via {@link TimeContext#currentWatermark()} respectively.
 *
 * <p>Specifying an {@code on_time} argument in the function call instructs the framework to return
 * a {@code rowtime} column in the function's output for subsequent time-based operations.
 *
 * <p>The {@link ArgumentTrait#REQUIRE_ON_TIME} makes the {@code on_time} argument mandatory if
 * necessary.
 *
 * <h2>Timers</h2>
 *
 * <p>A PTF that takes set semantic tables can support timers. Timers allow for continuing the
 * processing at a later point in time. This makes waiting, synchronization, or timeouts possible. A
 * timer fires for the registered time when the watermark progresses the logical clock.
 *
 * <p>Timers can be named ({@link TimeContext#registerOnTime(String, Object)}) or unnamed ({@link
 * TimeContext#registerOnTime(Object)}). The name of a timer can be useful for replacing or deleting
 * an existing timer, or for identifying multiple timers via {@link OnTimerContext#currentTimer()}
 * when they fire.
 *
 * <p>An {@code onTimer()} method must be declared next to the eval() method for reacting to timer
 * events. The signature of the onTimer() method must contain an optional {@link OnTimerContext}
 * followed by all state entries (as declared in the eval() method).
 *
 * <p>Flink takes care of storing and restoring timers during failures or restarts. Thus, timers are
 * a special kind of state. Similarly, timers are scoped to a virtual processor defined by the
 * PARTITION BY clause. A timer can only be registered and deleted in the current virtual processor.
 *
 * <pre>{@code
 * // Function that waits for a second event or timeouts after 60 seconds
 * class TimerFunction extends ProcessTableFunction<String> {
 *   public static class SeenState {
 *     public String seen = null;
 *   }
 *
 *   public void eval(Context ctx, @StateHint SeenState memory, @ArgumentHint( { TABLE_AS_SET, REQUIRE_ON_TIME } ) Row input) {
 *     TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
 *     if (memory.seen == null) {
 *       memory.seen = input.getField(0).toString();
 *       timeCtx.registerOnTimer("timeout", timeCtx.time().plusSeconds(60));
 *     } else {
 *       collect("Second event arrived for: " + memory.seen);
 *       ctx.clearAll();
 *     }
 *   }
 *
 *   public void onTimer(SeenState memory) {
 *     collect("Timeout for: " + memory.seen);
 *   }
 * }
 * }</pre>
 *
 * <h2>Efficiency and Design Principles</h2>
 *
 * <p>Registering too many timers might affect performance. An ever-growing timer state can happen
 * by an unlimited number of partitions (i.e. an open keyspace) or even within a partition. Thus,
 * reduce the number of registered timers to a minimum and consider cleaning up timers if they are
 * not needed anymore via {@link Context#clearAllTimers()} or {@link
 * TimeContext#clearTimer(String)}.
 *
 * @param <T> The type of the output row. Either an explicit composite type or an atomic type that
 *     is implicitly wrapped into a row consisting of one field.
 */
@PublicEvolving
public abstract class ProcessTableFunction<T> extends UserDefinedFunction {

    /** The code generated collector used to emit rows. */
    private transient Collector<T> collector;

    /** Internal use. Sets the current collector. */
    public final void setCollector(Collector<T> collector) {
        this.collector = collector;
    }

    /**
     * Emits an (implicit or explicit) output row.
     *
     * <p>If null is emitted as an explicit row, it will be skipped by the runtime. For implicit
     * rows, the row's field will be null.
     *
     * @param row the output row
     */
    protected final void collect(T row) {
        collector.collect(row);
    }

    @Override
    public final FunctionKind getKind() {
        return FunctionKind.PROCESS_TABLE;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInferenceExtractor.forProcessTableFunction(typeFactory, (Class) getClass());
    }

    /**
     * Context that can be added as a first argument to the eval() method for additional information
     * about the input tables and other services provided by the framework.
     */
    @PublicEvolving
    public interface Context {

        /**
         * Returns a specialized {@link TimeContext} to work with time and timers.
         *
         * <p>Time and timer timestamps can be represented as either {@link Instant}, {@link
         * LocalDateTime}, or {@link Long}. Time and timers are based on milliseconds since epoch
         * and do not take the local session timezone into consideration.
         *
         * <p>Note: A timer context is always scoped under the currently processed event (either the
         * current input row or a firing timer).
         *
         * @param conversionClass representation of time and timer timestamps
         * @return the context for time and timer services
         */
        <TimeType> TimeContext<TimeType> timeContext(Class<TimeType> conversionClass);

        /**
         * Returns additional information about the semantics of a table argument.
         *
         * @param argName name of the table argument; either reflectively extracted or manually
         *     defined via {@link ArgumentHint#name()}.
         */
        TableSemantics tableSemanticsFor(String argName);

        /**
         * Clears the given state entry within the virtual partition once the eval() method returns.
         *
         * <p>Semantically this is equal to setting all fields of the state entry to null shortly
         * before the eval() method returns.
         *
         * @param stateName name of the state entry; either reflectively extracted or manually
         *     defined via {@link StateHint#name()}.
         */
        void clearState(String stateName);

        /**
         * Clears all state entries within the virtual partition once the eval() method returns.
         *
         * <p>Semantically, this is equal to calling {@link #clearState(String)} on all state
         * entries.
         */
        void clearAllState();

        /** Clears all timers within the virtual partition. */
        void clearAllTimers();

        /** Clears the virtual partition including timers and state. */
        void clearAll();
    }

    /**
     * A context that gives access to Flink's concepts of time and timers.
     *
     * <p>An event can have an event-time timestamp assigned. The timestamp can be accessed using
     * the {@link #time()} method.
     *
     * <p>Timers allow for continuing the processing at a later point in time. This makes waiting,
     * synchronization, or timeouts possible. A timer fires for the registered time when the
     * watermark progresses the logical clock.
     *
     * <p>Flink takes care of storing and restoring timers during failures or restarts. Thus, timers
     * are a special kind of state. Similarly, timers are scoped to a virtual processor defined by
     * the PARTITION BY clause. A timer can only be registered and deleted in the current virtual
     * processor.
     *
     * @param <TimeType> conversion class of timestamps, see {@link Context#timeContext(Class)}
     */
    @PublicEvolving
    public interface TimeContext<TimeType> {

        /**
         * Returns the timestamp of the currently processed event.
         *
         * <p>An event can be either the row of a table or a firing timer:
         *
         * <h1>Row event timestamp</h1>
         *
         * <p>The timestamp of the row currently being processed within the {@code eval()} method.
         *
         * <p>Powered by the function call's {@code on_time} argument, this method will return the
         * content of the referenced time attribute column. Returns {@code null} if the {@code
         * on_time} argument doesn't reference a time attribute column in the currently processed
         * table.
         *
         * <h1>Timer event timestamp</h1>
         *
         * <p>The timestamp of the firing timer currently being processed within the {@code
         * onTimer()} method.
         *
         * @return the event-time timestamp, or {@code null} if no timestamp is present
         */
        TimeType time();

        /**
         * Returns the current event-time watermark.
         *
         * <p>Watermarks are generated in sources and sent through the topology for advancing the
         * logical clock in each Flink subtask. The current watermark of a Flink subtask is the
         * global minimum watermark of all inputs (i.e. across all parallel inputs and table
         * partitions).
         *
         * <p>This method returns the current watermark of the Flink subtask that evaluates the PTF.
         * Thus, the returned timestamp represents the entire Flink subtask, independent of the
         * currently processed partition. This behavior is similar to a call to {@code SELECT
         * CURRENT_WATERMARK(...)} in SQL.
         *
         * <p>If a watermark was not received from all inputs, the method returns {@code null}.
         *
         * <p>In case this method is called within the {@code onTimer()} method, the returned
         * watermark is the triggering watermark that currently fires the timer.
         *
         * @return the current watermark of the Flink subtask, or {@code null} if no common logical
         *     time could be determined from the inputs
         */
        TimeType currentWatermark();

        /**
         * Registers a timer under the given name.
         *
         * <p>The timer fires when the {@link #currentWatermark()} advances the logical clock of the
         * Flink subtask to a timestamp later or equal to the desired timestamp. In other words: A
         * timer only fires if a watermark was received from all inputs and the timestamp is smaller
         * or equal to the minimum of all received watermarks.
         *
         * <p>Timers can be named for distinguishing them in the {@code onTimer()} method.
         * Registering a timer under the same name twice will replace an existing timer.
         *
         * <p>Note: Because only PTFs taking set semantic tables support state, and timers are a
         * special kind of state, at least one {@link ArgumentTrait#TABLE_AS_SET} table argument
         * must be declared.
         *
         * @param name identifier of the timer
         * @param time timestamp when the timer should fire
         */
        void registerOnTime(String name, TimeType time);

        /**
         * Registers a timer.
         *
         * <p>The timer fires when the {@link #currentWatermark()} advances the logical clock of the
         * Flink subtask to a timestamp later or equal to the desired timestamp. In other words: A
         * timer only fires if a watermark was received from all inputs and the timestamp is smaller
         * or equal to the minimum of all received watermarks.
         *
         * <p>Only one timer can be registered for a given time.
         *
         * <p>Note: Because only PTFs taking set semantic tables support state, and timers are a
         * special kind of state, at least one {@link ArgumentTrait#TABLE_AS_SET} table argument
         * must be declared.
         *
         * @param time timestamp when the timer should fire
         */
        void registerOnTime(TimeType time);

        /**
         * Clears a timer that was previously registered under the given name.
         *
         * <p>The call is ignored if no timer can be found.
         *
         * @param name identifier of the timer
         */
        void clearTimer(String name);

        /**
         * Clears a timer that was previously registered for a given time.
         *
         * <p>The call is ignored if no timer can be found. Named timers cannot be deleted with this
         * method.
         *
         * @param time timestamp when the timer should have fired
         */
        void clearTimer(TimeType time);

        /** Deletes all timers within the virtual partition. */
        void clearAllTimers();
    }

    /** Special {@link Context} that is available when the {@code onTimer()} method is called. */
    @PublicEvolving
    public interface OnTimerContext extends Context {

        /**
         * Returns the name of the timer currently being fired, if the timer is a named timer (i.e.
         * registered using {@link TimeContext#registerOnTime(String, Object)}).
         *
         * <p>Note: The time of the firing timer is available via {@link
         * OnTimerContext#timeContext(Class)}.
         *
         * @return the timer's name, or {@code null} if the timer is unnamed (i.e. registered using
         *     {@link TimeContext#registerOnTime(Object)})
         */
        String currentTimer();
    }
}
