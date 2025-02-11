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

package org.apache.flink.table.annotation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableAggregateFunction;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A hint that declares an intermediate result (i.e. state entry) that is managed by the framework
 * (i.e. Flink managed state).
 *
 * <p>State hints are primarily intended for {@link ProcessTableFunction}. A PTF supports multiple
 * state entries at the beginning of an eval()/onTimer() method (after an optional context
 * parameter).
 *
 * <p>Aggregating functions (i.e. {@link AggregateFunction} and {@link TableAggregateFunction})
 * support a single state entry at the beginning of an accumulate()/retract() method (i.e. the
 * accumulator).
 *
 * <p>Because state needs to be mutable for read and write access, only row or structured types
 * qualify as a data type for state entries. For example, {@code @StateHint(name = "count", type
 * = @DataTypeHint("ROW<count BIGINT>"))} is a state entry with the data type BIGINT named "count".
 *
 * <p>Note: A state entry is partitioned by a key and can not be accessed globally. The partitioning
 * (or a single partition in case of no partitioning) is defined by the corresponding function call.
 *
 * @see FunctionHint
 */
@PublicEvolving
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.PARAMETER})
public @interface StateHint {

    /**
     * The name of the state entry. It must be unique among other state entries.
     *
     * <p>This can be used to provide a descriptive name for the state entry. The name can be used
     * for referencing the entry during clean up.
     */
    String name() default "";

    /**
     * The data type hint for the state entry.
     *
     * <p>This can be used to provide additional information about the expected data type of the
     * argument. The {@link DataTypeHint} annotation can be used to specify the data type explicitly
     * or provide hints for the reflection-based extraction of the data type.
     */
    DataTypeHint type() default @DataTypeHint();

    /**
     * The time-to-live (TTL) duration that automatically cleans up the state entry.
     *
     * <p>It specifies a minimum time interval for how long idle state (i.e., state which was not
     * updated by a create or write operation) will be retained. State will never be cleared until
     * it was idle for less than the minimum time, and will be cleared at some time after it was
     * idle.
     *
     * <p>Use this for being able to efficiently manage an ever-growing state size or for complying
     * with data protection requirements.
     *
     * <p>The cleanup is based on processing time, which effectively corresponds to the wall clock
     * time as defined by {@link System#currentTimeMillis()}).
     *
     * <p>The provided string must use Flink's duration syntax (e.g., "3 days", "45 min", "3 hours",
     * "60 s"). If no unit is specified, the value is interpreted as milliseconds. The TTL setting
     * on a state entry has higher precedence than the global state TTL configuration for the entire
     * pipeline.
     *
     * @see org.apache.flink.util.TimeUtils#parseDuration(String)
     */
    String ttl() default "";
}
