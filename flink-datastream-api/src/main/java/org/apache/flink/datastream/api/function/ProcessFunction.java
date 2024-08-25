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

package org.apache.flink.datastream.api.function;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.datastream.watermark.DeclarableWatermark;

import java.util.Collections;
import java.util.Set;

/** Base class for all user defined process functions. */
@Experimental
public interface ProcessFunction extends Function, DeclarableWatermark {
    /**
     * Initialization method for the function. It is called before the actual working methods (like
     * processRecord) and thus suitable for one time setup work.
     *
     * <p>By default, this method does nothing.
     *
     * @throws Exception Implementations may forward exceptions, which are caught by the runtime.
     *     When the runtime catches an exception, it aborts the task and lets the fail-over logic
     *     decide whether to retry the task execution.
     */
    default void open() throws Exception {}

    /**
     * Explicitly declares states upfront. Each specific state must be declared in this method
     * before it can be used.
     *
     * @return all declared states used by this process function.
     */
    default Set<StateDeclaration> usesStates() {
        return Collections.emptySet();
    }

    /**
     * Tear-down method for the user code. It is called after the last call to the main working
     * methods (e.g. processRecord).
     *
     * <p>This method can be used for clean up work.
     *
     * @throws Exception Implementations may forward exceptions, which are caught by the runtime.
     *     When the runtime catches an exception, it aborts the task and lets the fail-over logic
     *     decide whether to retry the task execution.
     */
    default void close() throws Exception {}
}
