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

package org.apache.flink.table.delegation;

import org.apache.flink.FlinkVersion;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.TableEnvironment;

import java.io.File;
import java.util.List;

/**
 * Internal interface wrapping a plan. This is used in order to propagate the plan back and forth
 * the {@link Planner} interface. The {@link TableEnvironment} wraps it in an implementation of
 * {@link CompiledPlan}, to provide the fluent user-friendly interface.
 */
@Internal
public interface InternalPlan {

    /** @see CompiledPlan#asJsonString() */
    String asJsonString();

    /**
     * Note that {@code ignoreIfExists} has precedence over {@code failIfExists}.
     *
     * @see CompiledPlan#writeToFile(File, boolean)
     */
    void writeToFile(File file, boolean ignoreIfExists, boolean failIfExists);

    /** @see CompiledPlan#getFlinkVersion() */
    FlinkVersion getFlinkVersion();

    /** This returns an ordered list of sink identifiers, if any. */
    List<String> getSinkIdentifiers();
}
