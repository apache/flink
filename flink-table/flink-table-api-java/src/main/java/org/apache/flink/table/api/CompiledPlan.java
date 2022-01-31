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

package org.apache.flink.table.api;

import org.apache.flink.annotation.Experimental;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This interface represents a compiled plan that can be executed using {@link
 * TableEnvironment#executePlan(CompiledPlan)}. A plan can be compiled starting from a SQL query
 * using {@link TableEnvironment#compilePlanSql(String)} and can be loaded back from a file or a
 * string using {@link TableEnvironment#loadPlan(PlanReference)}. A plan can be persisted using
 * {@link #writeToFile(Path, boolean)} or by manually extracting the JSON representation with {@link
 * #asJsonString()}.
 */
@Experimental
public interface CompiledPlan {

    // --- Writer methods

    /** Convert the plan to a JSON string representation. */
    String asJsonString();

    /** @see #writeToFile(File) */
    default void writeToFile(String path) throws IOException {
        writeToFile(Paths.get(path));
    }

    /** @see #writeToFile(File, boolean) */
    default void writeToFile(String path, boolean ignoreIfExists) throws IOException {
        writeToFile(Paths.get(path), ignoreIfExists);
    }

    /** @see #writeToFile(File) */
    default void writeToFile(Path path) throws IOException {
        writeToFile(path.toFile());
    }

    /** @see #writeToFile(File, boolean) */
    default void writeToFile(Path path, boolean ignoreIfExists)
            throws IOException, UnsupportedOperationException {
        writeToFile(path.toFile(), ignoreIfExists);
    }

    /**
     * Write this plan to a file using the JSON representation. This will not overwrite the file if
     * it's already existing.
     */
    default void writeToFile(File file) throws IOException {
        writeToFile(file, true);
    }

    /** Write this plan to a file using the JSON representation. */
    void writeToFile(File file, boolean ignoreIfExists)
            throws IOException, UnsupportedOperationException;

    // --- Accessors

    /** Get the flink version used to compile the plan. */
    String getFlinkVersion();
}
