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
import org.apache.flink.table.api.config.TableConfigOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Represents a static, executable entity that has been compiled from a Table & SQL API pipeline
 * definition. It encodes operators, expressions, functions, data types, and table connectors.
 *
 * <p>Every new Flink version might introduce improved optimizer rules, more efficient operators,
 * and other changes that impact the behavior of previously defined pipelines. In order to ensure
 * backwards compatibility and enable stateful streaming job upgrades, compiled plans can be
 * persisted and reloaded across Flink versions. See the website documentation for more information
 * about provided guarantees during stateful pipeline upgrades.
 *
 * <p>A plan can be compiled from a SQL query using {@link TableEnvironment#compilePlanSql(String)}.
 * It can be persisted using {@link #writeToFile(Path, boolean)} or by manually extracting the JSON
 * representation with {@link #asJsonString()}. A plan can be loaded back from a file or a string
 * using {@link TableEnvironment#loadPlan(PlanReference)}. Instances can be executed using {@link
 * TableEnvironment#executePlan(CompiledPlan)}.
 *
 * <p>Depending on the configuration, permanent catalog metadata (such as information about tables
 * and functions) will be persisted in the plan as well. Anonymous/inline objects will be persisted
 * if possible or fail the compilation otherwise. Temporary objects are never part of a plan and
 * need to be present during a restore.
 *
 * <p>Note: Plan restores assume a stable session context. Configuration, loaded modules and
 * catalogs, and temporary objects must not change. Schema evolution and changes of function
 * signatures are not supported.
 *
 * @see TableConfigOptions#PLAN_COMPILE_CATALOG_OBJECTS
 * @see TableConfigOptions#PLAN_RESTORE_CATALOG_OBJECTS
 * @see PlanReference
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
     * Writes this plan to a file using the JSON representation. This will not overwrite the file if
     * it's already existing.
     */
    default void writeToFile(File file) throws IOException {
        writeToFile(file, true);
    }

    /** Writes this plan to a file using the JSON representation. */
    void writeToFile(File file, boolean ignoreIfExists)
            throws IOException, UnsupportedOperationException;

    // --- Accessors

    /** Returns the Flink version used to compile the plan. */
    String getFlinkVersion();
}
