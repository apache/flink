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

package org.apache.flink.table.test.program;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Interface for test bases that want to run lists of {@link TableTestProgram}s.
 *
 * <p>NOTE: See {@link TableTestProgram} for a full example.
 *
 * <p>Use {@link #supportedPrograms()} for assertions (usually in test bases), and {@link
 * #programs()} for program lists (usually in final tests).
 */
public interface TableTestProgramRunner {

    /**
     * List of {@link TableTestProgram}s that this runner should run.
     *
     * <p>Usually, this list should reference some test programs stored in static variables that can
     * be shared across runners.
     */
    List<TableTestProgram> programs();

    /**
     * Runners should call this method to get started.
     *
     * <p>Compared to {@link #programs()}, this method will perform some pre-checks.
     */
    default List<TableTestProgram> supportedPrograms() {
        final List<TableTestProgram> programs = programs();

        final List<String> ids = programs.stream().map(p -> p.id).collect(Collectors.toList());
        final List<String> duplicates =
                ids.stream()
                        .filter(id -> Collections.frequency(ids, id) > 1)
                        .distinct()
                        .collect(Collectors.toList());
        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException("Duplicate test program id found: " + duplicates);
        }

        final Set<TestStep.TestKind> setupSteps = supportedSetupSteps();
        final Set<TestStep.TestKind> runSteps = supportedRunSteps();

        programs.forEach(
                p -> {
                    p.setupSteps.stream()
                            .map(TestStep::getKind)
                            .filter(k -> !setupSteps.contains(k))
                            .findFirst()
                            .ifPresent(
                                    k -> {
                                        throw new UnsupportedOperationException(
                                                "Test runner does not support setup step: " + k);
                                    });
                    p.runSteps.stream()
                            .map(TestStep::getKind)
                            .filter(k -> !runSteps.contains(k))
                            .findFirst()
                            .ifPresent(
                                    k -> {
                                        throw new UnsupportedOperationException(
                                                "Test runner does not support run step: " + k);
                                    });
                });

        return programs;
    }

    /**
     * Lists setup steps that are supported by this runner.
     *
     * <p>E.g. some runners might not want to run generic {@link TestStep.TestKind#SQL} because they
     * want to enrich CREATE TABLE statements.
     *
     * <p>This also ensures that runners don't need to be updated when a new kind of step is added,
     * or steps get silently dropped.
     */
    EnumSet<TestStep.TestKind> supportedSetupSteps();

    /**
     * Lists run steps that are supported by this runner.
     *
     * <p>E.g. some runners might not want to run generic {@link TestStep.TestKind#SQL} because they
     * want to enrich CREATE TABLE statements.
     *
     * <p>This also ensures that runners don't need to be updated when a new kind of step is added,
     * or steps get silently dropped.
     */
    EnumSet<TestStep.TestKind> supportedRunSteps();
}
