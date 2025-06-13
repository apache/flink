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

package org.apache.flink.table.ml;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Enum representing different types of machine learning tasks. Each task type has a corresponding
 * name that can be used for identification.
 */
@Experimental
public enum TaskType {
    CLASSIFICATION("classification", true),
    CLUSTERING("clustering", false),
    EMBEDDING("embedding", true),
    REGRESSION("regression", true),
    TEXT_GENERATION("text_generation", true);

    private final String name;
    private final boolean supportsEvaluation;

    TaskType(String name, boolean supportsEvaluation) {
        this.name = name;
        this.supportsEvaluation = supportsEvaluation;
    }

    public String getName() {
        return name;
    }

    public boolean supportsEvaluation() {
        return supportsEvaluation;
    }

    public static TaskType fromName(String name) {
        return Arrays.stream(values())
                .filter(taskType -> taskType.name.equals(name))
                .findFirst()
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "Invalid task type: '"
                                                + name
                                                + "'. Supported task types are: "
                                                + Arrays.stream(TaskType.values())
                                                        .map(TaskType::getName)
                                                        .collect(Collectors.toList())
                                                + "."));
    }

    public static boolean isValidTaskType(String name) {
        return Arrays.stream(values()).anyMatch(taskType -> taskType.name.equals(name));
    }

    public static Optional<RuntimeException> throwOrReturnInvalidTaskType(
            String task, boolean throwException) {
        if (!isValidTaskType(task)) {
            ValidationException exception =
                    new ValidationException(
                            "Invalid task type: '"
                                    + task
                                    + "'. Supported task types are: "
                                    + Arrays.stream(TaskType.values())
                                            .map(TaskType::getName)
                                            .collect(Collectors.toList())
                                    + ".");
            if (throwException) {
                throw exception;
            } else {
                return Optional.of(exception);
            }
        }
        return Optional.empty();
    }
}
