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

import javax.annotation.Nullable;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Pointer to a persisted plan. You can load the content of this reference into a {@link
 * CompiledPlan} using {@link TableEnvironment#loadPlan(PlanReference)} or you can directly execute
 * it with {@link TableEnvironment#executePlan(PlanReference)}.
 */
@Experimental
public final class PlanReference {

    private final @Nullable File file;
    private final @Nullable String content;

    private PlanReference(@Nullable File file, @Nullable String content) {
        this.file = file;
        this.content = content;
    }

    /** @see #fromFile(File) */
    public static PlanReference fromFile(String path) {
        return fromFile(Paths.get(path).toFile());
    }

    /** @see #fromFile(File) */
    public static PlanReference fromFile(Path path) {
        return fromFile(path.toFile());
    }

    /** Create a reference starting from a file path. */
    public static PlanReference fromFile(File file) {
        return new PlanReference(file, null);
    }

    /** Create a reference starting from a JSON string. */
    public static PlanReference fromJsonString(String jsonString) {
        return new PlanReference(null, jsonString);
    }

    /**
     * Create a reference from a file in the classpath, using {@code
     * Thread.currentThread().getContextClassLoader()} as {@link ClassLoader}.
     *
     * @throws TableException if the classpath resource cannot be found
     */
    public static PlanReference fromClasspath(String classpathFilePath) {
        return fromClasspath(Thread.currentThread().getContextClassLoader(), classpathFilePath);
    }

    /**
     * Create a reference from a file in the classpath.
     *
     * @throws TableException if the classpath resource cannot be found
     */
    public static PlanReference fromClasspath(ClassLoader classLoader, String classpathFilePath)
            throws TableException {
        URL url = classLoader.getResource(classpathFilePath);
        if (url == null) {
            throw new TableException(
                    "Cannot load the plan reference from classpath, resource not found: "
                            + classpathFilePath);
        }

        try {
            return PlanReference.fromFile(new File(url.toURI()));
        } catch (URISyntaxException e) {
            throw new TableException(
                    "Cannot load the plan reference from classpath, invalid URI: "
                            + classpathFilePath,
                    e);
        }
    }

    public Optional<File> getFile() {
        return Optional.ofNullable(file);
    }

    public Optional<String> getContent() {
        return Optional.ofNullable(content);
    }
}
