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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Unresolved pointer to a persisted plan.
 *
 * <p>A plan represents a static, executable entity that has been compiled from a Table & SQL API
 * pipeline definition.
 *
 * <p>You can load the content of this reference into a {@link CompiledPlan} using {@link
 * TableEnvironment#loadPlan(PlanReference)}, or you can directly load and execute it with {@link
 * TableEnvironment#executePlan(PlanReference)}.
 *
 * @see CompiledPlan
 */
@Experimental
public abstract class PlanReference {

    private PlanReference() {}

    /** @see #fromFile(File) */
    public static PlanReference fromFile(String path) {
        Objects.requireNonNull(path, "Path cannot be null");
        return fromFile(Paths.get(path).toFile());
    }

    /** @see #fromFile(File) */
    public static PlanReference fromFile(Path path) {
        Objects.requireNonNull(path, "Path cannot be null");
        return fromFile(path.toFile());
    }

    /** Create a reference starting from a file path. */
    public static PlanReference fromFile(File file) {
        Objects.requireNonNull(file, "File cannot be null");
        return new FilePlanReference(file);
    }

    /** Create a reference starting from a JSON string. */
    public static PlanReference fromJsonString(String jsonString) {
        Objects.requireNonNull(jsonString, "Json string cannot be null");
        return new ContentPlanReference(jsonString);
    }

    /**
     * Create a reference from a file in the classpath, using {@code
     * Thread.currentThread().getContextClassLoader()} as {@link ClassLoader}.
     */
    public static PlanReference fromResource(String resourcePath) {
        Objects.requireNonNull(resourcePath, "Resource path cannot be null");
        return fromResource(Thread.currentThread().getContextClassLoader(), resourcePath);
    }

    /** Create a reference from a file in the classpath. */
    public static PlanReference fromResource(ClassLoader classLoader, String resourcePath) {
        Objects.requireNonNull(classLoader, "ClassLoader cannot be null");
        Objects.requireNonNull(resourcePath, "Resource path cannot be null");
        return new ResourcePlanReference(classLoader, resourcePath);
    }

    /** Plan reference to a file in the local filesystem. */
    @Experimental
    public static class FilePlanReference extends PlanReference {

        private final File file;

        private FilePlanReference(File file) {
            this.file = file;
        }

        public File getFile() {
            return file;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FilePlanReference that = (FilePlanReference) o;
            return Objects.equals(file, that.file);
        }

        @Override
        public int hashCode() {
            return Objects.hash(file);
        }

        @Override
        public String toString() {
            return "Plan from file '" + file + "'";
        }
    }

    /** Plan reference to a string containing the serialized persisted plan in JSON. */
    @Experimental
    public static class ContentPlanReference extends PlanReference {

        private final String content;

        private ContentPlanReference(String content) {
            this.content = content;
        }

        public String getContent() {
            return content;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ContentPlanReference that = (ContentPlanReference) o;
            return Objects.equals(content, that.content);
        }

        @Override
        public int hashCode() {
            return Objects.hash(content);
        }

        @Override
        public String toString() {
            return "Plan:\n" + content;
        }
    }

    /** Plan reference to a file in the provided {@link ClassLoader}. */
    @Experimental
    public static class ResourcePlanReference extends PlanReference {

        private final ClassLoader classLoader;
        private final String resourcePath;

        private ResourcePlanReference(ClassLoader classLoader, String resourcePath) {
            this.classLoader = classLoader;
            this.resourcePath =
                    resourcePath.startsWith("/") ? resourcePath.substring(1) : resourcePath;
        }

        public ClassLoader getClassLoader() {
            return classLoader;
        }

        public String getResourcePath() {
            return resourcePath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ResourcePlanReference that = (ResourcePlanReference) o;
            return Objects.equals(classLoader, that.classLoader)
                    && Objects.equals(resourcePath, that.resourcePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classLoader, resourcePath);
        }

        @Override
        public String toString() {
            return "Plan from resource '" + resourcePath + "' in classloader '" + classLoader + "'";
        }
    }
}
