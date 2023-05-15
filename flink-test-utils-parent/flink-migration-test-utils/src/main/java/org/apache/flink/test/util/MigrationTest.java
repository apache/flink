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

package org.apache.flink.test.util;

import org.apache.flink.FlinkVersion;
import org.apache.flink.test.migration.PublishedVersionUtils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Interface for state migration tests. */
public interface MigrationTest {

    static FlinkVersion getMostRecentlyPublishedVersion() {
        return PublishedVersionUtils.getMostRecentlyPublishedVersion();
    }

    /**
     * Marks a method as snapshots generator. The method should be like
     *
     * <pre>
     * {@literal @}SnapshotsGenerator
     * void function(FlinkVersion version) {}
     * </pre>
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface SnapshotsGenerator {}

    /**
     * Marks a method as parameterized snapshots generator. The value should be the method that
     * returns Collection of arguments.
     *
     * <p>The method generating parameters should be like
     *
     * <pre>
     * Collection<?> generateMethodParameters(FlinkVersion version) {}
     * </pre>
     *
     * <p>The generator method should be like
     *
     * <pre>
     * {@literal @}ParameterizedSnapshotsGenerator("generateMethodParameters")
     * void function(T parameter) {}
     * </pre>
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface ParameterizedSnapshotsGenerator {
        String value();
    }
}
