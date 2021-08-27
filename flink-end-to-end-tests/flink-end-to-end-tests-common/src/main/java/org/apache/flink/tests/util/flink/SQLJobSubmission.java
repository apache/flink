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

package org.apache.flink.tests.util.flink;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Programmatic definition of a SQL job-submission. */
public class SQLJobSubmission {

    private final List<String> sqlLines;
    private final List<String> jars;

    private SQLJobSubmission(List<String> sqlLines, List<String> jars) {
        this.sqlLines = checkNotNull(sqlLines);
        this.jars = checkNotNull(jars);
    }

    public List<String> getJars() {
        return this.jars;
    }

    public List<String> getSqlLines() {
        return this.sqlLines;
    }

    /** Builder for the {@link SQLJobSubmission}. */
    public static class SQLJobSubmissionBuilder {
        private final List<String> sqlLines;
        private final List<String> jars = new ArrayList<>();

        public SQLJobSubmissionBuilder(List<String> sqlLines) {
            this.sqlLines = sqlLines;
        }

        public SQLJobSubmissionBuilder addJar(Path jarFile) {
            this.jars.add(jarFile.toAbsolutePath().toString());
            return this;
        }

        public SQLJobSubmissionBuilder addJars(Path... jarFiles) {
            for (Path jarFile : jarFiles) {
                addJar(jarFile);
            }
            return this;
        }

        public SQLJobSubmissionBuilder addJars(List<Path> jarFiles) {
            jarFiles.forEach(this::addJar);
            return this;
        }

        public SQLJobSubmission build() {
            return new SQLJobSubmission(sqlLines, jars);
        }
    }
}
