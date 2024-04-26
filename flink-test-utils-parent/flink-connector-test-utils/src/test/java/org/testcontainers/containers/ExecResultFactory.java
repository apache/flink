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

package org.testcontainers.containers;

/**
 * The {@link Container.ExecResult} is package private, this is workaround to create instances of it
 * in tests.
 */
public class ExecResultFactory {

    private ExecResultFactory() {
        // intentional
    }

    /**
     * Factory method creates a {@link Container.ExecResult}.
     *
     * @param exitCode exit code
     * @param stdout stdout
     * @param stderr stderr
     * @return instance of {@link Container.ExecResult}
     */
    public static Container.ExecResult execResult(
            final int exitCode, final String stdout, final String stderr) {
        return new Container.ExecResult(exitCode, stdout, stderr);
    }
}
