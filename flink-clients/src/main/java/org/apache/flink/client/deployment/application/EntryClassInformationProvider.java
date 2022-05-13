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

package org.apache.flink.client.deployment.application;

import java.io.File;
import java.util.Optional;

/** {@code EntryClassInformationProvider} provides information about the entry class. */
public interface EntryClassInformationProvider {

    /**
     * Returns the {@link File} referring to the Jar file that contains the job class or no {@code
     * File} if the job class is located on the classpath.
     *
     * @return The {@code File} referring to the job's Jar archive or an empty {@code Optional} in
     *     case the job shall be extracted from the classpath.
     */
    Optional<File> getJarFile();

    /**
     * Returns the name of the job class or an empty {@code Optional} if the job class name cannot
     * be provided.
     *
     * @return The name of the job class or an empty {@code Optional}.
     */
    Optional<String> getJobClassName();
}
