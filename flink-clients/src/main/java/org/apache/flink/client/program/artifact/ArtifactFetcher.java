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

package org.apache.flink.client.program.artifact;

import org.apache.flink.configuration.Configuration;

import java.io.File;

/** Abstract artifact fetcher. */
abstract class ArtifactFetcher {

    /**
     * Fetch the resource from the uri to the targetDir.
     *
     * @param uri The artifact to be fetched.
     * @param flinkConfiguration Flink configuration.
     * @param targetDir The target dir to put the artifact.
     * @return The path of the fetched artifact.
     * @throws Exception Error during fetching the artifact.
     */
    abstract File fetch(String uri, Configuration flinkConfiguration, File targetDir)
            throws Exception;
}
