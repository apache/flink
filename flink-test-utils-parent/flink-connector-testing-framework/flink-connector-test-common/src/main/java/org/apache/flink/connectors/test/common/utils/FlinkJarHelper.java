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

package org.apache.flink.connectors.test.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Objects;

/** Utilities for searching Flink JAR files and main class of Flink jobs. */
public class FlinkJarHelper {

    private static File searchedJarFile = null;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJarHelper.class);

    /**
     * Search JAR file in target folder.
     *
     * @return JAR file
     * @throws FileNotFoundException if JAR file is not found in target folder
     */
    public static File searchConnectorJar() throws FileNotFoundException {
        if (searchedJarFile == null) {
            // Search JAR file in target directory
            String moduleName = new File(System.getProperty("user.dir")).getName();
            File targetDir = new File(System.getProperty("user.dir"), "target");
            File jobJar = null;
            LOG.debug("Searching JAR in {} with module name {}", targetDir, moduleName);
            for (File file : Objects.requireNonNull(targetDir.listFiles())) {
                String filename = file.getName();
                if (filename.startsWith(moduleName) && filename.endsWith("-connector-jar.jar")) {
                    jobJar = file;
                }
            }
            if (jobJar == null) {
                throw new FileNotFoundException(
                        "Cannot find relative JAR file in the target directory. Make sure the maven project is built correctly.");
            }
            searchedJarFile = jobJar;
        }
        LOG.info("Found JAR file {}", searchedJarFile.getName());
        return searchedJarFile;
    }
}
