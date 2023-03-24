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

package org.apache.flink.tools.ci.utils.deploy;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.tools.ci.utils.shared.ParserUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Parsing utils for the Maven deploy plugin. */
public class DeployParser {

    // Examples:
    //
    // Deployment on CI with alternative repo
    // [INFO] --- maven-deploy-plugin:2.8.2:deploy (default-deploy) @ flink-parent ---
    // [INFO] Using alternate deployment repository.../tmp/flink-validation-deployment
    //
    // Skipped deployment:
    // [INFO] --- maven-deploy-plugin:2.8.2:deploy (default-deploy) @ flink-parent ---
    // [INFO] Skipping artifact deployment
    private static final Pattern DEPLOY_MODULE_PATTERN =
            Pattern.compile(
                    ".maven-deploy-plugin:.*:deploy .* @ (?<module>[^ _]+)(?:_[0-9.]+)? --.*");

    /**
     * Parses the output of a Maven build where {@code deploy:deploy} was used, and returns a set of
     * deployed modules.
     */
    public static Set<String> parseDeployOutput(File buildResult) throws IOException {
        try (Stream<String> linesStream = Files.lines(buildResult.toPath())) {
            return parseDeployOutput(linesStream);
        }
    }

    @VisibleForTesting
    static Set<String> parseDeployOutput(Stream<String> lines) {
        return ParserUtils.parsePluginOutput(
                        lines, DEPLOY_MODULE_PATTERN, DeployParser::parseDeployBlock)
                .entrySet().stream()
                .filter(Map.Entry::getValue)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private static boolean parseDeployBlock(Iterator<String> block) {
        return block.hasNext() && !block.next().contains("Skipping artifact deployment");
    }
}
