/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tools.ci.utils.shade;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.tools.ci.utils.shared.Dependency;
import org.apache.flink.tools.ci.utils.shared.ParserUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Utils for parsing the shade-plugin output. */
public final class ShadeParser {

    private static final Pattern SHADE_NEXT_MODULE_PATTERN =
            Pattern.compile(
                    ".*:shade \\((?:shade-flink|shade-dist|default)\\) @ (?<module>[^ _]+)(?:_[0-9.]+)? --.*");

    private static final Pattern SHADE_INCLUDE_MODULE_PATTERN =
            Pattern.compile(
                    ".* "
                            + "(?<groupId>.*?):"
                            + "(?<artifactId>.*?):"
                            + "(?<type>.*?):"
                            + "(?:(?<classifier>.*?):)?"
                            + "(?<version>.*?)"
                            + " in the shaded jar");

    /**
     * Parses the output of a Maven build where {@code shade:shade} was used, and returns a set of
     * bundled dependencies for each module.
     *
     * <p>The returned dependencies will NEVER contain the scope or optional flag.
     *
     * <p>This method only considers the {@code shade-flink} and {@code shade-dist} executions,
     * because all artifacts we produce that are either published or referenced are created by these
     * executions. In other words, all artifacts from other executions are only used internally by
     * the module that created them.
     */
    public static Map<String, Set<Dependency>> parseShadeOutput(Path buildOutput)
            throws IOException {
        try (Stream<String> lines = Files.lines(buildOutput)) {
            return parseShadeOutput(lines);
        }
    }

    @VisibleForTesting
    static Map<String, Set<Dependency>> parseShadeOutput(Stream<String> lines) {
        return ParserUtils.parsePluginOutput(
                lines.filter(line -> !line.contains(" Excluding ")),
                SHADE_NEXT_MODULE_PATTERN,
                ShadeParser::parseBlock);
    }

    private static Set<Dependency> parseBlock(Iterator<String> block) {
        final Set<Dependency> dependencies = new LinkedHashSet<>();

        Optional<Dependency> parsedDependency = parseDependency(block.next());
        while (parsedDependency.isPresent()) {
            dependencies.add(parsedDependency.get());

            if (block.hasNext()) {
                parsedDependency = parseDependency(block.next());
            } else {
                parsedDependency = Optional.empty();
            }
        }

        return dependencies;
    }

    @VisibleForTesting
    static Optional<Dependency> parseDependency(String line) {
        Matcher dependencyMatcher = SHADE_INCLUDE_MODULE_PATTERN.matcher(line);
        if (!dependencyMatcher.find()) {
            return Optional.empty();
        }

        return Optional.of(
                Dependency.create(
                        dependencyMatcher.group("groupId"),
                        dependencyMatcher.group("artifactId"),
                        dependencyMatcher.group("version"),
                        dependencyMatcher.group("classifier")));
    }

    private ShadeParser() {}
}
