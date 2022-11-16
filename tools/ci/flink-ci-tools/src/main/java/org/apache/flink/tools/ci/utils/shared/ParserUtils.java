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

package org.apache.flink.tools.ci.utils.shared;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Parsing utils. */
public class ParserUtils {

    /**
     * Iterates over the given lines, identifying plugin execution blocks with the given pattern and
     * parses the plugin output with the given parser.
     *
     * <p>This method assumes that the given pattern matches at most once for each module.
     *
     * <p>The given pattern must include a {@code module} group that captures the module that the
     * plugin runs on (without the scala suffix!).
     *
     * @param lines maven output lines
     * @param executionLinePattern pattern that matches plugin executions
     * @param blockParser parser for the plugin block
     * @return map containing the parser result for each module
     * @param <D> block parser output
     */
    public static <D> Map<String, D> parsePluginOutput(
            Stream<String> lines,
            Pattern executionLinePattern,
            Function<Iterator<String>, D> blockParser) {
        final Map<String, D> result = new LinkedHashMap<>();

        final Iterator<String> iterator = lines.iterator();

        while (iterator.hasNext()) {
            Matcher moduleMatcher = executionLinePattern.matcher(iterator.next());
            while (!moduleMatcher.find()) {
                if (iterator.hasNext()) {
                    moduleMatcher = executionLinePattern.matcher(iterator.next());
                } else {
                    return result;
                }
            }
            final String currentModule = moduleMatcher.group("module");

            if (!iterator.hasNext()) {
                throw new IllegalStateException("Expected more output from the plugin.");
            }

            result.put(currentModule, blockParser.apply(iterator));
        }
        return result;
    }
}
