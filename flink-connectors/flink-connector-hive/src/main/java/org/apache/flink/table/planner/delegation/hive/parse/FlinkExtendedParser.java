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

package org.apache.flink.table.planner.delegation.hive.parse;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.ClearOperation;
import org.apache.flink.table.operations.command.HelpOperation;
import org.apache.flink.table.operations.command.QuitOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link FlinkExtendedParser} is used for parsing some special command which should be converted to
 * Flink's {@link org.apache.flink.table.operations.Operation} and follow Flink's behavior.
 */
public class FlinkExtendedParser {
    private static final List<ExtendedParseStrategy> PARSE_STRATEGIES =
            Arrays.asList(
                    ClearOperationParseStrategy.INSTANCE,
                    HelpOperationParseStrategy.INSTANCE,
                    QuitOperationParseStrategy.INSTANCE,
                    ResetOperationParseStrategy.INSTANCE);

    /**
     * Convert the statement which match some special command of Flink to {@link Operation}.
     *
     * @return the operation for Flink's extended command, empty for no match Flink's extended
     *     command.
     */
    public static Optional<Operation> parseFlinkExtendedCommand(String statement) {
        for (ExtendedParseStrategy strategy : PARSE_STRATEGIES) {
            if (strategy.match(statement)) {
                return Optional.of(strategy.convert(statement));
            }
        }
        return Optional.empty();
    }

    /**
     * Convert the statement to {@link SetOperation} with Flink's parse rule.
     *
     * @return the {@link SetOperation}, empty if the statement is not set command.
     */
    public static Optional<Operation> parseSet(String statement) {
        if (SetOperationParseStrategy.INSTANCE.match(statement)) {
            return Optional.of(SetOperationParseStrategy.INSTANCE.convert(statement));
        }
        return Optional.empty();
    }

    /** Convert the command to {@link ResetOperation} with Flink's parse rule. */
    public static Operation parseReset(String statement) {
        if (ResetOperationParseStrategy.INSTANCE.match(statement)) {
            return ResetOperationParseStrategy.INSTANCE.convert(statement);
        } else {
            throw new TableException(
                    String.format("The statement %s is not a RESET command.", statement));
        }
    }

    /**
     * Strategy to parse statement to {@link Operation}. parsing some special command which can't
     * supported by {@link org.apache.flink.table.planner.delegation.hive.HiveParser}, e.g. {@code
     * QUIT}, {@code CLEAR}.
     */
    private interface ExtendedParseStrategy {
        /** Determine whether the input statement is satisfied the strategy. */
        boolean match(String statement);

        /** Convert the input statement to the {@link Operation}. */
        Operation convert(String statement);
    }

    /** Strategy to parse statement to {@link Operation} by regex. */
    public abstract static class AbstractRegexParseStrategy implements ExtendedParseStrategy {
        protected static final int DEFAULT_PATTERN_FLAGS =
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

        protected Pattern pattern;

        protected AbstractRegexParseStrategy(Pattern pattern) {
            this.pattern = pattern;
        }

        @Override
        public boolean match(String statement) {
            return pattern.matcher(statement.trim()).matches();
        }
    }

    /** Strategy to parse statement to {@link ClearOperation}. */
    private static class ClearOperationParseStrategy extends AbstractRegexParseStrategy {

        private static final ClearOperationParseStrategy INSTANCE =
                new ClearOperationParseStrategy();

        private ClearOperationParseStrategy() {
            super(Pattern.compile("CLEAR\\s*;?", DEFAULT_PATTERN_FLAGS));
        }

        @Override
        public Operation convert(String statement) {
            return new ClearOperation();
        }
    }

    /** Strategy to parse statement to {@link HelpOperation}. */
    private static class HelpOperationParseStrategy extends AbstractRegexParseStrategy {

        private static final HelpOperationParseStrategy INSTANCE = new HelpOperationParseStrategy();

        private HelpOperationParseStrategy() {
            super(Pattern.compile("HELP\\s*;?", DEFAULT_PATTERN_FLAGS));
        }

        @Override
        public Operation convert(String statement) {
            return new HelpOperation();
        }
    }

    /** Operation to parse statement to {@link QuitOperation}. */
    private static class QuitOperationParseStrategy extends AbstractRegexParseStrategy {

        private static final QuitOperationParseStrategy INSTANCE = new QuitOperationParseStrategy();

        private QuitOperationParseStrategy() {
            super(Pattern.compile("(EXIT|QUIT)\\s*;?", DEFAULT_PATTERN_FLAGS));
        }

        @Override
        public Operation convert(String statement) {
            return new QuitOperation();
        }
    }

    /** Strategy to parse statement to {@link ResetOperation}. */
    private static class ResetOperationParseStrategy extends AbstractRegexParseStrategy {

        private static final ResetOperationParseStrategy INSTANCE =
                new ResetOperationParseStrategy();

        private ResetOperationParseStrategy() {
            super(Pattern.compile("RESET(\\s+(?<key>[^'\\s]+)\\s*)?\\s*;?", DEFAULT_PATTERN_FLAGS));
        }

        @Override
        public Operation convert(String statement) {
            Matcher matcher = pattern.matcher(statement.trim());
            String key;

            if (matcher.find()) {
                key = matcher.group("key");
            } else {
                throw new TableException(
                        String.format(
                                "Failed to convert the statement to RESET operation: %s.",
                                statement));
            }

            return new ResetOperation(key);
        }
    }

    /** Strategy to parse statement to {@link SetOperation}. */
    private static class SetOperationParseStrategy extends AbstractRegexParseStrategy {

        static final SetOperationParseStrategy INSTANCE = new SetOperationParseStrategy();

        private SetOperationParseStrategy() {
            super(
                    Pattern.compile(
                            "SET(\\s+(?<key>[^'\\s]+)\\s*=\\s*('(?<quotedVal>[^']*)'|(?<val>[^;\\s]+)))?\\s*;?",
                            DEFAULT_PATTERN_FLAGS));
        }

        @Override
        public Operation convert(String statement) {
            Matcher matcher = pattern.matcher(statement.trim());
            final List<String> operands = new ArrayList<>();
            if (matcher.find()) {
                if (matcher.group("key") != null) {
                    operands.add(matcher.group("key"));
                    operands.add(
                            matcher.group("quotedVal") != null
                                    ? matcher.group("quotedVal")
                                    : matcher.group("val"));
                }
            }

            // only capture SET
            if (operands.isEmpty()) {
                return new SetOperation();
            } else if (operands.size() == 2) {
                return new SetOperation(operands.get(0), operands.get(1));
            } else {
                // impossible
                throw new TableException(
                        String.format(
                                "Failed to convert the statement to SET operation: %s.",
                                statement));
            }
        }
    }
}
