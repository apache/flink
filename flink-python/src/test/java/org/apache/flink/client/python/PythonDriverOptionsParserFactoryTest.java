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

package org.apache.flink.client.python;

import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link PythonDriverOptionsParserFactory}. */
class PythonDriverOptionsParserFactoryTest {

    private static final CommandLineParser<PythonDriverOptions> commandLineParser =
            new CommandLineParser<>(new PythonDriverOptionsParserFactory());

    @Test
    void testPythonDriverOptionsParsing() throws FlinkParseException {
        final String[] args = {"--python", "xxx.py", "--input", "in.txt"};
        verifyPythonDriverOptionsParsing(args);
    }

    @Test
    void testPymoduleOptionParsing() throws FlinkParseException {
        final String[] args = {"--pyModule", "xxx", "--input", "in.txt"};
        verifyPythonDriverOptionsParsing(args);
    }

    @Test
    void testShortOptions() throws FlinkParseException {
        final String[] args = {"-py", "xxx.py", "--input", "in.txt"};
        verifyPythonDriverOptionsParsing(args);
    }

    @Test
    void testMultipleEntrypointsSpecified() {
        final String[] args = {"--python", "xxx.py", "--pyModule", "yyy", "--input", "in.txt"};
        assertThatThrownBy(() -> commandLineParser.parse(args))
                .isInstanceOf(FlinkParseException.class);
    }

    @Test
    void testEntrypointNotSpecified() {
        final String[] args = {"--input", "in.txt"};
        assertThatThrownBy(() -> commandLineParser.parse(args))
                .isInstanceOf(FlinkParseException.class);
    }

    private void verifyPythonDriverOptionsParsing(final String[] args) throws FlinkParseException {
        final PythonDriverOptions pythonCommandOptions = commandLineParser.parse(args);

        if (pythonCommandOptions.getEntryPointScript().isPresent()) {
            assertThat(pythonCommandOptions.getEntryPointScript().get()).isEqualTo("xxx.py");
        } else {
            assertThat(pythonCommandOptions.getEntryPointModule()).isEqualTo("xxx");
        }

        // verify the python program arguments
        final List<String> programArgs = pythonCommandOptions.getProgramArgs();
        assertThat(programArgs).containsExactly("--input", "in.txt");
    }
}
