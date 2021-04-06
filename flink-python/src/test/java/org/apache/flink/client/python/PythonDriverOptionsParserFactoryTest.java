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

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests for the {@link PythonDriverOptionsParserFactory}. */
public class PythonDriverOptionsParserFactoryTest {

    private static final CommandLineParser<PythonDriverOptions> commandLineParser =
            new CommandLineParser<>(new PythonDriverOptionsParserFactory());

    @Test
    public void testPythonDriverOptionsParsing() throws FlinkParseException {
        final String[] args = {"--python", "xxx.py", "--input", "in.txt"};
        verifyPythonDriverOptionsParsing(args);
    }

    @Test
    public void testPymoduleOptionParsing() throws FlinkParseException {
        final String[] args = {"--pyModule", "xxx", "--input", "in.txt"};
        verifyPythonDriverOptionsParsing(args);
    }

    @Test
    public void testShortOptions() throws FlinkParseException {
        final String[] args = {"-py", "xxx.py", "--input", "in.txt"};
        verifyPythonDriverOptionsParsing(args);
    }

    @Test(expected = FlinkParseException.class)
    public void testMultipleEntrypointsSpecified() throws FlinkParseException {
        final String[] args = {"--python", "xxx.py", "--pyModule", "yyy", "--input", "in.txt"};
        commandLineParser.parse(args);
    }

    @Test(expected = FlinkParseException.class)
    public void testEntrypointNotSpecified() throws FlinkParseException {
        final String[] args = {"--input", "in.txt"};
        commandLineParser.parse(args);
    }

    private void verifyPythonDriverOptionsParsing(final String[] args) throws FlinkParseException {
        final PythonDriverOptions pythonCommandOptions = commandLineParser.parse(args);

        if (pythonCommandOptions.getEntryPointScript().isPresent()) {
            assertEquals("xxx.py", pythonCommandOptions.getEntryPointScript().get());
        } else {
            assertEquals("xxx", pythonCommandOptions.getEntryPointModule());
        }

        // verify the python program arguments
        final List<String> programArgs = pythonCommandOptions.getProgramArgs();
        assertEquals(2, programArgs.size());
        assertEquals("--input", programArgs.get(0));
        assertEquals("in.txt", programArgs.get(1));
    }
}
