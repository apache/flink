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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for the {@link PythonDriverOptionsParserFactory}.
 */
public class PythonDriverOptionsParserFactoryTest {

	private static final CommandLineParser<PythonDriverOptions> commandLineParser = new CommandLineParser<>(
		new PythonDriverOptionsParserFactory());

	@Test
	public void testPythonDriverOptionsParsing() throws FlinkParseException {
		final String[] args = {"--python", "xxx.py", "--pyFiles", "a.py,b.py,c.py", "--input", "in.txt"};
		verifyPythonDriverOptionsParsing(args);
	}

	@Test
	public void testPymoduleOptionParsing() throws FlinkParseException {
		final String[] args = {"--pyModule", "xxx", "--pyFiles", "xxx.py,a.py,b.py,c.py", "--input", "in.txt"};
		verifyPythonDriverOptionsParsing(args);
	}

	@Test
	public void testPythonDependencyOptionsParsing() throws FlinkParseException {
		final String[] args = {
			"--python", "xxx.py",
			"--pyFiles", "/absolute/a.py,relative/b.py,relative/c.py",
			"--pyRequirements", "d.txt#e_dir",
			"--pyExecutable", "/usr/bin/python",
			"--pyArchives", "g.zip,h.zip#data,h.zip#data2",
		};
		verifyPythonDependencyOptionsParsing(args);
	}

	@Test
	public void testPythonDependencyShortOptionsParsing() throws FlinkParseException {
		final String[] args = {
			"-py", "xxx.py",
			"-pyfs", "/absolute/a.py,relative/b.py,relative/c.py",
			"-pyreq", "d.txt#e_dir",
			"-pyexec", "/usr/bin/python",
			"-pyarch", "g.zip,h.zip#data,h.zip#data2",
		};
		verifyPythonDependencyOptionsParsing(args);
	}

	@Test
	public void testRequirementsOptionsWithoutCachedDir() throws FlinkParseException {
		final String[] args = {
			"-py", "xxx.py",
			"-pyreq", "d.txt",
		};
		PythonDriverOptions pythonCommandOptions = commandLineParser.parse(args);

		assertEquals(new Tuple2<>("d.txt", null), pythonCommandOptions.getPyRequirements().get());
	}

	@Test
	public void testShortOptions() throws FlinkParseException {
		final String[] args = {"-py", "xxx.py", "-pyfs", "a.py,b.py,c.py", "--input", "in.txt"};
		verifyPythonDriverOptionsParsing(args);
	}

	@Test(expected = FlinkParseException.class)
	public void testMultipleEntrypointsSpecified() throws FlinkParseException {
		final String[] args = {
			"--python", "xxx.py", "--pyModule", "yyy", "--pyFiles", "a.py,b.py,c.py", "--input", "in.txt"};
		commandLineParser.parse(args);
	}

	@Test(expected = FlinkParseException.class)
	public void testEntrypointNotSpecified() throws FlinkParseException {
		final String[] args = {"--pyFiles", "a.py,b.py,c.py", "--input", "in.txt"};
		commandLineParser.parse(args);
	}

	@Test(expected = FlinkParseException.class)
	public void testPyFilesNotSpecified() throws FlinkParseException {
		final String[] args = {"--pyModule", "yyy", "--input", "in.txt"};
		commandLineParser.parse(args);
	}

	private void verifyPythonDriverOptionsParsing(final String[] args) throws FlinkParseException {
		final PythonDriverOptions pythonCommandOptions = commandLineParser.parse(args);

		// verify the parsed python entrypoint module
		assertEquals("xxx", pythonCommandOptions.getEntrypointModule());

		// verify the parsed python library files
		final List<Path> pythonMainFile = pythonCommandOptions.getPythonLibFiles();
		assertNotNull(pythonMainFile);
		assertEquals(4, pythonMainFile.size());
		assertEquals(
			pythonMainFile.stream().map(Path::getName).collect(Collectors.joining(",")),
			"xxx.py,a.py,b.py,c.py");

		// verify the python program arguments
		final List<String> programArgs = pythonCommandOptions.getProgramArgs();
		assertEquals(2, programArgs.size());
		assertEquals("--input", programArgs.get(0));
		assertEquals("in.txt", programArgs.get(1));
	}

	private void verifyPythonDependencyOptionsParsing(final String[] args) throws FlinkParseException {
		PythonDriverOptions pythonCommandOptions = commandLineParser.parse(args);
		List<String> expectedPythonFiles = new ArrayList<>();
		expectedPythonFiles.add("/absolute/a.py");
		expectedPythonFiles.add("relative/b.py");
		expectedPythonFiles.add("relative/c.py");
		assertEquals(expectedPythonFiles, pythonCommandOptions.getPyFiles());
		assertEquals(new Tuple2<>("d.txt", "e_dir"), pythonCommandOptions.getPyRequirements().get());
		List<Tuple2<String, String>> expectedPythonArchives = new ArrayList<>();
		expectedPythonArchives.add(new Tuple2<>("g.zip", null));
		expectedPythonArchives.add(new Tuple2<>("h.zip", "data"));
		expectedPythonArchives.add(new Tuple2<>("h.zip", "data2"));
		assertEquals(expectedPythonArchives, pythonCommandOptions.getPyArchives());
		assertEquals("/usr/bin/python", pythonCommandOptions.getPyExecutable().get());
	}
}
