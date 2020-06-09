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

package org.apache.flink.client.cli;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.client.cli.CliFrontendParser.PYARCHIVE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYEXEC_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYFILES_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYMODULE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYREQUIREMENTS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PY_OPTION;
import static org.apache.flink.python.PythonOptions.PYTHON_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_REQUIREMENTS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PythonProgramOptions}.
 */
public class PythonProgramOptionsTest {

	private Options options;

	@Before
	public void setUp() {
		options = new Options();
		options.addOption(PY_OPTION);
		options.addOption(PYFILES_OPTION);
		options.addOption(PYMODULE_OPTION);
		options.addOption(PYREQUIREMENTS_OPTION);
		options.addOption(PYARCHIVE_OPTION);
		options.addOption(PYEXEC_OPTION);
	}

	@Test
	public void testCreateProgramOptionsWithPythonCommandLine() throws CliArgsException {
		String[] parameters = {
			"-py", "test.py",
			"-pym", "test",
			"-pyfs", "test1.py,test2.zip,test3.egg,test4_dir",
			"-pyreq", "a.txt#b_dir",
			"-pyarch", "c.zip#venv,d.zip",
			"-pyexec", "bin/python",
			"userarg1", "userarg2"
		};

		CommandLine line = CliFrontendParser.parse(options, parameters, false);
		PythonProgramOptions programOptions = (PythonProgramOptions) ProgramOptions.create(line);
		Configuration config = new Configuration();
		programOptions.applyToConfiguration(config);
		assertEquals("test1.py,test2.zip,test3.egg,test4_dir", config.get(PythonOptions.PYTHON_FILES));
		assertEquals("a.txt#b_dir", config.get(PYTHON_REQUIREMENTS));
		assertEquals("c.zip#venv,d.zip", config.get(PythonOptions.PYTHON_ARCHIVES));
		assertEquals("bin/python", config.get(PYTHON_EXECUTABLE));
		assertArrayEquals(
			new String[] {"--python", "test.py", "--pyModule", "test", "userarg1", "userarg2"},
			programOptions.getProgramArgs());
	}

	@Test
	public void testCreateProgramOptionsWithLongOptions() throws CliArgsException {
		String[] args = {
			"--python", "xxx.py",
			"--pyModule", "xxx",
			"--pyFiles", "/absolute/a.py,relative/b.py,relative/c.py",
			"--pyRequirements", "d.txt#e_dir",
			"--pyExecutable", "/usr/bin/python",
			"--pyArchives", "g.zip,h.zip#data,h.zip#data2",
			"userarg1", "userarg2"
		};

		CommandLine line = CliFrontendParser.parse(options, args, false);
		PythonProgramOptions programOptions = (PythonProgramOptions) ProgramOptions.create(line);
		Configuration config = new Configuration();
		programOptions.applyToConfiguration(config);
		assertEquals("/absolute/a.py,relative/b.py,relative/c.py", config.get(PythonOptions.PYTHON_FILES));
		assertEquals("d.txt#e_dir", config.get(PYTHON_REQUIREMENTS));
		assertEquals("g.zip,h.zip#data,h.zip#data2", config.get(PythonOptions.PYTHON_ARCHIVES));
		assertEquals("/usr/bin/python", config.get(PYTHON_EXECUTABLE));
		assertArrayEquals(
			new String[] {"--python", "xxx.py", "--pyModule", "xxx", "userarg1", "userarg2"},
			programOptions.getProgramArgs());
	}
}
