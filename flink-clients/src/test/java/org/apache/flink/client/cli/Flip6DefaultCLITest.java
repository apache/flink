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
import org.apache.flink.util.TestLogger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for the {@link Flip6DefaultCLI}.
 */
public class Flip6DefaultCLITest extends TestLogger {

	/**
	 * Tests that the flip6 command line switch is recognized.
	 */
	@Test
	public void testFlip6Switch() throws CliArgsException {
		final String[] args = {"-flip6"};
		final Flip6DefaultCLI flip6DefaultCLI = new Flip6DefaultCLI(new Configuration());

		final Options options = new Options();
		flip6DefaultCLI.addGeneralOptions(options);
		flip6DefaultCLI.addRunOptions(options);

		final CommandLine commandLine = CliFrontendParser.parse(options, args, false);

		Assert.assertTrue(commandLine.hasOption(Flip6DefaultCLI.FLIP_6.getOpt()));
		Assert.assertTrue(flip6DefaultCLI.isActive(commandLine));
	}
}
