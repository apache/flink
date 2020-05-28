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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.cli.FallbackYarnSessionCli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * Tests for the {@link FallbackYarnSessionCliTest}.
 */
public class FallbackYarnSessionCliTest {

	@Test(expected = IllegalStateException.class)
	public void testExceptionWhenActiveWithYarnApplicationId() throws ParseException {
		checkIfYarnFallbackCLIisActiveWithCLIArgs(
				"run",
				"-yid", ApplicationId.newInstance(0L, 0).toString());
	}

	@Test(expected = IllegalStateException.class)
	public void testExceptionWhenActiveWithExplicitClusterType() throws ParseException {
		checkIfYarnFallbackCLIisActiveWithCLIArgs(
				"run",
				"-m", FallbackYarnSessionCli.ID);
	}

	@Test
	public void testFalseWhenNotActive() throws ParseException {
		final boolean isActive = checkIfYarnFallbackCLIisActiveWithCLIArgs("run");
		assertFalse(isActive);
	}

	private boolean checkIfYarnFallbackCLIisActiveWithCLIArgs(final String... args) throws ParseException {
		final Options options = new Options();
		final FallbackYarnSessionCli cliUnderTest =
				new FallbackYarnSessionCli(new Configuration());
		cliUnderTest.addGeneralOptions(options);

		final CommandLineParser parser = new DefaultParser();
		final CommandLine cmd = parser.parse(options, args);
		return cliUnderTest.isActive(cmd);
	}
}
