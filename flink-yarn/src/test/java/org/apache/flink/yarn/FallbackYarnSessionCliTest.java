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

import org.apache.commons.cli.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.cli.FallbackYarnSessionCli;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.junit.jupiter.api.Assertions.assertFalse;

/** Tests for the {@link FallbackYarnSessionCliTest}. */
public class FallbackYarnSessionCliTest {

    @Test
    public void testExceptionWhenActiveWithYarnApplicationId() throws ParseException {
        assertThrows(
                IllegalStateException.class,
                () -> {
                    checkIfYarnFallbackCLIisActiveWithCLIArgs(
                            "run", "-yid", ApplicationId.newInstance(0L, 0).toString());
                });
    }

    @Test
    public void testExceptionWhenActiveWithExplicitClusterType() throws ParseException {
        assertThrows(
                IllegalStateException.class,
                () -> {
                    checkIfYarnFallbackCLIisActiveWithCLIArgs(
                            "run", "-m", FallbackYarnSessionCli.ID);
                });
    }

    @Test
    public void testFalseWhenNotActive() throws ParseException {
        final boolean isActive = checkIfYarnFallbackCLIisActiveWithCLIArgs("run");
        assertFalse(isActive);
    }

    private boolean checkIfYarnFallbackCLIisActiveWithCLIArgs(final String... args)
            throws ParseException {
        final Options options = new Options();
        final FallbackYarnSessionCli cliUnderTest = new FallbackYarnSessionCli(new Configuration());
        cliUnderTest.addGeneralOptions(options);

        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);
        return cliUnderTest.isActive(cmd);
    }
}
