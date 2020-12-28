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
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;

import org.apache.commons.cli.CommandLine;
import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link DefaultCLI}. */
public class DefaultCLITest {

    /** Verifies command line options are correctly materialized. */
    @Test
    public void testCommandLineMaterialization() throws Exception {
        final String hostname = "home-sweet-home";
        final int port = 1234;
        final String[] args = {"-m", hostname + ':' + port};

        final AbstractCustomCommandLine defaultCLI = new DefaultCLI();
        final CommandLine commandLine = defaultCLI.parseCommandLineOptions(args, false);

        Configuration configuration = defaultCLI.toConfiguration(commandLine);

        assertThat(configuration.get(RestOptions.ADDRESS), is(hostname));
        assertThat(configuration.get(RestOptions.PORT), is(port));
    }

    @Test
    public void testDynamicPropertyMaterialization() throws Exception {
        final String[] args = {
            "-D" + PipelineOptions.AUTO_WATERMARK_INTERVAL.key() + "=42",
            "-D" + PipelineOptions.AUTO_GENERATE_UIDS.key() + "=true"
        };

        final AbstractCustomCommandLine defaultCLI = new DefaultCLI();
        final CommandLine commandLine = defaultCLI.parseCommandLineOptions(args, false);

        Configuration configuration = defaultCLI.toConfiguration(commandLine);

        assertThat(
                configuration.get(PipelineOptions.AUTO_WATERMARK_INTERVAL),
                is(Duration.ofMillis(42L)));
        assertThat(configuration.get(PipelineOptions.AUTO_GENERATE_UIDS), is(true));
    }
}
