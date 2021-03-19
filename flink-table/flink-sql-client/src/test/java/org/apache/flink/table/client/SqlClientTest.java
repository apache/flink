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

package org.apache.flink.table.client;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for {@link SqlClient}. */
public class SqlClientTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private PrintStream originalPrintStream;

    private ByteArrayOutputStream testOutputStream;

    private Map<String, String> originalEnv;

    private String historyPath;

    @Before
    public void before() throws IOException {
        originalEnv = System.getenv();
        originalPrintStream = System.out;
        testOutputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(testOutputStream, true));

        // prepare conf dir
        File confFolder = tempFolder.newFolder("conf");
        File confYaml = new File(confFolder, "flink-conf.yaml");
        if (!confYaml.createNewFile()) {
            throw new IOException("Can't create testing flink-conf.yaml file.");
        }

        // adjust the test environment for the purposes of this test
        Map<String, String> map = new HashMap<>(System.getenv());
        map.put(ENV_FLINK_CONF_DIR, confFolder.getAbsolutePath());
        CommonTestUtils.setEnv(map);

        historyPath = tempFolder.newFile("history").toString();
    }

    @After
    public void after() throws InterruptedException {
        System.setOut(originalPrintStream);
        CommonTestUtils.setEnv(originalEnv);
    }

    private String getStdoutString() {
        return testOutputStream.toString();
    }

    @Test(timeout = 20000)
    public void testEmbeddedWithOptions() throws InterruptedException {
        String[] args = new String[] {"embedded", "-hist", historyPath};
        verifyCliStarted(args);
        assertThat(getStdoutString(), containsString("Command history file path: " + historyPath));
    }

    @Test(timeout = 20000)
    public void testEmbeddedWithLongOptions() throws InterruptedException {
        String[] args = new String[] {"embedded", "--history", historyPath};
        verifyCliStarted(args);
        assertThat(getStdoutString(), containsString("Command history file path: " + historyPath));
    }

    @Test(timeout = 20000)
    public void testEmbeddedWithoutOptions() throws InterruptedException {
        String[] args = new String[] {"embedded"};
        verifyCliStarted(args);
    }

    @Test(timeout = 20000)
    public void testEmptyOptions() throws InterruptedException {
        String[] args = new String[] {};
        verifyCliStarted(args);
    }

    @Test(timeout = 20000)
    public void testUnsupportedGatewayMode() throws Exception {
        String[] args = new String[] {"gateway"};
        thrown.expect(SqlClientException.class);
        thrown.expectMessage("Gateway mode is not supported yet.");
        SqlClient.main(args);
    }

    @Test(timeout = 20000)
    public void testPrintHelpForUnknownMode() throws IOException {
        String[] args = new String[] {"unknown"};
        SqlClient.main(args);
        final URL url = getClass().getClassLoader().getResource("sql-client-help.out");
        Objects.requireNonNull(url);
        final String help = FileUtils.readFileUtf8(new File(url.getFile()));
        assertEquals(help, getStdoutString());
    }

    private void verifyCliStarted(String[] args) throws InterruptedException {
        Thread thread = new Thread(() -> SqlClient.main(args));
        thread.start();

        // wait until cli is started
        while (!getStdoutString().contains("Command history file path")) {
            //noinspection BusyWait
            Thread.sleep(100);
        }

        while (!thread.isInterrupted()) {
            thread.interrupt();
        }
    }
}
