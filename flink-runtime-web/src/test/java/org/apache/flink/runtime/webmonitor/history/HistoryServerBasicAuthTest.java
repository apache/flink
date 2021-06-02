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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.rest.RestBasicAuthITCase;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

/** Test for the HistoryServer Basic authentication integration. */
public class HistoryServerBasicAuthTest extends TestLogger {

    private static final String PASSWORD_FILE =
            RestBasicAuthITCase.class.getResource("/.htpasswd").getFile();

    private static final String USER_CREDENTIALS = "testusr:testpwd";

    @Rule public final TemporaryFolder haBaseFolder = new TemporaryFolder();

    private File jmDirectory;

    @Before
    public void setUp() throws Exception {
        jmDirectory = haBaseFolder.newFolder("jm");
    }

    @Test
    public void testAuthGoodCredentials() throws Exception {
        Configuration serverConfig = getServerConfig();
        HistoryServer hs = new HistoryServer(serverConfig);

        try {
            hs.start();
            String baseUrl = "http://localhost:" + hs.getWebPort();
            String authHeader =
                    "Basic " + new String(Base64.getEncoder().encode(USER_CREDENTIALS.getBytes()));
            Assert.assertEquals(200, getHTTPResponseCode(baseUrl, authHeader));
        } finally {
            hs.stop();
        }
    }

    @Test
    public void testAuthBadCredentials() throws Exception {
        Configuration serverConfig = getServerConfig();
        HistoryServer hs = new HistoryServer(serverConfig);

        try {
            hs.start();
            String baseUrl = "http://localhost:" + hs.getWebPort();
            Assert.assertEquals(401, getHTTPResponseCode(baseUrl, null));
        } finally {
            hs.stop();
        }
    }

    private Configuration getServerConfig() {
        Configuration config = new Configuration();
        config.setString(
                HistoryServerOptions.HISTORY_SERVER_ARCHIVE_DIRS, jmDirectory.toURI().toString());
        config.set(HistoryServerOptions.HISTORY_SERVER_WEB_BASIC_AUTH_ENABLED, true);
        config.set(SecurityOptions.BASIC_AUTH_PWD_FILE, PASSWORD_FILE);
        config.set(SecurityOptions.BASIC_AUTH_ENABLED, true);
        config.set(SecurityOptions.BASIC_AUTH_CLIENT_CREDENTIALS, USER_CREDENTIALS);
        return config;
    }

    private int getHTTPResponseCode(String url, String authHeader) throws Exception {
        URL u = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) u.openConnection();
        connection.setConnectTimeout(10000);
        if (authHeader != null) {
            connection.setRequestProperty("Authorization", authHeader);
        }
        connection.connect();
        return connection.getResponseCode();
    }
}
