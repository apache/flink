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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link HistoryServerUtils}.
 */
public class HistoryServerUtilsTest extends TestLogger {

	private static final String HOSTNAME = "foobar";
	private static final int PORT = 1234;

	@Test
	public void testIsSSLEnabledDefault() {
		final Configuration configuration = new Configuration();

		assertThat(HistoryServerUtils.isSSLEnabled(configuration), is(false));
	}

	@Test
	public void testIsSSLEnabledWithoutRestSSL() {
		final Configuration configuration = new Configuration();
		configuration.setBoolean(HistoryServerOptions.HISTORY_SERVER_WEB_SSL_ENABLED, true);

		assertThat(HistoryServerUtils.isSSLEnabled(configuration), is(false));
	}

	@Test
	public void testIsSSLEnabledOnlyRestSSL() {
		final Configuration configuration = new Configuration();
		configuration.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);

		assertThat(HistoryServerUtils.isSSLEnabled(configuration), is(false));
	}

	@Test
	public void testIsSSLEnabled() {
		final Configuration configuration = new Configuration();
		enableSSL(configuration);

		assertThat(HistoryServerUtils.isSSLEnabled(configuration), is(true));
	}

	private void enableSSL(Configuration configuration) {
		configuration.setBoolean(HistoryServerOptions.HISTORY_SERVER_WEB_SSL_ENABLED, true);
		configuration.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
	}

	@Test
	public void testGetHistoryServerURL() throws MalformedURLException {
		final Configuration configuration = createDefaultConfiguration();

		final Optional<URL> historyServerURL = HistoryServerUtils.getHistoryServerURL(configuration);

		assertThat(historyServerURL.get(), is(new URL("http", HOSTNAME, PORT, "")));
	}

	@Test
	public void testGetHistoryServerURLWithSSL() throws MalformedURLException {
		final Configuration configuration = createDefaultConfiguration();
		enableSSL(configuration);

		final Optional<URL> historyServerURL = HistoryServerUtils.getHistoryServerURL(configuration);

		assertThat(historyServerURL.get(), is(new URL("https", HOSTNAME, PORT, "")));
	}

	@Test
	public void testGetHistoryServerURLWithoutHS() {
		final Configuration configuration = new Configuration();

		assertThat(HistoryServerUtils.getHistoryServerURL(configuration).isPresent(), is(false));
	}

	@Nonnull
	private Configuration createDefaultConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.setString(HistoryServerOptions.HISTORY_SERVER_WEB_ADDRESS, HOSTNAME);
		configuration.setInteger(HistoryServerOptions.HISTORY_SERVER_WEB_PORT, PORT);
		return configuration;
	}
}
