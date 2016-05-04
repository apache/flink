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

package org.apache.flink.streaming.connectors.wikiedits;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class WikipediaEditsSourceTest {

	private static final Logger LOG = LoggerFactory.getLogger(WikipediaEditsSourceTest.class);

	/**
	 * NOTE: if you are behind a firewall you may need to use a SOCKS Proxy for this test.
	 *
	 * We first check the connection to the IRC server. If it fails, this test
	 * is effectively ignored.
	 *
	 * @see <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html">Socks Proxy</a>
	 */
	@Test(timeout = 120 * 1000)
	public void testWikipediaEditsSource() throws Exception {
		final int numRetries = 5;
		final int waitBetweenRetriesMillis = 2000;
		final int connectTimeout = 1000;

		boolean success = false;

		for (int i = 0; i < numRetries && !success; i++) {
			// Check connection
			boolean canConnect = false;

			String host = WikipediaEditsSource.DEFAULT_HOST;
			int port = WikipediaEditsSource.DEFAULT_PORT;

			try (Socket s = new Socket()) {
				s.connect(new InetSocketAddress(host, port), connectTimeout);
				canConnect = s.isConnected();
			} catch (Throwable ignored) {
			}

			if (canConnect) {
				StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				env.getConfig().disableSysoutLogging();

				DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());

				edits.addSink(new SinkFunction<WikipediaEditEvent>() {
					@Override
					public void invoke(WikipediaEditEvent value) throws Exception {
						throw new Exception("Expected test exception");
					}
				});

				try {
					env.execute();
					fail("Did not throw expected Exception.");
				} catch (Exception e) {
					assertNotNull(e.getCause());
					assertEquals("Expected test exception", e.getCause().getMessage());
				}

				success = true;
			} else {
				LOG.info("Failed to connect to IRC server ({}/{}). Retrying in {} ms.",
						i + 1,
						numRetries,
						waitBetweenRetriesMillis);

				Thread.sleep(waitBetweenRetriesMillis);
			}
		}

		if (success) {
			LOG.info("Successfully ran test.");
		} else {
			LOG.info("Skipped test, because not able to connect to IRC server.");
		}
	}
}
