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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class WikipediaEditsSourceTest {

	/**
	 * NOTE: if you are behind a firewall you may need to use a SOCKS Proxy for this test
	 *
	 * @see <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html">Socks Proxy</a>
	 */
	@Test(timeout = 120 * 1000)
	public void testWikipediaEditsSource() throws Exception {

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
		}
		catch (Exception e) {
			assertNotNull(e.getCause());
			assertEquals("Expected test exception", e.getCause().getMessage());
		}
	}
}
