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
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Tests for the HistoryServerStaticFileServerHandler.
 */
public class HistoryServerStaticFileServerHandlerTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testRespondWithFile() throws Exception {
		File webDir = tmp.newFolder("webDir");
		Router router = new Router()
			.addGet("/:*", new HistoryServerStaticFileServerHandler(webDir));
		WebFrontendBootstrap webUI = new WebFrontendBootstrap(
			router,
			LoggerFactory.getLogger(HistoryServerStaticFileServerHandlerTest.class),
			tmp.newFolder("uploadDir"),
			null,
			"localhost",
			0,
			new Configuration());

		int port = webUI.getServerPort();
		try {
			// verify that 404 message is returned when requesting a non-existent file
			String notFound404 = HistoryServerTest.getFromHTTP("http://localhost:" + port + "/hello");
			Assert.assertTrue(notFound404.contains("404 Not Found"));

			// verify that a) a file can be loaded using the ClassLoader and b) that the HistoryServer
			// index_hs.html is injected
			String index = HistoryServerTest.getFromHTTP("http://localhost:" + port + "/index.html");
			Assert.assertTrue(index.contains("Completed Jobs"));

			// verify that index.html is appended if the request path ends on '/'
			String index2 = HistoryServerTest.getFromHTTP("http://localhost:" + port + "/");
			Assert.assertEquals(index, index2);

			// verify that a 404 message is returned when requesting a directory
			File dir = new File(webDir, "dir.json");
			dir.mkdirs();
			String dirNotFound404 = HistoryServerTest.getFromHTTP("http://localhost:" + port + "/dir");
			Assert.assertTrue(dirNotFound404.contains("404 Not Found"));

			// verify that a 404 message is returned when requesting a file outside the webDir
			tmp.newFile("secret");
			String x = HistoryServerTest.getFromHTTP("http://localhost:" + port + "/../secret");
			Assert.assertTrue(x.contains("404 Not Found"));
		} finally {
			webUI.shutdown();
		}
	}
}
