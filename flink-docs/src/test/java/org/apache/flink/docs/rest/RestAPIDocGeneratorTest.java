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

package org.apache.flink.docs.rest;

import org.apache.flink.docs.rest.data.TestDocumentingRestEndpoint;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.util.FileUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Tests for the {@link RestAPIDocGenerator}.
 */
public class RestAPIDocGeneratorTest {

	@Test
	public void testDocGeneration() throws Exception {
		File file = File.createTempFile("rest_v0_", ".html");
		RestAPIDocGenerator.createHtmlFile(
			new TestDocumentingRestEndpoint(),
			RestAPIVersion.V0,
			file.toPath());
		String actual = cleanUpHtml(FileUtils.readFile(file, "UTF-8"));

		File expectedFile = new File(
			RestAPIDocGeneratorTest.class.getClassLoader().getResource("rest_v0_expected.html").getFile());
		String expected = cleanUpHtml(FileUtils.readFile(expectedFile, "UTF-8"));

		Assert.assertEquals(expected, actual);
	}

	private String cleanUpHtml(String html) {
		html = removeComments(html);
		html = removeUniqueId(html);
		return html.trim();
	}

	private String removeComments(String s) {
		return s.replaceAll("<!--[\\s\\S]+?-->", "");
	}

	private String removeUniqueId(String s) {
		return s
			.replaceAll("id=\\\".+?\\\"", "id=\"\"")
			.replaceAll("data-target=\\\".+?\\\"", "data-target=\"\"");
	}
}
