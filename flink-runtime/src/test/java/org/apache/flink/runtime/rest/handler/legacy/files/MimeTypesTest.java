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

package org.apache.flink.runtime.rest.handler.legacy.files;

import org.apache.flink.runtime.rest.handler.util.MimeTypes;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests for the MIME types map.
 */
public class MimeTypesTest {

	@Test
	public void testCompleteness() {
		try {
			assertNotNull(MimeTypes.getMimeTypeForExtension("txt"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("htm"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("html"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("css"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("js"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("json"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("png"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("jpg"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("jpeg"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("gif"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("woff"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("woff2"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("otf"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("ttf"));
			assertNotNull(MimeTypes.getMimeTypeForExtension("eot"));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testFileNameExtraction() {
		try {
			assertNotNull(MimeTypes.getMimeTypeForFileName("test.txt"));
			assertNotNull(MimeTypes.getMimeTypeForFileName("t.txt"));
			assertNotNull(MimeTypes.getMimeTypeForFileName("first.second.third.txt"));

			assertNull(MimeTypes.getMimeTypeForFileName(".txt"));
			assertNull(MimeTypes.getMimeTypeForFileName("txt"));
			assertNull(MimeTypes.getMimeTypeForFileName("test."));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
