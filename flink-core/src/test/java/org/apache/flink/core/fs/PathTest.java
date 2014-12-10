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
package org.apache.flink.core.fs;

import java.io.IOException;
import java.net.URI;
import org.junit.Test;
import static org.junit.Assert.*;

public class PathTest {
	@Test
	public void testParsing() {
		URI u;
		String scheme = "hdfs";
		String authority = "localhost:8000";
		String path = "/test/test";

		// correct usage
		// hdfs://localhost:8000/test/test
		u = new Path(scheme + "://" + authority + path).toUri();
		assertEquals(scheme, u.getScheme());
		assertEquals(authority, u.getAuthority());
		assertEquals(path, u.getPath());
		// hdfs:///test/test
		u = new Path(scheme + "://" + path).toUri();
		assertEquals(scheme, u.getScheme());
		assertEquals(null, u.getAuthority());
		assertEquals(path, u.getPath());
		// hdfs:/test/test
		u = new Path(scheme + ":" + path).toUri();
		assertEquals(scheme, u.getScheme());
		assertEquals(null, u.getAuthority());
		assertEquals(path, u.getPath());

		// incorrect usage
		// hdfs://test/test
		u = new Path(scheme + ":/" + path).toUri();
		assertEquals(scheme, u.getScheme());
		assertEquals("test", u.getAuthority());
		assertEquals("/test", u.getPath());
		// hdfs:////test/test
		u = new Path(scheme + ":///" + path).toUri();
		assertEquals("hdfs", u.getScheme());
		assertEquals(null, u.getAuthority());
		assertEquals(path, u.getPath());
	}

	@Test
	public void testMakeQualified() throws IOException {
		String path;
		Path p;
		URI u;

		path = "test/test";
		p = new Path(path);
		u = p.toUri();
		p = p.makeQualified(FileSystem.get(u));
		u = p.toUri();
		assertEquals("file", u.getScheme());
		assertEquals(null, u.getAuthority());
		assertEquals(FileSystem.getLocalFileSystem().getWorkingDirectory().toUri().getPath() + "/" + path, u.getPath());

		path = "/test/test";
		p = new Path(path);
		u = p.toUri();
		p = p.makeQualified(FileSystem.get(u));
		u = p.toUri();
		assertEquals("file", u.getScheme());
		assertEquals(null, u.getAuthority());
		assertEquals(path, u.getPath());
	}
}
