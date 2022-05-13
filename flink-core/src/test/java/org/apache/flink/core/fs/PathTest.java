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

import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link Path} class. */
public class PathTest {

    @Test
    public void testPathFromString() {

        Path p = new Path("/my/path");
        assertEquals("/my/path", p.toUri().getPath());
        assertNull(p.toUri().getScheme());

        p = new Path("/my/path/");
        assertEquals("/my/path", p.toUri().getPath());
        assertNull(p.toUri().getScheme());

        p = new Path("/my//path/");
        assertEquals("/my/path", p.toUri().getPath());
        assertNull(p.toUri().getScheme());

        p = new Path("/my//path//a///");
        assertEquals("/my/path/a", p.toUri().getPath());
        assertNull(p.toUri().getScheme());

        p = new Path("\\my\\path\\\\a\\\\\\");
        assertEquals("/my/path/a", p.toUri().getPath());
        assertNull(p.toUri().getScheme());

        p = new Path("hdfs:///my/path");
        assertEquals("/my/path", p.toUri().getPath());
        assertEquals("hdfs", p.toUri().getScheme());

        p = new Path("hdfs:///my/path/");
        assertEquals("/my/path", p.toUri().getPath());
        assertEquals("hdfs", p.toUri().getScheme());

        p = new Path("file:///my/path");
        assertEquals("/my/path", p.toUri().getPath());
        assertEquals("file", p.toUri().getScheme());

        p = new Path("C:/my/windows/path");
        assertEquals("/C:/my/windows/path", p.toUri().getPath());

        p = new Path("file:/C:/my/windows/path");
        assertEquals("/C:/my/windows/path", p.toUri().getPath());

        try {
            new Path((String) null);
            fail();
        } catch (Exception e) {
            // exception expected
        }

        try {
            new Path("");
            fail();
        } catch (Exception e) {
            // exception expected
        }
    }

    @Test
    public void testIsAbsolute() {

        // UNIX

        Path p = new Path("/my/abs/path");
        assertTrue(p.isAbsolute());

        p = new Path("/");
        assertTrue(p.isAbsolute());

        p = new Path("./my/rel/path");
        assertFalse(p.isAbsolute());

        p = new Path("my/rel/path");
        assertFalse(p.isAbsolute());

        // WINDOWS

        p = new Path("C:/my/abs/windows/path");
        assertTrue(p.isAbsolute());

        p = new Path("y:/my/abs/windows/path");
        assertTrue(p.isAbsolute());

        p = new Path("/y:/my/abs/windows/path");
        assertTrue(p.isAbsolute());

        p = new Path("b:\\my\\abs\\windows\\path");
        assertTrue(p.isAbsolute());

        p = new Path("/c:/my/dir");
        assertTrue(p.isAbsolute());

        p = new Path("/C:/");
        assertTrue(p.isAbsolute());

        p = new Path("C:");
        assertFalse(p.isAbsolute());

        p = new Path("C:/");
        assertTrue(p.isAbsolute());

        p = new Path("C:my\\relative\\path");
        assertFalse(p.isAbsolute());

        p = new Path("\\my\\dir");
        assertTrue(p.isAbsolute());

        p = new Path("\\");
        assertTrue(p.isAbsolute());

        p = new Path(".\\my\\relative\\path");
        assertFalse(p.isAbsolute());

        p = new Path("my\\relative\\path");
        assertFalse(p.isAbsolute());

        p = new Path("\\\\myServer\\myDir");
        assertTrue(p.isAbsolute());
    }

    @Test
    public void testGetName() {

        Path p = new Path("/my/fancy/path");
        assertEquals("path", p.getName());

        p = new Path("/my/fancy/path/");
        assertEquals("path", p.getName());

        p = new Path("hdfs:///my/path");
        assertEquals("path", p.getName());

        p = new Path("hdfs:///myPath/");
        assertEquals("myPath", p.getName());

        p = new Path("/");
        assertEquals("", p.getName());

        p = new Path("C:/my/windows/path");
        assertEquals("path", p.getName());

        p = new Path("file:/C:/my/windows/path");
        assertEquals("path", p.getName());
    }

    @Test
    public void testGetParent() {

        Path p = new Path("/my/fancy/path");
        assertEquals("/my/fancy", p.getParent().toUri().getPath());

        p = new Path("/my/other/fancy/path/");
        assertEquals("/my/other/fancy", p.getParent().toUri().getPath());

        p = new Path("hdfs:///my/path");
        assertEquals("/my", p.getParent().toUri().getPath());

        p = new Path("hdfs:///myPath/");
        assertEquals("/", p.getParent().toUri().getPath());

        p = new Path("/");
        assertNull(p.getParent());

        p = new Path("C:/my/windows/path");
        assertEquals("/C:/my/windows", p.getParent().toUri().getPath());
    }

    @Test
    public void testSuffix() {

        Path p = new Path("/my/path");
        p = p.suffix("_123");
        assertEquals("/my/path_123", p.toUri().getPath());

        p = new Path("/my/path/");
        p = p.suffix("/abc");
        assertEquals("/my/path/abc", p.toUri().getPath());

        p = new Path("C:/my/windows/path");
        p = p.suffix("/abc");
        assertEquals("/C:/my/windows/path/abc", p.toUri().getPath());
    }

    @Test
    public void testDepth() {

        Path p = new Path("/my/path");
        assertEquals(2, p.depth());

        p = new Path("/my/fancy/path/");
        assertEquals(3, p.depth());

        p = new Path("/my/fancy/fancy/fancy/fancy/fancy/fancy/fancy/fancy/fancy/fancy/path");
        assertEquals(12, p.depth());

        p = new Path("/");
        assertEquals(0, p.depth());

        p = new Path("C:/my/windows/path");
        assertEquals(4, p.depth());
    }

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
        // make relative path qualified
        String path = "test/test";
        Path p = new Path(path).makeQualified(FileSystem.getLocalFileSystem());
        URI u = p.toUri();

        assertEquals("file", u.getScheme());
        assertEquals(null, u.getAuthority());

        String q =
                new Path(FileSystem.getLocalFileSystem().getWorkingDirectory().getPath(), path)
                        .getPath();
        assertEquals(q, u.getPath());

        // make absolute path qualified
        path = "/test/test";
        p = new Path(path).makeQualified(FileSystem.getLocalFileSystem());
        u = p.toUri();
        assertEquals("file", u.getScheme());
        assertEquals(null, u.getAuthority());
        assertEquals(path, u.getPath());
    }
}
