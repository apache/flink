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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link Path} class. */
class PathTest {

    @Test
    void testPathFromString() {

        Path p = new Path("/my/path");
        assertThat(p.toUri().getPath()).isEqualTo("/my/path");
        assertThat(p.toUri().getScheme()).isNull();

        p = new Path("/my/path/");
        assertThat(p.toUri().getPath()).isEqualTo("/my/path");
        assertThat(p.toUri().getScheme()).isNull();

        p = new Path("/my//path/");
        assertThat(p.toUri().getPath()).isEqualTo("/my/path");
        assertThat(p.toUri().getScheme()).isNull();

        p = new Path("/my//path//a///");
        assertThat(p.toUri().getPath()).isEqualTo("/my/path/a");
        assertThat(p.toUri().getScheme()).isNull();

        p = new Path("\\my\\path\\\\a\\\\\\");
        assertThat(p.toUri().getPath()).isEqualTo("/my/path/a");
        assertThat(p.toUri().getScheme()).isNull();

        p = new Path("hdfs:///my/path");
        assertThat(p.toUri().getPath()).isEqualTo("/my/path");
        assertThat(p.toUri().getScheme()).isEqualTo("hdfs");

        p = new Path("hdfs:///my/path/");
        assertThat(p.toUri().getPath()).isEqualTo("/my/path");
        assertThat(p.toUri().getScheme()).isEqualTo("hdfs");

        p = new Path("file:///my/path");
        assertThat(p.toUri().getPath()).isEqualTo("/my/path");
        assertThat(p.toUri().getScheme()).isEqualTo("file");

        p = new Path("C:/my/windows/path");
        assertThat(p.toUri().getPath()).isEqualTo("/C:/my/windows/path");

        p = new Path("file:/C:/my/windows/path");
        assertThat(p.toUri().getPath()).isEqualTo("/C:/my/windows/path");

        assertThatThrownBy(() -> new Path((String) null)).isInstanceOf(Exception.class);

        assertThatThrownBy(() -> new Path("")).isInstanceOf(Exception.class);
    }

    @Test
    void testIsAbsolute() {

        // UNIX

        Path p = new Path("/my/abs/path");
        assertThat(p.isAbsolute()).isTrue();

        p = new Path("/");
        assertThat(p.isAbsolute()).isTrue();

        p = new Path("./my/rel/path");
        assertThat(p.isAbsolute()).isFalse();

        p = new Path("my/rel/path");
        assertThat(p.isAbsolute()).isFalse();

        // WINDOWS

        p = new Path("C:/my/abs/windows/path");
        assertThat(p.isAbsolute()).isTrue();

        p = new Path("y:/my/abs/windows/path");
        assertThat(p.isAbsolute()).isTrue();

        p = new Path("/y:/my/abs/windows/path");
        assertThat(p.isAbsolute()).isTrue();

        p = new Path("b:\\my\\abs\\windows\\path");
        assertThat(p.isAbsolute()).isTrue();

        p = new Path("/c:/my/dir");
        assertThat(p.isAbsolute()).isTrue();

        p = new Path("/C:/");
        assertThat(p.isAbsolute()).isTrue();

        p = new Path("C:");
        assertThat(p.isAbsolute()).isFalse();

        p = new Path("C:/");
        assertThat(p.isAbsolute()).isTrue();

        p = new Path("C:my\\relative\\path");
        assertThat(p.isAbsolute()).isFalse();

        p = new Path("\\my\\dir");
        assertThat(p.isAbsolute()).isTrue();

        p = new Path("\\");
        assertThat(p.isAbsolute()).isTrue();

        p = new Path(".\\my\\relative\\path");
        assertThat(p.isAbsolute()).isFalse();

        p = new Path("my\\relative\\path");
        assertThat(p.isAbsolute()).isFalse();

        p = new Path("\\\\myServer\\myDir");
        assertThat(p.isAbsolute()).isTrue();
    }

    @Test
    void testGetName() {

        Path p = new Path("/my/fancy/path");
        assertThat(p.getName()).isEqualTo("path");

        p = new Path("/my/fancy/path/");
        assertThat(p.getName()).isEqualTo("path");

        p = new Path("hdfs:///my/path");
        assertThat(p.getName()).isEqualTo("path");

        p = new Path("hdfs:///myPath/");
        assertThat(p.getName()).isEqualTo("myPath");

        p = new Path("/");
        assertThat(p.getName()).isEmpty();

        p = new Path("C:/my/windows/path");
        assertThat(p.getName()).isEqualTo("path");

        p = new Path("file:/C:/my/windows/path");
        assertThat(p.getName()).isEqualTo("path");
    }

    @Test
    void testGetParent() {

        Path p = new Path("/my/fancy/path");
        assertThat(p.getParent().toUri().getPath()).isEqualTo("/my/fancy");

        p = new Path("/my/other/fancy/path/");
        assertThat(p.getParent().toUri().getPath()).isEqualTo("/my/other/fancy");

        p = new Path("hdfs:///my/path");
        assertThat(p.getParent().toUri().getPath()).isEqualTo("/my");

        p = new Path("hdfs:///myPath/");
        assertThat(p.getParent().toUri().getPath()).isEqualTo("/");

        p = new Path("/");
        assertThat(p.getParent()).isNull();

        p = new Path("C:/my/windows/path");
        assertThat(p.getParent().toUri().getPath()).isEqualTo("/C:/my/windows");
    }

    @Test
    void testSuffix() {

        Path p = new Path("/my/path");
        p = p.suffix("_123");
        assertThat(p.toUri().getPath()).isEqualTo("/my/path_123");

        p = new Path("/my/path/");
        p = p.suffix("/abc");
        assertThat(p.toUri().getPath()).isEqualTo("/my/path/abc");

        p = new Path("C:/my/windows/path");
        p = p.suffix("/abc");
        assertThat(p.toUri().getPath()).isEqualTo("/C:/my/windows/path/abc");
    }

    @Test
    void testDepth() {

        Path p = new Path("/my/path");
        assertThat(p.depth()).isEqualTo(2);

        p = new Path("/my/fancy/path/");
        assertThat(p.depth()).isEqualTo(3);

        p = new Path("/my/fancy/fancy/fancy/fancy/fancy/fancy/fancy/fancy/fancy/fancy/path");
        assertThat(p.depth()).isEqualTo(12);

        p = new Path("/");
        assertThat(p.depth()).isZero();

        p = new Path("C:/my/windows/path");
        assertThat(p.depth()).isEqualTo(4);
    }

    @Test
    void testParsing() {
        URI u;
        String scheme = "hdfs";
        String authority = "localhost:8000";
        String path = "/test/test";

        // correct usage
        // hdfs://localhost:8000/test/test
        u = new Path(scheme + "://" + authority + path).toUri();
        assertThat(u.getScheme()).isEqualTo(scheme);
        assertThat(u.getAuthority()).isEqualTo(authority);
        assertThat(u.getPath()).isEqualTo(path);
        // hdfs:///test/test
        u = new Path(scheme + "://" + path).toUri();
        assertThat(u.getScheme()).isEqualTo(scheme);
        assertThat(u.getAuthority()).isNull();
        assertThat(u.getPath()).isEqualTo(path);
        // hdfs:/test/test
        u = new Path(scheme + ":" + path).toUri();
        assertThat(u.getScheme()).isEqualTo(scheme);
        assertThat(u.getAuthority()).isNull();
        assertThat(u.getPath()).isEqualTo(path);

        // incorrect usage
        // hdfs://test/test
        u = new Path(scheme + ":/" + path).toUri();
        assertThat(u.getScheme()).isEqualTo(scheme);
        assertThat(u.getAuthority()).isEqualTo("test");
        assertThat(u.getPath()).isEqualTo("/test");
        // hdfs:////test/test
        u = new Path(scheme + ":///" + path).toUri();
        assertThat(u.getScheme()).isEqualTo("hdfs");
        assertThat(u.getAuthority()).isNull();
        assertThat(u.getPath()).isEqualTo(path);
    }

    @Test
    void testMakeQualified() throws IOException {
        // make relative path qualified
        String path = "test/test";
        Path p = new Path(path).makeQualified(FileSystem.getLocalFileSystem());
        URI u = p.toUri();

        assertThat(u.getScheme()).isEqualTo("file");
        assertThat(u.getAuthority()).isNull();

        String q =
                new Path(FileSystem.getLocalFileSystem().getWorkingDirectory().getPath(), path)
                        .getPath();
        assertThat(u.getPath()).isEqualTo(q);

        // make absolute path qualified
        path = "/test/test";
        p = new Path(path).makeQualified(FileSystem.getLocalFileSystem());
        u = p.toUri();
        assertThat(u.getScheme()).isEqualTo("file");
        assertThat(u.getAuthority()).isNull();
        assertThat(u.getPath()).isEqualTo(path);
    }
}
