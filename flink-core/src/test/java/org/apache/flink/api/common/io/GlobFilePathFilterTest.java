/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.io;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.OperatingSystem;

import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GlobFilePathFilterTest {
    @Test
    public void testDefaultConstructorCreateMatchAllFilter() {
        GlobFilePathFilter matcher = new GlobFilePathFilter();
        assertFalse(matcher.filterPath(new Path("dir/file.txt")));
    }

    @Test
    public void testMatchAllFilesByDefault() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.<String>emptyList(), Collections.<String>emptyList());

        assertFalse(matcher.filterPath(new Path("dir/file.txt")));
    }

    @Test
    public void testExcludeFilesNotInIncludePatterns() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("dir/*"), Collections.<String>emptyList());

        assertFalse(matcher.filterPath(new Path("dir/file.txt")));
        assertTrue(matcher.filterPath(new Path("dir1/file.txt")));
    }

    @Test
    public void testExcludeFilesIfMatchesExclude() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("dir/*"),
                        Collections.singletonList("dir/file.txt"));

        assertTrue(matcher.filterPath(new Path("dir/file.txt")));
    }

    @Test
    public void testIncludeFileWithAnyCharacterMatcher() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("dir/?.txt"), Collections.<String>emptyList());

        assertFalse(matcher.filterPath(new Path("dir/a.txt")));
        assertTrue(matcher.filterPath(new Path("dir/aa.txt")));
    }

    @Test
    public void testIncludeFileWithCharacterSetMatcher() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("dir/[acd].txt"),
                        Collections.<String>emptyList());

        assertFalse(matcher.filterPath(new Path("dir/a.txt")));
        assertFalse(matcher.filterPath(new Path("dir/c.txt")));
        assertFalse(matcher.filterPath(new Path("dir/d.txt")));
        assertTrue(matcher.filterPath(new Path("dir/z.txt")));
    }

    @Test
    public void testIncludeFileWithCharacterRangeMatcher() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("dir/[a-d].txt"),
                        Collections.<String>emptyList());

        assertFalse(matcher.filterPath(new Path("dir/a.txt")));
        assertFalse(matcher.filterPath(new Path("dir/b.txt")));
        assertFalse(matcher.filterPath(new Path("dir/c.txt")));
        assertFalse(matcher.filterPath(new Path("dir/d.txt")));
        assertTrue(matcher.filterPath(new Path("dir/z.txt")));
    }

    @Test
    public void testExcludeHDFSFile() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("**"),
                        Collections.singletonList("/dir/file2.txt"));

        assertFalse(matcher.filterPath(new Path("hdfs:///dir/file1.txt")));
        assertTrue(matcher.filterPath(new Path("hdfs:///dir/file2.txt")));
        assertFalse(matcher.filterPath(new Path("hdfs:///dir/file3.txt")));
    }

    @Test
    public void testExcludeFilenameWithStart() {
        Assume.assumeTrue(
                "Windows does not allow asterisks in file names.", !OperatingSystem.isWindows());

        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("**"), Collections.singletonList("\\*"));

        assertTrue(matcher.filterPath(new Path("*")));
        assertFalse(matcher.filterPath(new Path("**")));
        assertFalse(matcher.filterPath(new Path("other.txt")));
    }

    @Test
    public void testSingleStarPattern() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("*"), Collections.<String>emptyList());

        assertFalse(matcher.filterPath(new Path("a")));
        assertTrue(matcher.filterPath(new Path("a/b")));
        assertTrue(matcher.filterPath(new Path("a/b/c")));
    }

    @Test
    public void testDoubleStarPattern() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("**"), Collections.<String>emptyList());

        assertFalse(matcher.filterPath(new Path("a")));
        assertFalse(matcher.filterPath(new Path("a/b")));
        assertFalse(matcher.filterPath(new Path("a/b/c")));
    }

    @Test(expected = NullPointerException.class)
    public void testIncluePatternIsNull() {
        new GlobFilePathFilter(null, Collections.<String>emptyList());
    }

    @Test(expected = NullPointerException.class)
    public void testExcludePatternIsNull() {
        new GlobFilePathFilter(Collections.singletonList("**"), null);
    }

    @Test
    public void testGlobFilterSerializable() throws IOException {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("**"), Collections.<String>emptyList());

        GlobFilePathFilter matcherCopy = CommonTestUtils.createCopySerializable(matcher);
        assertFalse(matcher.filterPath(new Path("a")));
        assertFalse(matcher.filterPath(new Path("a/b")));
        assertFalse(matcher.filterPath(new Path("a/b/c")));
    }
}
