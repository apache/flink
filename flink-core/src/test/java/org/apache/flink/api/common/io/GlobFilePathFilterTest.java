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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class GlobFilePathFilterTest {
    @Test
    public void testDefaultConstructorCreateMatchAllFilter() {
        GlobFilePathFilter matcher = new GlobFilePathFilter();
        assertThat(matcher.filterPath(new Path("dir/file.txt"))).isFalse();
    }

    @Test
    public void testMatchAllFilesByDefault() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.<String>emptyList(), Collections.<String>emptyList());

        assertThat(matcher.filterPath(new Path("dir/file.txt"))).isFalse();
    }

    @Test
    public void testExcludeFilesNotInIncludePatterns() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("dir/*"), Collections.<String>emptyList());

        assertThat(matcher.filterPath(new Path("dir/file.txt"))).isFalse();
        assertThat(matcher.filterPath(new Path("dir1/file.txt"))).isTrue();
    }

    @Test
    public void testExcludeFilesIfMatchesExclude() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("dir/*"),
                        Collections.singletonList("dir/file.txt"));

        assertThat(matcher.filterPath(new Path("dir/file.txt"))).isTrue();
    }

    @Test
    public void testIncludeFileWithAnyCharacterMatcher() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("dir/?.txt"), Collections.<String>emptyList());

        assertThat(matcher.filterPath(new Path("dir/a.txt"))).isFalse();
        assertThat(matcher.filterPath(new Path("dir/aa.txt"))).isTrue();
    }

    @Test
    public void testIncludeFileWithCharacterSetMatcher() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("dir/[acd].txt"),
                        Collections.<String>emptyList());

        assertThat(matcher.filterPath(new Path("dir/a.txt"))).isFalse();
        assertThat(matcher.filterPath(new Path("dir/c.txt"))).isFalse();
        assertThat(matcher.filterPath(new Path("dir/d.txt"))).isFalse();
        assertThat(matcher.filterPath(new Path("dir/z.txt"))).isTrue();
    }

    @Test
    public void testIncludeFileWithCharacterRangeMatcher() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("dir/[a-d].txt"),
                        Collections.<String>emptyList());

        assertThat(matcher.filterPath(new Path("dir/a.txt"))).isFalse();
        assertThat(matcher.filterPath(new Path("dir/b.txt"))).isFalse();
        assertThat(matcher.filterPath(new Path("dir/c.txt"))).isFalse();
        assertThat(matcher.filterPath(new Path("dir/d.txt"))).isFalse();
        assertThat(matcher.filterPath(new Path("dir/z.txt"))).isTrue();
    }

    @Test
    public void testExcludeHDFSFile() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("**"),
                        Collections.singletonList("/dir/file2.txt"));

        assertThat(matcher.filterPath(new Path("hdfs:///dir/file1.txt"))).isFalse();
        assertThat(matcher.filterPath(new Path("hdfs:///dir/file2.txt"))).isTrue();
        assertThat(matcher.filterPath(new Path("hdfs:///dir/file3.txt"))).isFalse();
    }

    @Test
    public void testExcludeFilenameWithStart() {
        assumeThat(OperatingSystem.isWindows())
                .as("Windows does not allow asterisks in file names.")
                .isFalse();

        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("**"), Collections.singletonList("\\*"));

        assertThat(matcher.filterPath(new Path("*"))).isTrue();
        assertThat(matcher.filterPath(new Path("**"))).isFalse();
        assertThat(matcher.filterPath(new Path("other.txt"))).isFalse();
    }

    @Test
    public void testSingleStarPattern() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("*"), Collections.<String>emptyList());

        assertThat(matcher.filterPath(new Path("a"))).isFalse();
        assertThat(matcher.filterPath(new Path("a/b"))).isTrue();
        assertThat(matcher.filterPath(new Path("a/b/c"))).isTrue();
    }

    @Test
    public void testDoubleStarPattern() {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("**"), Collections.<String>emptyList());

        assertThat(matcher.filterPath(new Path("a"))).isFalse();
        assertThat(matcher.filterPath(new Path("a/b"))).isFalse();
        assertThat(matcher.filterPath(new Path("a/b/c"))).isFalse();
    }

    @Test
    public void testIncluePatternIsNull() {
        Assertions.assertThatThrownBy(
                        () -> new GlobFilePathFilter(null, Collections.<String>emptyList()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testExcludePatternIsNull() {
        Assertions.assertThatThrownBy(
                        () -> new GlobFilePathFilter(Collections.singletonList("**"), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testGlobFilterSerializable() throws IOException {
        GlobFilePathFilter matcher =
                new GlobFilePathFilter(
                        Collections.singletonList("**"), Collections.<String>emptyList());

        GlobFilePathFilter matcherCopy = CommonTestUtils.createCopySerializable(matcher);

        assertThat(matcherCopy.filterPath(new Path("a"))).isFalse();
        assertThat(matcherCopy.filterPath(new Path("a/b"))).isFalse();
        assertThat(matcherCopy.filterPath(new Path("a/b/c"))).isFalse();
    }
}
