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

package org.apache.flink.connector.file.src.enumerate;

import org.apache.flink.connector.file.src.testutils.TestingFileSystem.TestFileStatus;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;
import java.util.regex.PatternSyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the glob grammar without accessing a filesystem or the host NIO glob implementation. */
class GlobPatternTest {

    @Test
    @Timeout(10)
    void testRepeatedStarsDoNotCauseExponentialBacktracking() {
        assertMatches("*a".repeat(32) + "b", "a".repeat(128), false);
        assertMatches("*a".repeat(32) + "b", "a".repeat(128) + "b", true);
    }

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            value = {
                "*|plain|true",
                "*.csv|report.csv|true",
                "*.csv|REPORT.CSV|false",
                "a?c|abc|true",
                "a?c|ac|false",
                "a?c|abbc|false",
                "[ab].csv|a.csv|true",
                "[a-c].csv|b.csv|true",
                "[!a-c].csv|d.csv|true",
                "[!a-c].csv|b.csv|false",
                "[^a].csv|^.csv|true",
                "[^a].csv|b.csv|false",
                "[*].csv|*.csv|true",
                "[?].csv|?.csv|true",
                "[[].csv|[.csv|true",
                "[]].csv|].csv|true",
                "[!]].csv|a.csv|true",
                "[!]].csv|].csv|false",
                "[-a].csv|-.csv|true",
                "[a-].csv|-.csv|true",
                "[a&b].csv|&.csv|true",
                "{a,b}*.csv|{a,b}1.csv|true",
                "{a,b}*.csv|a1.csv|false",
                "*.csv|文件.csv|true",
                "?*.csv|😀.csv|true",
                "😀*.csv|😀1.csv|true",
                "[😀].csv|😀.csv|true"
            })
    void testSegmentMatching(String pattern, String name, boolean matches) {
        assertMatches(pattern, name, matches);
    }

    @ParameterizedTest
    @ValueSource(strings = {"*.csv", "line?.csv", "[!a]*.csv"})
    void testNewlineIsACharacter(String pattern) {
        assertMatches(pattern, "line\n.csv", true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"[", "[]", "[!]", "[a", "[z-a]", "*[unterminated"})
    void testMalformedClass(String pattern) {
        assertThatThrownBy(() -> new GlobPattern(new Path("/root/" + pattern)))
                .isInstanceOf(PatternSyntaxException.class);
    }

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            value = {
                "/root/part-*/*.csv|/root",
                "/root/{a,b}/part-*/*.csv|/root/{a,b}",
                "/root/*.csv|/root",
                "part-*/*.csv|.",
                "*.csv|.",
                "/root/plain.csv|/root/plain.csv",
                ".|.",
                "hdfs://namenode|hdfs://namenode",
                "s3://bucket|s3://bucket",
                "/|/"
            })
    void testFixedPrefix(String pattern, String root) {
        assertThat(new GlobPattern(new Path(pattern)).getRoot()).isEqualTo(new Path(root));
    }

    private static void assertMatches(String pattern, String name, boolean expected) {
        final GlobPattern glob = new GlobPattern(new Path("/root/" + pattern));
        final Set<Integer> positions =
                glob.matchChild(
                        TestFileStatus.forFileWithDefaultBlock(new Path("/root/" + name), 1),
                        glob.initialPositions());
        assertThat(glob.isComplete(positions)).isEqualTo(expected);
    }
}
