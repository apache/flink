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
package org.apache.flink.api.common.io;

import org.apache.flink.core.fs.Path;

import lombok.Getter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultFilterTest {
    public static List<DefaultFilterTestData> data() {
        return Arrays.asList(
                new DefaultFilterTestData("file.txt", false),
                new DefaultFilterTestData(".file.txt", true),
                new DefaultFilterTestData("dir/.file.txt", true),
                new DefaultFilterTestData(".dir/file.txt", false),
                new DefaultFilterTestData("_file.txt", true),
                new DefaultFilterTestData("dir/_file.txt", true),
                new DefaultFilterTestData("_dir/file.txt", false),
                new DefaultFilterTestData(FilePathFilter.HADOOP_COPYING, true),
                new DefaultFilterTestData("dir/" + FilePathFilter.HADOOP_COPYING, true),
                new DefaultFilterTestData(FilePathFilter.HADOOP_COPYING + "/file.txt", false));
    }

    @Getter
    private static class DefaultFilterTestData {
        private final boolean shouldFilter;
        private final String filePath;

        public DefaultFilterTestData(String filePath, boolean shouldFilter) {
            this.filePath = filePath;
            this.shouldFilter = shouldFilter;
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void test(DefaultFilterTestData defaultFilterTestData) {
        FilePathFilter defaultFilter = FilePathFilter.createDefaultFilter();
        Path path = new Path(String.valueOf(defaultFilterTestData.getFilePath()));
        assertThat(defaultFilter.filterPath(path))
                .isEqualTo(defaultFilterTestData.isShouldFilter());
    }
}
