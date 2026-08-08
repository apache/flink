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

package org.apache.flink.fs.s3native.metrics;

import org.apache.flink.fs.s3native.NativeS3AFileSystemFactory;
import org.apache.flink.fs.s3native.NativeS3FileSystemFactory;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the native S3 factories register metrics under a {@code filesystem_type} label whose
 * value is the factory scheme, so {@code s3://} and {@code s3a://} traffic remain distinguishable.
 */
class NativeS3FileSystemFactoryMetricsTest {

    @ParameterizedTest(name = "filesystem_type={1}")
    @MethodSource("factories")
    void factoryUsesSchemeAsFilesystemTypeLabel(
            NativeS3FileSystemFactory factory, String expectedScheme) {
        RecordingGroup group = new RecordingGroup();
        factory.setMetricGroup(group);
        assertThat(group.keyedGroups).containsEntry("filesystem_type", expectedScheme);
    }

    private static Stream<Arguments> factories() {
        return Stream.of(
                Arguments.of(new NativeS3FileSystemFactory(), "s3"),
                Arguments.of(new NativeS3AFileSystemFactory(), "s3a"));
    }

    @Test
    void repeatedAttachmentWithSameGroupDoesNotCreateNewFilesystemTypeGroup() {
        RecordingGroup group = new RecordingGroup();
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();

        factory.setMetricGroup(group);
        factory.setMetricGroup(group);

        assertThat(group.addGroupCalls).isEqualTo(1);
    }

    private static final class RecordingGroup extends UnregisteredMetricsGroup {
        final Map<String, String> keyedGroups = new HashMap<>();
        int addGroupCalls;

        @Override
        public MetricGroup addGroup(String key, String value) {
            addGroupCalls++;
            keyedGroups.put(key, value);
            return this;
        }
    }
}
