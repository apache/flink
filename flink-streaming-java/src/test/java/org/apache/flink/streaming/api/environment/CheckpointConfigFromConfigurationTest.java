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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.util.Preconditions;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for configuring {@link CheckpointConfig} via {@link
 * CheckpointConfig#configure(ReadableConfig)}.
 */
@RunWith(Parameterized.class)
public class CheckpointConfigFromConfigurationTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestSpec<?>> specs() {
        return Arrays.asList(
                TestSpec.testValue(CheckpointingMode.AT_LEAST_ONCE)
                        .whenSetFromFile("execution.checkpointing.mode", "AT_LEAST_ONCE")
                        .viaSetter(CheckpointConfig::setCheckpointingMode)
                        .getterVia(CheckpointConfig::getCheckpointingMode)
                        .nonDefaultValue(CheckpointingMode.AT_LEAST_ONCE),
                TestSpec.testValue(10000L)
                        .whenSetFromFile("execution.checkpointing.interval", "10 s")
                        .viaSetter(CheckpointConfig::setCheckpointInterval)
                        .getterVia(CheckpointConfig::getCheckpointInterval)
                        .nonDefaultValue(100L),
                TestSpec.testValue(12000L)
                        .whenSetFromFile("execution.checkpointing.timeout", "12 s")
                        .viaSetter(CheckpointConfig::setCheckpointTimeout)
                        .getterVia(CheckpointConfig::getCheckpointTimeout)
                        .nonDefaultValue(100L),
                TestSpec.testValue(12)
                        .whenSetFromFile("execution.checkpointing.max-concurrent-checkpoints", "12")
                        .viaSetter(CheckpointConfig::setMaxConcurrentCheckpoints)
                        .getterVia(CheckpointConfig::getMaxConcurrentCheckpoints)
                        .nonDefaultValue(100),
                TestSpec.testValue(1000L)
                        .whenSetFromFile("execution.checkpointing.min-pause", "1 s")
                        .viaSetter(CheckpointConfig::setMinPauseBetweenCheckpoints)
                        .getterVia(CheckpointConfig::getMinPauseBetweenCheckpoints)
                        .nonDefaultValue(100L),
                TestSpec.testValue(
                                CheckpointConfig.ExternalizedCheckpointCleanup
                                        .RETAIN_ON_CANCELLATION)
                        .whenSetFromFile(
                                "execution.checkpointing.externalized-checkpoint-retention",
                                "RETAIN_ON_CANCELLATION")
                        .viaSetter(CheckpointConfig::setExternalizedCheckpointCleanup)
                        .getterVia(CheckpointConfig::getExternalizedCheckpointCleanup)
                        .nonDefaultValue(
                                CheckpointConfig.ExternalizedCheckpointCleanup
                                        .DELETE_ON_CANCELLATION),
                TestSpec.testValue(12)
                        .whenSetFromFile(
                                "execution.checkpointing.tolerable-failed-checkpoints", "12")
                        .viaSetter(CheckpointConfig::setTolerableCheckpointFailureNumber)
                        .getterVia(CheckpointConfig::getTolerableCheckpointFailureNumber)
                        .nonDefaultValue(100),
                TestSpec.testValue(true)
                        .whenSetFromFile("execution.checkpointing.unaligned", "true")
                        .viaSetter(CheckpointConfig::enableUnalignedCheckpoints)
                        .getterVia(CheckpointConfig::isUnalignedCheckpointsEnabled)
                        .nonDefaultValue(true),
                TestSpec.testValue(
                                (CheckpointStorage)
                                        new FileSystemCheckpointStorage(
                                                "file:///path/to/checkpoint/dir"))
                        .whenSetFromFile(
                                CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(),
                                "file:///path/to/checkpoint/dir")
                        .viaSetter(CheckpointConfig::setCheckpointStorage)
                        .getterVia(CheckpointConfig::getCheckpointStorage)
                        .nonDefaultValue(
                                new FileSystemCheckpointStorage("file:///path/to/checkpoint/dir"))
                        .customMatcher(FileSystemCheckpointStorageMatcher::new));
    }

    /**
     * {@code FileSystemCheckpointStorageMatcher} verifies that the set {@link CheckpointStorage} is
     * of type {@link FileSystemCheckpointStorage} pointing to the same filesystem path.
     */
    private static class FileSystemCheckpointStorageMatcher
            extends TypeSafeMatcher<CheckpointStorage> {
        private static final Class<FileSystemCheckpointStorage> EXPECTED_CHECKPOINT_STORAGE_CLASS =
                FileSystemCheckpointStorage.class;
        private final FileSystemCheckpointStorage fileSystemCheckpointStorageFromSetter;

        public FileSystemCheckpointStorageMatcher(CheckpointStorage checkpointStorageFromSetter) {
            Preconditions.checkArgument(
                    checkpointStorageFromSetter
                            .getClass()
                            .equals(EXPECTED_CHECKPOINT_STORAGE_CLASS));
            this.fileSystemCheckpointStorageFromSetter =
                    (FileSystemCheckpointStorage) checkpointStorageFromSetter;
        }

        @Override
        public void describeTo(Description description) {
            describeObject(fileSystemCheckpointStorageFromSetter, description);
        }

        @Override
        protected void describeMismatchSafely(
                CheckpointStorage checkpointStorageFromFile, Description description) {
            if (!checkpointStorageFromFile.getClass().equals(EXPECTED_CHECKPOINT_STORAGE_CLASS)) {
                description.appendText(
                        "Passed object is not of type "
                                + EXPECTED_CHECKPOINT_STORAGE_CLASS.getCanonicalName());
            } else {
                describeObject(
                        (FileSystemCheckpointStorage) checkpointStorageFromFile, description);
            }
            super.describeMismatchSafely(checkpointStorageFromFile, description);
        }

        private static void describeObject(
                FileSystemCheckpointStorage fileSystemCheckpointStorage, Description description) {
            description
                    .appendText(EXPECTED_CHECKPOINT_STORAGE_CLASS.getCanonicalName())
                    .appendText("(")
                    .appendText(fileSystemCheckpointStorage.getCheckpointPath().toString())
                    .appendText(")");
        }

        @Override
        protected boolean matchesSafely(CheckpointStorage checkpointStorageFromFile) {
            if (!checkpointStorageFromFile.getClass().equals(EXPECTED_CHECKPOINT_STORAGE_CLASS)) {
                return false;
            }
            final FileSystemCheckpointStorage fileSystemCheckpointStorageFromFile =
                    (FileSystemCheckpointStorage) checkpointStorageFromFile;
            return fileSystemCheckpointStorageFromFile
                    .getCheckpointPath()
                    .equals(fileSystemCheckpointStorageFromSetter.getCheckpointPath());
        }
    }

    @Parameterized.Parameter public TestSpec<?> spec;

    @Test
    public void testLoadingFromConfiguration() {
        CheckpointConfig configFromSetters = new CheckpointConfig();
        CheckpointConfig configFromFile = new CheckpointConfig();

        Configuration configuration = new Configuration();
        configuration.setString(spec.key, spec.value);
        configFromFile.configure(configuration);

        spec.setValue(configFromSetters);
        spec.assertEqual(configFromFile, configFromSetters);
    }

    @Test
    public void testNotOverridingIfNotSet() {
        CheckpointConfig config = new CheckpointConfig();

        spec.setNonDefaultValue(config);
        Configuration configuration = new Configuration();
        config.configure(configuration);

        spec.assertEqualNonDefault(config);
    }

    private static class TestSpec<T> {
        private String key;
        private String value;
        private final T objectValue;
        private T nonDefaultValue;
        private BiConsumer<CheckpointConfig, T> setter;
        private Function<CheckpointConfig, T> getter;

        private Function<T, Matcher<T>> createMatcher = CoreMatchers::equalTo;

        private TestSpec(T value) {
            this.objectValue = value;
        }

        public static <T> TestSpec<T> testValue(T value) {
            return new TestSpec<>(value);
        }

        public TestSpec<T> whenSetFromFile(String key, String value) {
            this.key = key;
            this.value = value;
            return this;
        }

        public TestSpec<T> viaSetter(BiConsumer<CheckpointConfig, T> setter) {
            this.setter = setter;
            return this;
        }

        public TestSpec<T> getterVia(Function<CheckpointConfig, T> getter) {
            this.getter = getter;
            return this;
        }

        public TestSpec<T> nonDefaultValue(T nonDefaultValue) {
            this.nonDefaultValue = nonDefaultValue;
            return this;
        }

        public TestSpec<T> customMatcher(Function<T, Matcher<T>> customMatcher) {
            this.createMatcher = customMatcher;
            return this;
        }

        public void setValue(CheckpointConfig config) {
            setter.accept(config, objectValue);
        }

        public void setNonDefaultValue(CheckpointConfig config) {
            setter.accept(config, nonDefaultValue);
        }

        public void assertEqual(
                CheckpointConfig configFromFile, CheckpointConfig configFromSetters) {
            assertThat(
                    getter.apply(configFromFile),
                    createMatcher.apply(getter.apply(configFromSetters)));
        }

        public void assertEqualNonDefault(CheckpointConfig configFromFile) {
            assertThat(getter.apply(configFromFile), createMatcher.apply(nonDefaultValue));
        }

        @Override
        public String toString() {
            return "key='" + key + '\'';
        }
    }
}
