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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.TernaryBoolean;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Executors;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.EXPONENTIAL_DELAY;
import static org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for configuring {@link StreamExecutionEnvironment} via {@link
 * StreamExecutionEnvironment#configure(ReadableConfig, ClassLoader)} with complex types.
 *
 * @see StreamExecutionEnvironmentConfigurationTest
 */
class StreamExecutionEnvironmentComplexConfigurationTest {
    @Test
    void testJobConfigFromEnvToExecutionGraph() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        String path = "file:///valid";
        configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");
        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, path);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, EXPONENTIAL_DELAY.getMainValue());

        // mutate config according to configuration
        env.configure(configuration, Thread.currentThread().getContextClassLoader());
        env.fromSequence(0, 1).addSink(new DiscardingSink<>());

        DefaultScheduler scheduler =
                new DefaultSchedulerBuilder(
                                env.getStreamGraph().getJobGraph(),
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                Executors.newSingleThreadScheduledExecutor())
                        .build();
        Configuration jobConfiguration = scheduler.getExecutionGraph().getJobConfiguration();

        assertThat(jobConfiguration.get(StateBackendOptions.STATE_BACKEND))
                .isEqualTo(configuration.get(StateBackendOptions.STATE_BACKEND));
        assertThat(jobConfiguration.get(CheckpointingOptions.CHECKPOINT_STORAGE))
                .isEqualTo(configuration.get(CheckpointingOptions.CHECKPOINT_STORAGE));
        assertThat(jobConfiguration.get(RestartStrategyOptions.RESTART_STRATEGY))
                .isEqualTo(configuration.get(RestartStrategyOptions.RESTART_STRATEGY));
    }

    @Test
    void testLoadingCachedFilesFromConfiguration() {
        StreamExecutionEnvironment envFromConfiguration =
                StreamExecutionEnvironment.getExecutionEnvironment();
        envFromConfiguration.registerCachedFile("/tmp4", "file4", true);

        Configuration configuration = new Configuration();
        configuration.setString(
                "pipeline.cached-files",
                "name:file1,path:/tmp1,executable:true;"
                        + "name:file2,path:/tmp2;"
                        + "name:file3,path:'oss://bucket/file1'");

        // mutate config according to configuration
        envFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());

        assertThat(envFromConfiguration.getCachedFiles())
                .isEqualTo(
                        Arrays.asList(
                                Tuple2.of(
                                        "file1",
                                        new DistributedCache.DistributedCacheEntry("/tmp1", true)),
                                Tuple2.of(
                                        "file2",
                                        new DistributedCache.DistributedCacheEntry("/tmp2", false)),
                                Tuple2.of(
                                        "file3",
                                        new DistributedCache.DistributedCacheEntry(
                                                "oss://bucket/file1", false))));
    }

    @Test
    void testLoadingKryoSerializersFromConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(
                "pipeline.default-kryo-serializers",
                "class:'org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentComplexConfigurationTest$CustomPojo'"
                        + ",serializer:'org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentComplexConfigurationTest$CustomPojoSerializer'");

        // mutate config according to configuration
        StreamExecutionEnvironment envFromConfiguration =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        LinkedHashMap<Object, Object> serializers = new LinkedHashMap<>();
        serializers.put(CustomPojo.class, CustomPojoSerializer.class);
        assertThat(
                        envFromConfiguration
                                .getConfig()
                                .getSerializerConfig()
                                .getDefaultKryoSerializerClasses())
                .isEqualTo(serializers);
    }

    @Test
    void testNotOverridingStateBackendWithDefaultsFromConfiguration() {
        StreamExecutionEnvironment envFromConfiguration =
                StreamExecutionEnvironment.getExecutionEnvironment();
        envFromConfiguration.setStateBackend(new MemoryStateBackend());

        // mutate config according to configuration
        envFromConfiguration.configure(
                new Configuration(), Thread.currentThread().getContextClassLoader());

        StateBackend actualStateBackend = envFromConfiguration.getStateBackend();
        assertThat(actualStateBackend).isInstanceOf(MemoryStateBackend.class);
    }

    @Test
    void testOverridingChangelogStateBackendWithFromConfigurationWhenSet() {
        StreamExecutionEnvironment envFromConfiguration =
                StreamExecutionEnvironment.getExecutionEnvironment();
        assertThat(TernaryBoolean.UNDEFINED)
                .isEqualTo(envFromConfiguration.isChangelogStateBackendEnabled());

        Configuration configuration = new Configuration();
        configuration.set(ENABLE_STATE_CHANGE_LOG, true);
        envFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());
        assertThat(TernaryBoolean.TRUE)
                .isEqualTo(envFromConfiguration.isChangelogStateBackendEnabled());

        envFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());
        assertThat(TernaryBoolean.TRUE)
                .isEqualTo(envFromConfiguration.isChangelogStateBackendEnabled());

        configuration.set(ENABLE_STATE_CHANGE_LOG, false);
        envFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());
        assertThat(TernaryBoolean.FALSE)
                .isEqualTo(envFromConfiguration.isChangelogStateBackendEnabled());
    }

    @Test
    void testNotOverridingCachedFilesFromConfiguration() {
        StreamExecutionEnvironment envFromConfiguration =
                StreamExecutionEnvironment.getExecutionEnvironment();
        envFromConfiguration.registerCachedFile("/tmp3", "file3", true);

        Configuration configuration = new Configuration();

        // mutate config according to configuration
        envFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());

        assertThat(envFromConfiguration.getCachedFiles())
                .isEqualTo(
                        Arrays.asList(
                                Tuple2.of(
                                        "file3",
                                        new DistributedCache.DistributedCacheEntry(
                                                "/tmp3", true))));
    }

    @Test
    void testLoadingListenersFromConfiguration() {
        StreamExecutionEnvironment envFromConfiguration =
                StreamExecutionEnvironment.getExecutionEnvironment();
        List<Class> listenersClass =
                Arrays.asList(BasicJobSubmittedCounter.class, BasicJobExecutedCounter.class);

        Configuration configuration = new Configuration();
        ConfigUtils.encodeCollectionToConfig(
                configuration, DeploymentOptions.JOB_LISTENERS, listenersClass, Class::getName);

        // mutate config according to configuration
        envFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());

        assertThat(envFromConfiguration.getJobListeners().size()).isEqualTo(2);
        assertThat(envFromConfiguration.getJobListeners().get(0))
                .isInstanceOf(BasicJobSubmittedCounter.class);
        assertThat(envFromConfiguration.getJobListeners().get(1))
                .isInstanceOf(BasicJobExecutedCounter.class);
    }

    @Test
    void testGettingEnvironmentWithConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 10);
        configuration.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(100));

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        assertThat(env.getParallelism()).isEqualTo(10);
        assertThat(env.getConfig().getAutoWatermarkInterval()).isEqualTo(100L);
    }

    @Test
    void testLocalEnvironmentExplicitParallelism() {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 10);
        configuration.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(100));

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2, configuration);

        assertThat(env.getParallelism()).isEqualTo(2);
        assertThat(env.getConfig().getAutoWatermarkInterval()).isEqualTo(100L);
    }

    /** JobSubmitted counter listener for unit test. */
    public static class BasicJobSubmittedCounter implements JobListener {
        private int count = 0;

        @Override
        public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
            this.count = this.count + 1;
        }

        @Override
        public void onJobExecuted(
                @Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {}
    }

    /** JobExecuted counter listener for unit test. */
    public static class BasicJobExecutedCounter implements JobListener {
        private int count = 0;

        @Override
        public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
            this.count = this.count + 1;
        }

        @Override
        public void onJobExecuted(
                @Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {}
    }

    /** A dummy class to specify a Kryo serializer for. */
    public static class CustomPojo {}

    /** A dummy Kryo serializer which can be registered. */
    public static class CustomPojoSerializer extends Serializer<CustomPojo> {
        @Override
        public void write(Kryo kryo, Output output, CustomPojo object) {}

        @Override
        public CustomPojo read(Kryo kryo, Input input, Class<CustomPojo> type) {
            return null;
        }
    }
}
