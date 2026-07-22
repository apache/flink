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

package org.apache.flink.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MdcOptions;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.MdcUtils.MdcCloseable;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.logging.log4j.core.LogEvent;
import org.assertj.core.api.AbstractObjectAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.util.MdcUtils.asContextData;
import static org.apache.flink.util.MdcUtils.wrapCallable;
import static org.apache.flink.util.MdcUtils.wrapRunnable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.slf4j.event.Level.DEBUG;

/** Tests for the {@link MdcUtils}. */
class MdcUtilsTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MdcUtilsTest.class);
    private static final Runnable LOGGING_RUNNABLE = () -> LOGGER.info("ignore");

    @RegisterExtension
    public final LoggerAuditingExtension loggerExtension =
            new LoggerAuditingExtension(MdcUtilsTest.class, DEBUG);

    @BeforeEach
    @AfterEach
    void clearMdcAndRegistry() {
        MDC.clear();
        JobMdcRegistry.clear();
    }

    @Test
    void testJobIDAsContext() {
        JobID jobID = new JobID();
        assertThat(MdcUtils.asContextData(jobID))
                .isEqualTo(Collections.singletonMap(MdcUtils.JOB_ID, jobID.toHexString()));
    }

    private static Stream<Arguments> wrappingMechanisms() {
        return Stream.of(
                Arguments.of(
                        "MdcCloseable",
                        (ThrowingConsumer<JobID, Exception>)
                                jobID -> {
                                    try (MdcCloseable ignored =
                                            MdcUtils.withContext(asContextData(jobID))) {
                                        LOGGER.warn("ignore");
                                    }
                                }),
                Arguments.of(
                        "wrapRunnable",
                        (ThrowingConsumer<JobID, Exception>)
                                jobID ->
                                        wrapRunnable(asContextData(jobID), LOGGING_RUNNABLE).run()),
                Arguments.of(
                        "wrapCallable",
                        (ThrowingConsumer<JobID, Exception>)
                                jobID ->
                                        wrapCallable(
                                                        asContextData(jobID),
                                                        () -> {
                                                            LOGGER.info("ignore");
                                                            return null;
                                                        })
                                                .call()),
                Arguments.of(
                        "scopeToJob(Executor)",
                        (ThrowingConsumer<JobID, Exception>)
                                jobID ->
                                        MdcUtils.scopeToJob(jobID, Executors.directExecutor())
                                                .execute(LOGGING_RUNNABLE)),
                Arguments.of(
                        "scopeToJob(ExecutorService)",
                        (ThrowingConsumer<JobID, Exception>)
                                jobID ->
                                        MdcUtils.scopeToJob(
                                                        jobID, Executors.newDirectExecutorService())
                                                .submit(LOGGING_RUNNABLE)
                                                .get()));
    }

    @ParameterizedTest
    @MethodSource("wrappingMechanisms")
    void testJobIdLoggedByWrappingMechanism(
            String scenario, ThrowingConsumer<JobID, Exception> action) throws Exception {
        assertJobIDLogged(jobID -> action.accept(jobID));
    }

    @Test
    void testMdcCloseableRemovesJobId() {
        JobID jobID = new JobID();
        try (MdcCloseable ignored = MdcUtils.withContext(asContextData(jobID))) {
            // ...
        }
        LOGGER.warn("with-job");
        assertJobIdLogged(null);
    }

    @Test
    void testScopeScheduledExecutorService() throws Exception {
        ScheduledExecutorService ses =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        try {
            assertJobIDLogged(
                    jobID ->
                            MdcUtils.scopeToJob(jobID, ses)
                                    .schedule(LOGGING_RUNNABLE, 1L, TimeUnit.MILLISECONDS)
                                    .get());
        } finally {
            ses.shutdownNow();
        }
    }

    // --- asContextData(JobID, Configuration): map-based extraction ---

    private static Stream<Arguments> configurationBranches() {
        return Stream.of(
                // both keys present
                Arguments.of(
                        Map.of("job.key-1", "mdc-key-1", "job.key-2", "mdc-key-2"),
                        "val-1",
                        "val-2",
                        3),
                // only key-1
                Arguments.of(Map.of("job.key-1", "mdc-key-1"), "val-1", null, 2),
                // only key-2
                Arguments.of(Map.of("job.key-2", "mdc-key-2"), null, "val-2", 2),
                // empty map → only job-id
                Arguments.of(Collections.emptyMap(), null, null, 1));
    }

    @ParameterizedTest
    @MethodSource("configurationBranches")
    void testContextEntriesExtractedFromConfiguration(
            final Map<String, String> keyMapping,
            final String key1Value,
            final String key2Value,
            final int expectedSize) {
        final JobID jobID = new JobID();
        final Configuration conf = new Configuration();
        conf.set(MdcOptions.JOB_CONFIGURATION_TO_MDC_KEYS, keyMapping);
        if (key1Value != null) {
            conf.setString("job.key-1", key1Value);
        }
        if (key2Value != null) {
            conf.setString("job.key-2", key2Value);
        }

        final Map<String, String> context = MdcUtils.asContextData(jobID, conf);

        assertContextEntries(context, jobID, key1Value, key2Value, expectedSize);
    }

    @Test
    void testConfigContextIsUnmodifiable() {
        final JobID jobID = new JobID();
        final Configuration conf = new Configuration();
        conf.set(MdcOptions.JOB_CONFIGURATION_TO_MDC_KEYS, Map.of("job.key-1", "mdc-key-1"));
        conf.setString("job.key-1", "val-1");

        final Map<String, String> context = MdcUtils.asContextData(jobID, conf);

        assertThatThrownBy(() -> context.put("extra", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static Stream<Arguments> skippedValueCases() {
        return Stream.of(Arguments.of("blank value", "  "), Arguments.of("missing key", null));
    }

    @ParameterizedTest
    @MethodSource("skippedValueCases")
    void testKeySkippedWhenValueAbsentOrBlank(String scenario, String configValue) {
        final JobID jobID = new JobID();
        final Configuration conf = new Configuration();
        conf.set(MdcOptions.JOB_CONFIGURATION_TO_MDC_KEYS, Map.of("job.key-1", "mdc-key-1"));
        if (configValue != null) {
            conf.setString("job.key-1", configValue);
        }

        final Map<String, String> context = MdcUtils.asContextData(jobID, conf);

        assertThat(context)
                .as(scenario)
                .isEqualTo(Collections.singletonMap(MdcUtils.JOB_ID, jobID.toHexString()));
    }

    // --- JobMdcRegistry integration: registry-first lookup ---

    @Test
    void testAsContextDataUsesRegistry() {
        final JobID jobID = new JobID();
        JobMdcRegistry.registerOrClear(
                jobID, MdcTestFixtures.enrichedConfiguration("val-1", "val-2"));

        assertThat(MdcUtils.asContextData(jobID))
                .containsEntry(MdcUtils.JOB_ID, jobID.toHexString())
                .containsEntry("mdc-key-1", "val-1")
                .containsEntry("mdc-key-2", "val-2")
                .hasSize(3);
    }

    @Test
    void testMdcRestoredAfterScopeCloses() {
        final JobID jobID = new JobID();
        final Configuration conf = new Configuration();
        conf.set(
                MdcOptions.JOB_CONFIGURATION_TO_MDC_KEYS,
                Map.of("job.key-1", "mdc-key-1", "job.key-2", "mdc-key-2"));
        conf.setString("job.key-1", "scoped-val-1");
        conf.setString("job.key-2", "scoped-val-2");

        try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobID, conf))) {
            assertThat(MDC.get("mdc-key-1")).isEqualTo("scoped-val-1");
        }
        assertThat(MDC.get("mdc-key-1")).isNull();
        assertThat(MDC.get("mdc-key-2")).isNull();
    }

    private static Stream<Arguments> jobScopedRunners() {
        return Stream.of(
                Arguments.of(
                        (Function<JobID, ThrowingConsumer<Runnable, Exception>>)
                                jobID -> {
                                    final Executor wrapped =
                                            MdcUtils.scopeToJob(jobID, Executors.directExecutor());
                                    return wrapped::execute;
                                }),
                Arguments.of(
                        (Function<JobID, ThrowingConsumer<Runnable, Exception>>)
                                jobID -> {
                                    final ExecutorService wrapped =
                                            MdcUtils.scopeToJob(
                                                    jobID, Executors.newDirectExecutorService());
                                    return action ->
                                            wrapped.submit(
                                                            () -> {
                                                                action.run();
                                                                return null;
                                                            })
                                                    .get();
                                }),
                Arguments.of(
                        (Function<JobID, ThrowingConsumer<Runnable, Exception>>)
                                jobID -> {
                                    final ManuallyTriggeredScheduledExecutorService ses =
                                            new ManuallyTriggeredScheduledExecutorService();
                                    final ScheduledExecutorService wrapped =
                                            MdcUtils.scopeToJob(jobID, ses);
                                    return action -> {
                                        wrapped.schedule(action, 0L, TimeUnit.MILLISECONDS);
                                        ses.triggerScheduledTasks();
                                    };
                                }));
    }

    @ParameterizedTest
    @MethodSource("jobScopedRunners")
    void testScopeToJobCapturesEnrichedContextAtConstructionTime(
            final Function<JobID, ThrowingConsumer<Runnable, Exception>> runnerFactory)
            throws Exception {
        final JobID jobID = new JobID();

        // Register before scopeToJob — context is baked at construction time
        JobMdcRegistry.registerOrClear(
                jobID, MdcTestFixtures.enrichedConfiguration("val-1", "val-2"));
        final ThrowingConsumer<Runnable, Exception> runner = runnerFactory.apply(jobID);
        final AtomicReference<Map<String, String>> captured = new AtomicReference<>();
        final Runnable capture = () -> captured.set(MDC.getCopyOfContextMap());

        runner.accept(capture);
        assertThat(captured.get())
                .containsEntry(MdcUtils.JOB_ID, jobID.toHexString())
                .containsEntry("mdc-key-1", "val-1")
                .containsEntry("mdc-key-2", "val-2")
                .hasSize(3);
    }

    // --- helpers ---

    private static void assertContextEntries(
            final Map<String, String> context,
            final JobID jobID,
            final String expectedKey1,
            final String expectedKey2,
            final int expectedSize) {
        assertThat(context)
                .containsEntry(MdcUtils.JOB_ID, jobID.toHexString())
                .hasSize(expectedSize);
        if (expectedKey1 != null) {
            assertThat(context).containsEntry("mdc-key-1", expectedKey1);
        }
        if (expectedKey2 != null) {
            assertThat(context).containsEntry("mdc-key-2", expectedKey2);
        }
    }

    private void assertJobIDLogged(ThrowingConsumer<JobID, Exception> action) throws Exception {
        JobID jobID = new JobID();
        action.accept(jobID);
        assertJobIdLogged(jobID);
    }

    private void assertJobIdLogged(JobID jobId) {
        AbstractObjectAssert<?, Object> extracting =
                assertThat(loggerExtension.getEvents())
                        .singleElement()
                        .extracting(LogEvent::getContextData)
                        .extracting(m -> m.getValue(MdcUtils.JOB_ID));
        if (jobId == null) {
            extracting.isNull();
        } else {
            extracting.isEqualTo(jobId.toHexString());
        }
    }
}
