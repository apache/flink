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

package org.apache.flink.table.gateway.workflow.scheduler;

import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.quartz.JobKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;

import static org.quartz.impl.StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME;
import static org.quartz.impl.StdSchedulerFactory.PROP_SCHED_RMI_EXPORT;
import static org.quartz.impl.StdSchedulerFactory.PROP_SCHED_RMI_PROXY;
import static org.quartz.impl.StdSchedulerFactory.PROP_THREAD_POOL_CLASS;
import static org.quartz.impl.StdSchedulerFactory.PROP_THREAD_POOL_PREFIX;

/** Utility class for quartz scheduler. */
public class QuartzSchedulerUtils {

    public static final String SCHEDULE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter DEFAULT_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern(SCHEDULE_TIME_FORMAT);

    public static final String QUARTZ_JOB_PREFIX = "quartz_job";
    public static final String QUARTZ_JOB_GROUP = "default_group";
    public static final String UNDERLINE = "_";
    public static final String WORKFLOW_INFO = "workflowInfo";

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    /**
     * This method is used for initializing the quartz scheduler config, the related options copy
     * from quartz.properties, but we override the option {@code org.quartz.threadPool.threadCount}
     * to 1 for minimizing the thread impact to SqlGateway.
     */
    public static Properties initializeQuartzSchedulerConfig() {
        Properties properties = new Properties();
        properties.setProperty(PROP_SCHED_INSTANCE_NAME, "FlinkQuartzScheduler");
        properties.setProperty(PROP_THREAD_POOL_PREFIX, "FlinkQuartzSchedulerThreadPool");
        properties.setProperty(PROP_SCHED_RMI_EXPORT, "false");
        properties.setProperty(PROP_SCHED_RMI_PROXY, "false");
        properties.setProperty(PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
        properties.setProperty("org.quartz.threadPool.threadCount", "1");
        properties.setProperty("org.quartz.threadPool.threadPriority", "5");
        properties.setProperty(
                "org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread",
                "true");
        properties.setProperty(
                "org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread",
                "true");
        properties.setProperty("org.quartz.jobStore.misfireThreshold", "60000");
        properties.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");

        return properties;
    }

    public static JobKey getJobKey(String materializedTableIdentifier) {
        String jobName = QUARTZ_JOB_PREFIX + UNDERLINE + materializedTableIdentifier;
        return JobKey.jobKey(jobName, QUARTZ_JOB_GROUP);
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    String.format(
                            "Failed to deserialize JSON string to %s: %s.",
                            clazz.getSimpleName(), e.getMessage()),
                    e);
        }
    }

    public static <T> String toJson(T t) {
        try {
            return OBJECT_MAPPER.writeValueAsString(t);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    String.format(
                            "Failed to serialize instance of %s to JSON string: %s.",
                            t.getClass().getSimpleName(), e.getMessage()),
                    e);
        }
    }

    public static String dateToString(Date date) {
        return date2LocalDateTime(date).format(DEFAULT_DATETIME_FORMATTER);
    }

    /**
     * Convert date to local datetime.
     *
     * @param date date
     * @return local datetime
     */
    private static LocalDateTime date2LocalDateTime(Date date) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(date.getTime()), ZoneId.systemDefault());
    }
}
