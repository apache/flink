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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/** Contains information of a Profiling Instance. */
public class ProfilingInfo implements ResponseBody, Serializable {
    private static final long serialVersionUID = 1L;
    public static final String FIELD_NAME_STATUS = "status";
    public static final String FIELD_NAME_MODE = "mode";
    public static final String FIELD_NAME_TRIGGER_TIME = "triggerTime";
    public static final String FIELD_NAME_FINISHED_TIME = "finishedTime";
    public static final String FIELD_NAME_DURATION = "duration";
    public static final String FIELD_NAME_MESSAGE = "message";
    public static final String FIELD_NAME_OUTPUT_FILE = "outputFile";

    @JsonProperty(FIELD_NAME_STATUS)
    private ProfilingStatus status;

    @JsonProperty(FIELD_NAME_MODE)
    private ProfilingMode mode;

    @JsonProperty(FIELD_NAME_TRIGGER_TIME)
    private Long triggerTime;

    @JsonProperty(FIELD_NAME_FINISHED_TIME)
    private Long finishedTime;

    @JsonProperty(FIELD_NAME_DURATION)
    private Long duration;

    @JsonProperty(FIELD_NAME_MESSAGE)
    private String message;

    @JsonProperty(FIELD_NAME_OUTPUT_FILE)
    private String outputFile;

    private ProfilingInfo() {}

    private ProfilingInfo(
            ProfilingStatus status,
            ProfilingMode mode,
            long triggerTime,
            long finishedTime,
            long duration,
            String message,
            String outputFile) {
        this.status = status;
        this.mode = mode;
        this.triggerTime = triggerTime;
        this.finishedTime = finishedTime;
        this.duration = duration;
        this.message = message;
        this.outputFile = outputFile;
    }

    public ProfilingInfo fail(String message) {
        this.status = ProfilingStatus.FAILED;
        this.message = message;
        this.finishedTime = System.currentTimeMillis();
        return this;
    }

    public ProfilingInfo success(String outputFile) {
        this.status = ProfilingStatus.FINISHED;
        this.finishedTime = System.currentTimeMillis();
        this.outputFile = outputFile;
        this.message = "Profiling Successful";
        return this;
    }

    public ProfilingStatus getStatus() {
        return status;
    }

    public ProfilingMode getProfilingMode() {
        return mode;
    }

    public Long getDuration() {
        return duration;
    }

    public String getMessage() {
        return message;
    }

    public Long getFinishedTime() {
        return finishedTime;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public long getTriggerTime() {
        return triggerTime;
    }

    public static ProfilingInfo create(long duration, ProfilingMode mode) {
        ProfilingInfo profilingInfo = new ProfilingInfo();
        profilingInfo.mode = mode;
        profilingInfo.triggerTime = System.currentTimeMillis();
        profilingInfo.status = ProfilingStatus.RUNNING;
        profilingInfo.duration = duration;
        return profilingInfo;
    }

    @JsonCreator
    public static ProfilingInfo create(
            @JsonProperty(FIELD_NAME_STATUS) ProfilingStatus status,
            @JsonProperty(FIELD_NAME_MODE) ProfilingMode mode,
            @JsonProperty(FIELD_NAME_TRIGGER_TIME) long triggerTime,
            @JsonProperty(FIELD_NAME_FINISHED_TIME) long finishedTime,
            @JsonProperty(FIELD_NAME_DURATION) long duration,
            @JsonProperty(FIELD_NAME_MESSAGE) String message,
            @JsonProperty(FIELD_NAME_OUTPUT_FILE) String outputPath) {
        return new ProfilingInfo(
                status, mode, triggerTime, finishedTime, duration, message, outputPath);
    }

    @Override
    public String toString() {
        return "ProfilingInfo{"
                + "status="
                + status
                + ", mode="
                + mode
                + ", triggerTime="
                + triggerTime
                + ", finishedTime="
                + finishedTime
                + ", duration="
                + duration
                + ", message='"
                + message
                + '\''
                + ", outputFile='"
                + outputFile
                + '\''
                + '}';
    }

    /** Profiling Status. */
    public enum ProfilingStatus {
        RUNNING,
        FINISHED,
        FAILED;

        public boolean isRunning() {
            return this == RUNNING;
        }
    }

    /** Supported profiling mode in async-profiler. */
    public enum ProfilingMode {
        CPU,
        ALLOC,
        LOCK,
        WALL,
        ITIMER;

        public String getCode() {
            return this.name().toLowerCase();
        }
    }
}
