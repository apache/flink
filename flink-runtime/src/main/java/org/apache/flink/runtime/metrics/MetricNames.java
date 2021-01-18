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

package org.apache.flink.runtime.metrics;

/** Collection of metric names. */
public class MetricNames {
    private MetricNames() {}

    public static final String SUFFIX_RATE = "PerSecond";

    public static final String IO_NUM_RECORDS_IN = "numRecordsIn";
    public static final String IO_NUM_RECORDS_OUT = "numRecordsOut";
    public static final String IO_NUM_RECORDS_IN_RATE = IO_NUM_RECORDS_IN + SUFFIX_RATE;
    public static final String IO_NUM_RECORDS_OUT_RATE = IO_NUM_RECORDS_OUT + SUFFIX_RATE;

    public static final String IO_NUM_BYTES_IN = "numBytesIn";
    public static final String IO_NUM_BYTES_OUT = "numBytesOut";
    public static final String IO_NUM_BYTES_IN_RATE = IO_NUM_BYTES_IN + SUFFIX_RATE;
    public static final String IO_NUM_BYTES_OUT_RATE = IO_NUM_BYTES_OUT + SUFFIX_RATE;

    public static final String IO_NUM_BUFFERS_IN = "numBuffersIn";
    public static final String IO_NUM_BUFFERS_OUT = "numBuffersOut";
    public static final String IO_NUM_BUFFERS_OUT_RATE = IO_NUM_BUFFERS_OUT + SUFFIX_RATE;

    public static final String IO_CURRENT_INPUT_WATERMARK = "currentInputWatermark";
    @Deprecated public static final String IO_CURRENT_INPUT_1_WATERMARK = "currentInput1Watermark";
    @Deprecated public static final String IO_CURRENT_INPUT_2_WATERMARK = "currentInput2Watermark";
    public static final String IO_CURRENT_INPUT_WATERMARK_PATERN = "currentInput%dWatermark";
    public static final String IO_CURRENT_OUTPUT_WATERMARK = "currentOutputWatermark";

    public static final String NUM_RUNNING_JOBS = "numRunningJobs";
    public static final String TASK_SLOTS_AVAILABLE = "taskSlotsAvailable";
    public static final String TASK_SLOTS_TOTAL = "taskSlotsTotal";
    public static final String NUM_REGISTERED_TASK_MANAGERS = "numRegisteredTaskManagers";
    public static final String NUM_PENDING_TASK_MANAGERS = "numPendingTaskManagers";

    public static final String NUM_RESTARTS = "numRestarts";

    // Includes failures that ignore restarts, thus the value is larger than numRestarts.
    public static final String NUM_JOB_FAILURES = "numJobFailures";

    @Deprecated public static final String FULL_RESTARTS = "fullRestarts";

    public static final String MEMORY_USED = "Used";
    public static final String MEMORY_COMMITTED = "Committed";
    public static final String MEMORY_MAX = "Max";

    public static final String IS_BACK_PRESSURED = "isBackPressured";

    public static final String CHECKPOINT_ALIGNMENT_TIME = "checkpointAlignmentTime";
    public static final String CHECKPOINT_START_DELAY_TIME = "checkpointStartDelayNanos";

    public static final String START_WORKER_FAILURE_RATE = "startWorkFailure" + SUFFIX_RATE;

    public static String currentInputWatermarkName(int index) {
        return String.format(IO_CURRENT_INPUT_WATERMARK_PATERN, index);
    }

    public static final String TASK_IDLE_TIME = "idleTimeMs" + SUFFIX_RATE;
    public static final String TASK_BUSY_TIME = "busyTimeMs" + SUFFIX_RATE;
    public static final String TASK_BACK_PRESSURED_TIME = "backPressuredTimeMs" + SUFFIX_RATE;
    public static final String TASK_SOFT_BACK_PRESSURED_TIME =
            "softBackPressuredTimeMs" + SUFFIX_RATE;
    public static final String TASK_HARD_BACK_PRESSURED_TIME =
            "hardBackPressuredTimeMs" + SUFFIX_RATE;
    public static final String TASK_MAX_SOFT_BACK_PRESSURED_TIME = "maxSoftBackPressureTimeMs";
    public static final String TASK_MAX_HARD_BACK_PRESSURED_TIME = "maxHardBackPressureTimeMs";

    public static final String ESTIMATED_TIME_TO_CONSUME_BUFFERS =
            "estimatedTimeToConsumeBuffersMs";
    public static final String DEBLOATED_BUFFER_SIZE = "debloatedBufferSize";

    // FLIP-33 sink
    // deprecated use NUM_RECORDS_SEND_ERRORS instead.
    @Deprecated public static final String NUM_RECORDS_OUT_ERRORS = "numRecordsOutErrors";
    public static final String NUM_RECORDS_SEND_ERRORS = "numRecordsSendErrors";
    public static final String CURRENT_SEND_TIME = "currentSendTime";
    public static final String NUM_RECORDS_SEND = "numRecordsSend";
    public static final String NUM_BYTES_SEND = "numBytesSend";

    // FLIP-33 source
    public static final String NUM_RECORDS_IN_ERRORS = "numRecordsInErrors";
    public static final String CURRENT_FETCH_EVENT_TIME_LAG = "currentFetchEventTimeLag";
    public static final String CURRENT_EMIT_EVENT_TIME_LAG = "currentEmitEventTimeLag";
    public static final String WATERMARK_LAG = "watermarkLag";
    public static final String PENDING_RECORDS = "pendingRecords";
    public static final String PENDING_BYTES = "pendingBytes";
    public static final String SOURCE_IDLE_TIME = "sourceIdleTime";

    // FLIP-182 (watermark alignment)
    public static final String WATERMARK_ALIGNMENT_DRIFT = "watermarkAlignmentDrift";

    public static final String MAILBOX_THROUGHPUT = "mailboxMailsPerSecond";
    public static final String MAILBOX_LATENCY = "mailboxLatencyMs";
    public static final String MAILBOX_SIZE = "mailboxQueueSize";
}
