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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A special {@link StreamTask} that is used for executing feedback edges. This is used in
 * combination with {@link StreamIterationTail}.
 */
@Internal
public class StreamIterationHead<OUT> extends OneInputStreamTask<OUT, OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamIterationHead.class);

    private RecordWriterOutput<OUT>[] streamOutputs;

    private final BlockingQueue<StreamRecord<OUT>> dataChannel;
    private final String brokerID;
    private final long iterationWaitTime;
    private final boolean shouldWait;

    public StreamIterationHead(Environment env) throws Exception {
        super(env);
        final String iterationId = getConfiguration().getIterationId();
        if (iterationId == null || iterationId.length() == 0) {
            throw new FlinkRuntimeException("Missing iteration ID in the task configuration");
        }

        this.dataChannel = new ArrayBlockingQueue<>(1);
        this.brokerID =
                createBrokerIdString(
                        getEnvironment().getJobID(),
                        iterationId,
                        getEnvironment().getTaskInfo().getIndexOfThisSubtask());
        this.iterationWaitTime = getConfiguration().getIterationWaitTime();
        this.shouldWait = iterationWaitTime > 0;
    }

    // ------------------------------------------------------------------------

    @Override
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
        StreamRecord<OUT> nextRecord =
                shouldWait
                        ? dataChannel.poll(iterationWaitTime, TimeUnit.MILLISECONDS)
                        : dataChannel.take();

        if (nextRecord != null) {
            for (RecordWriterOutput<OUT> output : streamOutputs) {
                output.collect(nextRecord);
            }
        } else {
            controller.suspendDefaultAction();
            mailboxProcessor.suspend();
        }
    }

    // ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    @Override
    public void init() {
        // offer the queue for the tail
        BlockingQueueBroker.INSTANCE.handIn(brokerID, dataChannel);
        LOG.info("Iteration head {} added feedback queue under {}", getName(), brokerID);

        this.streamOutputs = (RecordWriterOutput<OUT>[]) getStreamOutputs();

        // If timestamps are enabled we make sure to remove cyclic watermark dependencies
        if (isSerializingTimestamps()) {
            for (RecordWriterOutput<OUT> output : streamOutputs) {
                output.emitWatermark(new Watermark(Long.MAX_VALUE));
            }
        }
    }

    @Override
    protected void cleanUpInternal() {
        // make sure that we remove the queue from the broker, to prevent a resource leak
        BlockingQueueBroker.INSTANCE.remove(brokerID);
        LOG.info("Iteration head {} removed feedback queue under {}", getName(), brokerID);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Creates the identification string with which head and tail task find the shared blocking
     * queue for the back channel. The identification string is unique per parallel head/tail pair
     * per iteration per job.
     *
     * @param jid The job ID.
     * @param iterationID The id of the iteration in the job.
     * @param subtaskIndex The parallel subtask number
     * @return The identification string.
     */
    public static String createBrokerIdString(JobID jid, String iterationID, int subtaskIndex) {
        return jid + "-" + iterationID + "-" + subtaskIndex;
    }
}
