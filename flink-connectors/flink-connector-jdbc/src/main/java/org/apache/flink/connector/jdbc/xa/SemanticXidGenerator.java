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

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;

import javax.transaction.xa.Xid;

import java.security.SecureRandom;
import java.util.Optional;

/**
 * Generates {@link Xid} from:
 *
 * <ol>
 *   <li>To provide uniqueness over other jobs and apps, and other instances
 *   <li>of this job, gtrid consists of
 *   <li>job id (16 bytes)
 *   <li>subtask index (4 bytes)
 *   <li>checkpoint id (4 bytes)
 *   <li>bqual consists of 4 random bytes (generated using {@link SecureRandom})
 * </ol>
 *
 * <p>Each {@link SemanticXidGenerator} instance MUST be used for only one Sink (otherwise Xids will
 * collide).
 */
@Internal
class SemanticXidGenerator implements XidGenerator {

    private static final long serialVersionUID = 1L;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final int FORMAT_ID = 201;

    private transient byte[]
            gtridBuffer; // globalTransactionId = job id + task index + checkpoint id
    private transient byte[] bqualBuffer; // branchQualifier = random bytes

    @Override
    public void open() {
        gtridBuffer = new byte[3 * Long.BYTES];
        bqualBuffer = getRandomBytes(Integer.BYTES);
    }

    @Override
    public Xid generateXid(RuntimeContext runtimeContext, long checkpointId) {
        Optional<JobID> jobId = runtimeContext.getJobId();
        if (jobId.isPresent()) {
            System.arraycopy(jobId.get().getBytes(), 0, gtridBuffer, 0, JobID.SIZE);
        } else {
            // fall back to RNG if jobId is unavailable for some reason
            System.arraycopy(getRandomBytes(JobID.SIZE), 0, gtridBuffer, 0, JobID.SIZE);
        }

        writeNumber(runtimeContext.getIndexOfThisSubtask(), gtridBuffer, JobID.SIZE);
        // deliberately write only 4 bytes of checkpoint id
        writeNumber((int) checkpointId, gtridBuffer, JobID.SIZE + Integer.BYTES);
        // relying on arrays copying inside XidImpl constructor
        return new XidImpl(FORMAT_ID, gtridBuffer, bqualBuffer);
    }

    private static void writeNumber(int number, byte[] dst, int dstOffset) {
        for (int i = dstOffset; i < dstOffset + Integer.BYTES; i++) {
            dst[i] = (byte) number;
            number >>>= Byte.SIZE;
        }
    }

    private byte[] getRandomBytes(int size) {
        byte[] bytes = new byte[size];
        SECURE_RANDOM.nextBytes(bytes);
        return bytes;
    }
}
