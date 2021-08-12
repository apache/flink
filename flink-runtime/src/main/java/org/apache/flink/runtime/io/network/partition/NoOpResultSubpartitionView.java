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

package org.apache.flink.runtime.io.network.partition;

import javax.annotation.Nullable;

/** A dummy implementation of the {@link ResultSubpartitionView}. */
public class NoOpResultSubpartitionView implements ResultSubpartitionView {

    @Nullable
    public ResultSubpartition.BufferAndBacklog getNextBuffer() {
        return null;
    }

    @Override
    public void notifyDataAvailable() {}

    @Override
    public void releaseAllResources() {}

    @Override
    public boolean isReleased() {
        return false;
    }

    @Override
    public void resumeConsumption() {}

    @Override
    public void acknowledgeAllDataProcessed() {}

    @Override
    public Throwable getFailureCause() {
        return null;
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        return new AvailabilityWithBacklog(false, 0);
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {}
}
