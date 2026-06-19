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

package org.apache.flink.connector.base.sink.writer.strategy;

import org.apache.flink.annotation.Internal;

/** Dataclass to encapsulate results from completed requests. */
@Internal
public class BasicResultInfo implements ResultInfo {
    private final int failedMessages;
    private final int batchSize;

    public BasicResultInfo(int failedMessages, int batchSize) {
        this.failedMessages = failedMessages;
        this.batchSize = batchSize;
    }

    public int getFailedMessages() {
        return failedMessages;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
