/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;

/**
 * A callback interface that the source operator can implement to trigger custom actions when a
 * commit request completes, which should normally be triggered from checkpoint complete event.
 */
@Internal
public interface KafkaCommitCallback {

    /**
     * A callback method the user can implement to provide asynchronous handling of commit request
     * completion. This method will be called when the commit request sent to the server has been
     * acknowledged without error.
     */
    void onSuccess();

    /**
     * A callback method the user can implement to provide asynchronous handling of commit request
     * failure. This method will be called when the commit request failed.
     *
     * @param cause Kafka commit failure cause returned by kafka client
     */
    void onException(Throwable cause);
}
