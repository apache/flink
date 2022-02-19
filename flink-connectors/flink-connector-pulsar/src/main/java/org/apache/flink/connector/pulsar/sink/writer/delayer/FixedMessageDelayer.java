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

package org.apache.flink.connector.pulsar.sink.writer.delayer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;

/** A delayer for making sure all the messages would be sent in a fixed delay duration. */
@PublicEvolving
public class FixedMessageDelayer<IN> implements MessageDelayer<IN> {
    private static final long serialVersionUID = -7550834520312097614L;

    private final long delayDuration;

    public FixedMessageDelayer(long delayDuration) {
        this.delayDuration = delayDuration;
    }

    @Override
    public long deliverAt(IN message, PulsarSinkContext sinkContext) {
        if (delayDuration > 0) {
            return sinkContext.processTime() + delayDuration;
        } else {
            return delayDuration;
        }
    }
}
