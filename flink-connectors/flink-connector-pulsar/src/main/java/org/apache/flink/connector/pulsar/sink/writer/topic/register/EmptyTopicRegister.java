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

package org.apache.flink.connector.pulsar.sink.writer.topic.register;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicRegister;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** The topic register which would do nothing for just return an empty topic partitions. */
@Internal
public class EmptyTopicRegister<IN> implements TopicRegister<IN> {
    private static final long serialVersionUID = -9199261243659491097L;

    @Override
    public List<String> topics(IN in) {
        return Collections.emptyList();
    }

    @Override
    public void open(SinkConfiguration sinkConfiguration, ProcessingTimeService timeService) {
        // Nothing to do.
    }

    @Override
    public void close() throws IOException {
        // Nothing to do.
    }
}
