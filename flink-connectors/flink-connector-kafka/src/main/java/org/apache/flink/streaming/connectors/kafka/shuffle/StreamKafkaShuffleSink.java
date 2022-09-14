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

package org.apache.flink.streaming.connectors.kafka.shuffle;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * A customized {@link StreamOperator} for executing {@link FlinkKafkaShuffleProducer} that handle
 * both elements and watermarks. If the shuffle sink is determined to be useful to other sinks in
 * the future, we should abstract this operator to data stream api. For now, we keep the operator
 * this way to avoid public interface change.
 */
@Internal
class StreamKafkaShuffleSink<IN> extends StreamSink<IN> {

    public StreamKafkaShuffleSink(FlinkKafkaShuffleProducer flinkKafkaShuffleProducer) {
        super(flinkKafkaShuffleProducer);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        ((FlinkKafkaShuffleProducer) userFunction).invoke(mark);
    }
}
