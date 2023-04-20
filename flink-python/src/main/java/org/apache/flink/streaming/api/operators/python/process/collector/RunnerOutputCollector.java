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

package org.apache.flink.streaming.api.operators.python.process.collector;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Output collector for Python UDF runner. */
@Internal
public final class RunnerOutputCollector<OUT> implements Collector<Row> {

    private final TimestampedCollector<OUT> collector;

    private final StreamRecord<?> reuse;

    public RunnerOutputCollector(TimestampedCollector<OUT> collector) {
        this.collector = collector;
        this.reuse = new StreamRecord<>(null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void collect(Row runnerOutput) {
        long ts = (long) runnerOutput.getField(0);
        if (ts != Long.MIN_VALUE) {
            collector.setAbsoluteTimestamp(ts);
        } else {
            collector.eraseTimestamp();
        }
        collector.collect((OUT) runnerOutput.getField(1));
    }

    @SuppressWarnings("unchecked")
    public <X> void collect(OutputTag<X> outputTag, Row runnerOutput) {
        long ts = (long) runnerOutput.getField(0);
        StreamRecord<X> typedReuse = (StreamRecord<X>) reuse;
        typedReuse.replace((X) runnerOutput.getField(1));
        if (ts != Long.MIN_VALUE) {
            reuse.setTimestamp(ts);
            collector.collect(outputTag, typedReuse);
        } else {
            reuse.eraseTimestamp();
            collector.collect(outputTag, typedReuse);
        }
    }

    @Override
    public void close() {
        collector.close();
    }

    public static TypeInformation<Row> getRunnerOutputTypeInfo(
            TypeInformation<?> elementDataTypeInfo) {
        // structure: [timestamp, data]
        return Types.ROW(Types.LONG, elementDataTypeInfo);
    }
}
