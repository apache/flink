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

package org.apache.flink.streaming.api.functions.python.eventtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.java.tuple.Tuple2;

/** TimestampAssigner which extracts timestamp from the second field of the input element. */
@Internal
public class CustomTimestampAssigner<T> implements SerializableTimestampAssigner<Tuple2<T, Long>> {

    private static final long serialVersionUID = 1L;

    @Override
    public long extractTimestamp(Tuple2<T, Long> element, long recordTimestamp) {
        return element.f1;
    }
}
