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

package org.apache.flink.state.api.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.state.api.functions.Timestamper;
import org.apache.flink.streaming.api.functions.TimestampAssigner;

/** Wraps an existing {@link TimestampAssigner} into a {@link Timestamper}. */
@Internal
public class TimestampAssignerWrapper<T> implements Timestamper<T> {

    private static final long serialVersionUID = 1L;

    private final TimestampAssigner<T> assigner;

    public TimestampAssignerWrapper(TimestampAssigner<T> assigner) {
        this.assigner = assigner;
    }

    @Override
    public long timestamp(T element) {
        return assigner.extractTimestamp(element, Long.MIN_VALUE);
    }
}
