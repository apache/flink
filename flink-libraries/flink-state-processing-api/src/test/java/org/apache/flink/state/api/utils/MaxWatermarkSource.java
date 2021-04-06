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

package org.apache.flink.state.api.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * A simple source that emits a max watermark and then immediately returns. This provides an easy
 * way to trigger all event time timers for a restored savepoint.
 */
public class MaxWatermarkSource<T> implements SourceFunction<T> {

    private static final long serialVersionUID = 1L;

    @Override
    public void run(SourceContext<T> ctx) {
        ctx.emitWatermark(Watermark.MAX_WATERMARK);
    }

    @Override
    public void cancel() {}
}
