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

package org.apache.flink.datastream.impl.builtin;

import org.apache.flink.datastream.api.builtin.BuiltinFuncs;
import org.apache.flink.datastream.api.extension.window.function.OneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.TwoInputNonBroadcastWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.TwoOutputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalOneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalTwoInputWindowStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalTwoOutputWindowStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.utils.WindowUtils;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.TaggedUnion;

/** Window-related implementations in {@link BuiltinFuncs}. */
public class BuiltinWindowFuncs {

    /**
     * Wrap the WindowStrategy and OneInputWindowStreamProcessFunction within a ProcessFunction to
     * perform the window operation.
     */
    public static <IN, OUT, W extends Window> OneInputStreamProcessFunction<IN, OUT> window(
            WindowStrategy windowStrategy,
            OneInputWindowStreamProcessFunction<IN, OUT> windowProcessFunction) {
        WindowAssigner<IN, W> windowAssigner =
                (WindowAssigner<IN, W>) WindowUtils.createWindowAssigner(windowStrategy);
        return new InternalOneInputWindowStreamProcessFunction<>(
                windowProcessFunction,
                windowAssigner,
                windowAssigner.getDefaultTrigger(),
                WindowUtils.getAllowedLateness(windowStrategy),
                windowStrategy);
    }

    /**
     * Wrap the WindowStrategy and TwoInputWindowStreamProcessFunction within a ProcessFunction to
     * perform the window operation.
     */
    public static <IN1, IN2, OUT, W extends Window>
            TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> window(
                    WindowStrategy windowStrategy,
                    TwoInputNonBroadcastWindowStreamProcessFunction<IN1, IN2, OUT>
                            windowProcessFunction) {
        WindowAssigner<TaggedUnion<IN1, IN2>, W> windowAssigner =
                (WindowAssigner<TaggedUnion<IN1, IN2>, W>)
                        WindowUtils.createWindowAssigner(windowStrategy);
        return new InternalTwoInputWindowStreamProcessFunction<>(
                windowProcessFunction,
                windowAssigner,
                windowAssigner.getDefaultTrigger(),
                WindowUtils.getAllowedLateness(windowStrategy),
                windowStrategy);
    }

    /**
     * Wrap the WindowStrategy and TwoOutputWindowStreamProcessFunction within a ProcessFunction to
     * perform the window operation.
     */
    public static <IN, OUT1, OUT2, W extends Window>
            TwoOutputStreamProcessFunction<IN, OUT1, OUT2> window(
                    WindowStrategy windowStrategy,
                    TwoOutputWindowStreamProcessFunction<IN, OUT1, OUT2> windowProcessFunction) {
        WindowAssigner<IN, W> windowAssigner =
                (WindowAssigner<IN, W>) WindowUtils.createWindowAssigner(windowStrategy);
        return new InternalTwoOutputWindowStreamProcessFunction<>(
                windowProcessFunction,
                windowAssigner,
                windowAssigner.getDefaultTrigger(),
                WindowUtils.getAllowedLateness(windowStrategy),
                windowStrategy);
    }
}
