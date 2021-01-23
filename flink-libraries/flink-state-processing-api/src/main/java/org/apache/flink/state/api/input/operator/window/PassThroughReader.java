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

package org.apache.flink.state.api.input.operator.window;

import org.apache.flink.state.api.functions.WindowReaderFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * A {@link WindowReaderFunction} that just emits each input element.
 *
 * @param <KEY> The key type.
 * @param <W> The window type.
 * @param <IN> The type stored in state.
 */
public class PassThroughReader<KEY, W extends Window, IN>
        extends WindowReaderFunction<IN, IN, KEY, W> {

    @Override
    public void readWindow(KEY key, Context<W> context, Iterable<IN> elements, Collector<IN> out) {
        for (IN element : elements) {
            out.collect(element);
        }
    }
}
