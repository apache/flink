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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.RowData;

/** Interface for code generated filter condition function on single RowData. */
public interface FilterCondition extends RichFunction {

    /**
     * @return true if the filter condition stays true for the input row
     */
    boolean apply(Context ctx, RowData in);

    /**
     * Context for generating expressions such as e.g. {@code CURRENT_WATERMARK} or {@code
     * STREAMRECORD_TIMESTAMP}.
     *
     * @see ProcessFunction.Context
     */
    interface Context {
        /**
         * Timestamp of the element currently being processed or timestamp of a firing timer.
         *
         * <p>This might be {@code null}, depending on the stream's watermark strategy.
         */
        Long timestamp();

        /** A {@link TimerService} for querying time and registering timers. */
        TimerService timerService();

        static Context of(KeyedProcessFunction<?, ?, ?>.Context context) {
            return new Context() {
                @Override
                public Long timestamp() {
                    return context.timestamp();
                }

                @Override
                public TimerService timerService() {
                    return context.timerService();
                }
            };
        }

        static Context of(ProcessFunction<?, ?>.Context context) {
            return new Context() {
                @Override
                public Long timestamp() {
                    return context.timestamp();
                }

                @Override
                public TimerService timerService() {
                    return context.timerService();
                }
            };
        }

        Context INVALID_CONTEXT =
                new Context() {
                    @Override
                    public Long timestamp() {
                        throw new TableRuntimeException(
                                "Access to timestamp is not supported in this context.");
                    }

                    @Override
                    public TimerService timerService() {
                        throw new TableRuntimeException(
                                "Access to timerService is not supported in this context.");
                    }
                };
    }
}
