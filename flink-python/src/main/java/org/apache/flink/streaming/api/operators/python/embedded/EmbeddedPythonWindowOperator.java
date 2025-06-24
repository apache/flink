/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.python.embedded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.python.DataStreamPythonFunctionOperator;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.types.Row;

import pemja.core.object.PyIterator;

import java.util.List;

import static org.apache.flink.python.PythonOptions.MAP_STATE_READ_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.MAP_STATE_WRITE_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.PythonOptions.STATE_CACHE_SIZE;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link EmbeddedPythonWindowOperator} is responsible for executing user defined python
 * ProcessWindowFunction in embedded Python environment.
 */
@Internal
public class EmbeddedPythonWindowOperator<K, IN, OUT, W extends Window>
        extends AbstractOneInputEmbeddedPythonFunctionOperator<IN, OUT>
        implements Triggerable<K, W> {

    private static final long serialVersionUID = 1L;

    /** For serializing the window in checkpoints. */
    private final TypeSerializer<W> windowSerializer;

    /** The TypeInformation of the key. */
    private transient TypeInformation<K> keyTypeInfo;

    private transient PythonTypeUtils.DataConverter<K, Object> keyConverter;

    private transient WindowContextImpl windowContext;

    private transient WindowTimerContextImpl windowTimerContext;

    public EmbeddedPythonWindowOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<IN> inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo,
            TypeSerializer<W> windowSerializer) {
        super(config, pythonFunctionInfo, inputTypeInfo, outputTypeInfo);
        this.windowSerializer = checkNotNull(windowSerializer);
    }

    @Override
    public void open() throws Exception {
        keyTypeInfo = ((RowTypeInfo) this.getInputTypeInfo()).getTypeAt(0);

        keyConverter = PythonTypeUtils.TypeInfoToDataConverter.typeInfoDataConverter(keyTypeInfo);

        InternalTimerService<W> internalTimerService =
                getInternalTimerService("window-timers", windowSerializer, this);

        windowContext = new WindowContextImpl(internalTimerService);

        windowTimerContext = new WindowTimerContextImpl(internalTimerService);

        super.open();
    }

    @Override
    public List<FlinkFnApi.UserDefinedDataStreamFunction> createUserDefinedFunctionsProto() {
        return ProtoUtils.createUserDefinedDataStreamStatefulFunctionProtos(
                getPythonFunctionInfo(),
                getRuntimeContext(),
                getJobParameters(),
                keyTypeInfo,
                inBatchExecutionMode(getKeyedStateBackend()),
                config.get(PYTHON_METRIC_ENABLED),
                config.get(PYTHON_PROFILE_ENABLED),
                hasSideOutput,
                config.get(STATE_CACHE_SIZE),
                config.get(MAP_STATE_READ_CACHE_SIZE),
                config.get(MAP_STATE_WRITE_CACHE_SIZE));
    }

    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {
        collector.setAbsoluteTimestamp(timer.getTimestamp());
        invokeUserFunction(timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
        collector.eraseTimestamp();
        invokeUserFunction(timer);
    }

    @Override
    public Object getFunctionContext() {
        return windowContext;
    }

    @Override
    public Object getTimerContext() {
        return windowTimerContext;
    }

    @Override
    public <T> DataStreamPythonFunctionOperator<T> copy(
            DataStreamPythonFunctionInfo pythonFunctionInfo, TypeInformation<T> outputTypeInfo) {
        return new EmbeddedPythonWindowOperator<>(
                config, pythonFunctionInfo, getInputTypeInfo(), outputTypeInfo, windowSerializer);
    }

    private void invokeUserFunction(InternalTimer<K, W> timer) throws Exception {
        windowTimerContext.timer = timer;

        PyIterator results =
                (PyIterator)
                        interpreter.invokeMethod("operation", "on_timer", timer.getTimestamp());

        while (results.hasNext()) {
            OUT result = outputDataConverter.toInternal(results.next());
            collector.collect(result);
        }
        results.close();

        windowTimerContext.timer = null;
    }

    private class WindowContextImpl {
        private final InternalTimerService<W> timerService;

        WindowContextImpl(InternalTimerService<W> timerService) {
            this.timerService = timerService;
        }

        public TypeSerializer<W> getWindowSerializer() {
            return windowSerializer;
        }

        public long timestamp() {
            return timestamp;
        }

        public InternalTimerService<W> timerService() {
            return timerService;
        }

        @SuppressWarnings("unchecked")
        public Object getCurrentKey() {
            return keyConverter.toExternal(
                    (K) ((Row) EmbeddedPythonWindowOperator.this.getCurrentKey()).getField(0));
        }
    }

    private class WindowTimerContextImpl {
        private final InternalTimerService<W> timerService;

        private InternalTimer<K, W> timer;

        WindowTimerContextImpl(InternalTimerService<W> timerService) {
            this.timerService = timerService;
        }

        public InternalTimerService<W> timerService() {
            return timerService;
        }

        public long timestamp() {
            return timer.getTimestamp();
        }

        public W getWindow() {
            return timer.getNamespace();
        }

        @SuppressWarnings("unchecked")
        public Object getCurrentKey() {
            return keyConverter.toExternal((K) ((Row) timer.getKey()).getField(0));
        }
    }
}
