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

package org.apache.flink.streaming.api.operators.python.embedded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.types.Row;

import pemja.core.object.PyIterator;

import java.util.List;

import static org.apache.flink.python.PythonOptions.MAP_STATE_READ_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.MAP_STATE_WRITE_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.PythonOptions.STATE_CACHE_SIZE;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;

/**
 * {@link EmbeddedPythonKeyedProcessOperator} is responsible for executing user defined python
 * KeyedProcessFunction in embedded Python environment. It is also able to handle the timer and
 * state request from the python stateful user defined function.
 */
@Internal
public class EmbeddedPythonKeyedProcessOperator<K, IN, OUT>
        extends AbstractOneInputEmbeddedPythonFunctionOperator<IN, OUT>
        implements Triggerable<K, VoidNamespace> {

    private static final long serialVersionUID = 1L;

    /** The TypeInformation of the key. */
    private transient TypeInformation<K> keyTypeInfo;

    private transient ContextImpl context;

    private transient OnTimerContextImpl onTimerContext;

    private transient PythonTypeUtils.DataConverter<K, Object> keyConverter;

    public EmbeddedPythonKeyedProcessOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<IN> inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo) {
        super(config, pythonFunctionInfo, inputTypeInfo, outputTypeInfo);
    }

    @Override
    public void open() throws Exception {
        keyTypeInfo = ((RowTypeInfo) this.getInputTypeInfo()).getTypeAt(0);

        keyConverter = PythonTypeUtils.TypeInfoToDataConverter.typeInfoDataConverter(keyTypeInfo);

        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

        TimerService timerService = new SimpleTimerService(internalTimerService);

        context = new ContextImpl(timerService);

        onTimerContext = new OnTimerContextImpl(timerService);

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
    public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        collector.setAbsoluteTimestamp(timer.getTimestamp());
        invokeUserFunction(TimeDomain.EVENT_TIME, timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        collector.eraseTimestamp();
        invokeUserFunction(TimeDomain.PROCESSING_TIME, timer);
    }

    @Override
    public Object getFunctionContext() {
        return context;
    }

    @Override
    public Object getTimerContext() {
        return onTimerContext;
    }

    @Override
    public <T> AbstractEmbeddedDataStreamPythonFunctionOperator<T> copy(
            DataStreamPythonFunctionInfo pythonFunctionInfo, TypeInformation<T> outputTypeInfo) {
        return new EmbeddedPythonKeyedProcessOperator<>(
                config, pythonFunctionInfo, getInputTypeInfo(), outputTypeInfo);
    }

    private void invokeUserFunction(TimeDomain timeDomain, InternalTimer<K, VoidNamespace> timer)
            throws Exception {
        onTimerContext.timeDomain = timeDomain;
        onTimerContext.timer = timer;
        PyIterator results =
                (PyIterator)
                        interpreter.invokeMethod("operation", "on_timer", timer.getTimestamp());

        while (results.hasNext()) {
            OUT result = outputDataConverter.toInternal(results.next());
            collector.collect(result);
        }
        results.close();

        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
    }

    private class ContextImpl {

        private final TimerService timerService;

        ContextImpl(TimerService timerService) {
            this.timerService = timerService;
        }

        public long timestamp() {
            return timestamp;
        }

        public TimerService timerService() {
            return timerService;
        }

        @SuppressWarnings("unchecked")
        public Object getCurrentKey() {
            return keyConverter.toExternal(
                    (K)
                            ((Row) EmbeddedPythonKeyedProcessOperator.this.getCurrentKey())
                                    .getField(0));
        }
    }

    private class OnTimerContextImpl {

        private final TimerService timerService;

        private TimeDomain timeDomain;

        private InternalTimer<K, VoidNamespace> timer;

        OnTimerContextImpl(TimerService timerService) {
            this.timerService = timerService;
        }

        public long timestamp() {
            return timer.getTimestamp();
        }

        public TimerService timerService() {
            return timerService;
        }

        public int timeDomain() {
            return timeDomain.ordinal();
        }

        @SuppressWarnings("unchecked")
        public Object getCurrentKey() {
            return keyConverter.toExternal((K) ((Row) timer.getKey()).getField(0));
        }
    }
}
