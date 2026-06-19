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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimerServiceImpl;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.process.InputSortBuffer.SortedRowConsumer;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Implementation of {@link MultipleInputStreamOperator} for {@link ProcessTableFunction} with at
 * least one table with set semantics.
 */
public class ProcessSetTableOperator extends AbstractProcessTableOperator
        implements MultipleInputStreamOperator<RowData> {

    private final RecordComparator[] orderByComparators;
    private final List<StateTtlConfig> inputBufferTtlConfigs;
    private final MailboxExecutor mailboxExecutor;

    private transient Watermark[] inputWatermarks;
    private transient InputSortBuffer[] inputSortBuffers;
    private transient MailboxPartialWatermarkProcessor[] inputWatermarkProcessors;

    public ProcessSetTableOperator(
            StreamOperatorParameters<RowData> parameters,
            List<RuntimeTableSemantics> tableSemantics,
            List<RuntimeStateInfo> stateInfos,
            RecordComparator[] orderByComparators,
            ProcessTableRunner processTableRunner,
            HashFunction[] stateHashCode,
            RecordEqualiser[] stateEquals,
            RuntimeChangelogMode producedChangelogMode,
            List<StateTtlConfig> inputBufferTtlConfigs) {
        super(
                parameters,
                tableSemantics,
                stateInfos,
                processTableRunner,
                stateHashCode,
                stateEquals,
                producedChangelogMode);
        this.orderByComparators = orderByComparators;
        this.inputBufferTtlConfigs = inputBufferTtlConfigs;
        this.mailboxExecutor = parameters.getMailboxExecutor();
    }

    @Override
    public void open() throws Exception {
        super.open();

        setInputWatermarks();
        setInputSortBuffers();
        setInputWatermarkProcessors();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public List<Input> getInputs() {
        return IntStream.range(0, tableSemantics.size())
                .mapToObj(
                        inputIdx -> {
                            final TableSemantics inputSemantics = tableSemantics.get(inputIdx);
                            final int timeColumn = inputSemantics.timeColumn();
                            return new AbstractInput<RowData, RowData>(this, inputIdx + 1) {
                                @Override
                                public void processElement(StreamRecord<RowData> element)
                                        throws Exception {
                                    final InputSortBuffer sortBuffer = inputSortBuffers[inputIdx];
                                    if (sortBuffer != null) {
                                        sortBuffer.processElement(element.getValue());
                                    } else {
                                        processTableEvent(inputIdx, element.getValue(), timeColumn);
                                    }
                                }

                                @Override
                                public void processWatermark(Watermark mark) throws Exception {
                                    // Immediately update the sort buffer to avoid
                                    // adding late events while sorting is still in progress
                                    if (inputSortBuffers[inputIdx] != null) {
                                        inputSortBuffers[inputIdx].updateWatermark(
                                                mark.getTimestamp());
                                    }
                                    // Handle watermarks per-input but in an interruptable way.
                                    // Advances the combined watermark eventually.
                                    inputWatermarkProcessors[inputIdx].emitWatermarkInsideMailbox(
                                            mark);
                                }
                            };
                        })
                .collect(Collectors.toList());
    }

    private void processTableEvent(int inputIdx, RowData row, int timeColumn) throws Exception {
        processTableRunner.ingestTableEvent(
                inputIdx, row, timeColumn, inputWatermarks[inputIdx].getTimestamp());
        processTableRunner.processEval();
    }

    // --------------------------------------------------------------------------------------------
    // Watermark handling
    // --------------------------------------------------------------------------------------------

    private void setInputWatermarks() {
        // Initialize per-input watermark tracking
        inputWatermarks = new Watermark[tableSemantics.size()];
        Arrays.fill(inputWatermarks, Watermark.UNINITIALIZED);
    }

    private void setInputSortBuffers() {
        inputSortBuffers = new InputSortBuffer[tableSemantics.size()];

        IntStream.range(0, tableSemantics.size())
                .forEach(
                        inputIdx -> {
                            final RuntimeTableSemantics semantics = tableSemantics.get(inputIdx);
                            if (semantics.orderByColumns().length == 0) {
                                return; // No ORDER BY for this input
                            }
                            assert orderByComparators[inputIdx] != null;
                            final RecordComparator comparator = orderByComparators[inputIdx];
                            final int timeColumn = semantics.orderByColumns()[0];
                            final LogicalType inputType = semantics.dataType().getLogicalType();
                            final KeyedStateStore stateStore =
                                    getKeyedStateStore().orElseThrow(IllegalStateException::new);

                            final int idx = inputIdx;
                            final SortedRowConsumer rowConsumer =
                                    row -> processTableEvent(idx, row, timeColumn);

                            // Get TTL config for this specific input
                            final StateTtlConfig ttlConfig = inputBufferTtlConfigs.get(inputIdx);

                            // Create a sort buffer (it implements Triggerable)
                            inputSortBuffers[inputIdx] =
                                    new InputSortBuffer(
                                            inputIdx,
                                            inputType,
                                            timeColumn,
                                            comparator,
                                            stateStore,
                                            rowConsumer,
                                            ttlConfig,
                                            getKeyedStateBackend());

                            // Create a dedicated timer service
                            InternalTimerService<VoidNamespace> timerService =
                                    getInternalTimerService(
                                            "input-sort-buffer-" + inputIdx,
                                            VoidNamespaceSerializer.INSTANCE,
                                            inputSortBuffers[inputIdx]);

                            // Inject timer service
                            inputSortBuffers[inputIdx].setTimerService(timerService);
                        });
    }

    private void setInputWatermarkProcessors() {
        // Multi-inputs always use interruptible timers per input
        inputWatermarkProcessors = new MailboxPartialWatermarkProcessor[tableSemantics.size()];

        IntStream.range(0, tableSemantics.size())
                .forEach(
                        pos -> {
                            final int inputIdx = pos;

                            // Collect timer services for this input
                            List<InternalTimerServiceImpl<?, ?>> timerServices = new ArrayList<>();

                            if (inputSortBuffers[inputIdx] != null) {
                                InternalTimerService<?> timerService =
                                        inputSortBuffers[inputIdx].getTimerService();
                                timerServices.add((InternalTimerServiceImpl<?, ?>) timerService);
                            }

                            // Create a watermark processor for this input
                            inputWatermarkProcessors[inputIdx] =
                                    new MailboxPartialWatermarkProcessor(
                                            "input-" + inputIdx + "-watermark",
                                            mailboxExecutor,
                                            timerServices,
                                            derivedWatermark -> {
                                                // When timer processing completes:
                                                // - Update per-input watermark
                                                // - Trigger global watermark
                                                advanceInputWatermark(inputIdx, derivedWatermark);
                                                reportWatermark(derivedWatermark, inputIdx + 1);
                                            });
                        });
    }

    private void advanceInputWatermark(int inputIdx, Watermark watermark) {
        inputWatermarks[inputIdx] = watermark;
    }
}
