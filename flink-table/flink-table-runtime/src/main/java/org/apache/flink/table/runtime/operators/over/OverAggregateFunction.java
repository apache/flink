package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.shaded.guava30.com.google.common.collect.BoundType;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.table.runtime.operators.rank.ComparableRecordComparator;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.data.util.RowDataUtil.isRetractMsg;
import org.apache.flink.table.runtime.operators.aggregate.RecordCounter;

import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;

import org.apache.flink.table.runtime.util.RowDataStringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Iterator;


public class OverAggregateFunction 
    extends KeyedProcessFunction<RowData, RowData, RowData>  {

    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(OverAggregateFunction.class);

    private RecordEqualiser equaliser;
    private GeneratedRecordEqualiser generatedEqualiser;
    private final InternalTypeInfo<RowData> inputRowType;
    private final RowDataStringSerializer inputRowSerializer;

    static private final String STATE_NAME = "state";
    private transient ValueState<SortedMap<RowData, List<RowData>>> state;

    private transient AggsHandleFunction function = null;
    private PerKeyStateDataViewStore dataViewStore = null;
    private GeneratedAggsHandleFunction genAggsHandler;
    private final ComparableRecordComparator sortKeyComparator;
    private final RowDataKeySelector sortKeySelector;
    private final InternalTypeInfo<RowData> sortKeyType;
    private final RecordCounter recordCounter;

    private JoinedRowData outputRow;

    private boolean isStreamMode;

    // XXX(sergei): we do not support count(*) yet
    private final int indexOfCountStar = -1;

    public OverAggregateFunction(
            InternalTypeInfo<RowData> inputRowType,
            GeneratedAggsHandleFunction genAggsHandler,
            GeneratedRecordEqualiser generatedEqualiser,
            ComparableRecordComparator sortKeyComparator,
            RowDataKeySelector sortKeySelector,
            boolean isBatchBackfillEnabled
        ) {
        this.inputRowType = inputRowType;
        this.genAggsHandler = genAggsHandler;
        this.sortKeyComparator = sortKeyComparator;
        this.sortKeySelector = sortKeySelector;
        this.sortKeyType = sortKeySelector.getProducedType();
        this.generatedEqualiser = generatedEqualiser;
        this.recordCounter = RecordCounter.of(indexOfCountStar);
        this.inputRowSerializer = new RowDataStringSerializer(this.inputRowType);
        this.isStreamMode = !isBatchBackfillEnabled;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        dataViewStore = new PerKeyStateDataViewStore(getRuntimeContext());
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(dataViewStore);

        ListTypeInfo<RowData> valueTypeInfo = new ListTypeInfo<>(inputRowType);
        ValueStateDescriptor<SortedMap<RowData, List<RowData>>> valueStateDescriptor =
            new ValueStateDescriptor<>(
                STATE_NAME,
                new SortedMapTypeInfo<>(
                        sortKeyType,
                        valueTypeInfo,
                        sortKeyComparator));

        state = getRuntimeContext().getState(valueStateDescriptor);

        equaliser = generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
        generatedEqualiser = null;

        outputRow = new JoinedRowData();
    }

    @Override
    public boolean isHybridStreamBatchCapable() {
        return true;
    }

    public boolean isBatchMode() {
        return !this.isStreamMode;
    }

    private String getPrintableName() {
        return getRuntimeContext().getJobId() + " " + getRuntimeContext().getTaskName();
    }

    @Override
    public void emitStateAndSwitchToStreaming(Context ctx, Collector<RowData> out,
                    KeyedStateBackend<RowData> be) throws Exception {
        if (isStreamMode) {
            LOG.warn("Programming error in {} -- asked to switch to streaming while not in batch mode",
                        getPrintableName());
            return;
        }

        LOG.info("{} transitioning from Batch to Stream mode", getPrintableName());

        ListTypeInfo<RowData> valueTypeInfo = new ListTypeInfo<>(inputRowType);
        ValueStateDescriptor<SortedMap<RowData, List<RowData>>> valueStateDescriptor =
            new ValueStateDescriptor<>(
                STATE_NAME,
                new SortedMapTypeInfo<>(
                        sortKeyType,
                        valueTypeInfo,
                        sortKeyComparator));

        be.applyToAllKeys(VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                valueStateDescriptor,
                new KeyedStateFunction<RowData, ValueState<SortedMap<RowData, List<RowData>>>>() {
                    @Override
                    public void process(RowData key, ValueState<SortedMap<RowData, List<RowData>>> state) throws Exception {
                        // The access to dataState.get() below requires a current key
                        // set for partioned stream operators.
                        be.setCurrentKey(key);

                        SortedMap<RowData, List<RowData>> treeMap = state.value();

                        RowData acc = function.createAccumulators();
                        function.setAccumulators(acc);

                        for (List<RowData> records : treeMap.values()) {
                            for (RowData record : records) {
                                function.accumulate(record);
                                outputRow.setRowKind(RowKind.INSERT);
                                outputRow.replace(record, function.getValue());
                                out.collect(outputRow);
                            }
                        }
                    }
                });

        LOG.info("{} transitioned to Stream mode", getPrintableName());

        isStreamMode = true;
    }


    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        LOG.debug("SERGEI input {}", inputRowSerializer.asString(input));

        SortedMap<RowData, List<RowData>> sortedMap = state.value();

        if (sortedMap == null) {
            sortedMap = new TreeMap<>(sortKeyComparator);
        }
    
        RowData sortKey = sortKeySelector.getKey(input);

        List<RowData> records = sortedMap.get(sortKey);

        if (records == null) {
            records = new ArrayList<>();
            sortedMap.put(sortKey, records);
        }

        boolean isAccumulate = isAccumulateMsg(input);
        RowKind inputKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // clobber for comparisons

        RowData acc = null;

        // XXX(sergei): an inefficient implementation that's going after correctness
        // first; once we have that, we'll optimize for performance a bit
        //
        // The inefficient implementation looks as follows:
        //  - emit retraction for all rows in the current partition
        //  - add/remove the new row
        //  - emit inserts for all rows in the current partition

        if (isStreamMode) {
            acc = function.createAccumulators();
            function.setAccumulators(acc);

            for (List<RowData> recs : sortedMap.values()) {
                for (RowData record : recs) {
                    function.accumulate(record);
                    outputRow.setRowKind(RowKind.DELETE);
                    outputRow.replace(record, function.getValue());
                    out.collect(outputRow);
                }
            }
        }

        if (isAccumulate) {
            records.add(input);
        } else {
            if (!records.remove(input)) {
                LOG.warn("row not found in state: {}", inputRowSerializer.asString(input));
            }
            if (records.isEmpty()) {
                sortedMap.remove(sortKey);
            }
        }

        state.update(sortedMap);

        if (isStreamMode) {
            function.resetAccumulators();
            for (List<RowData> recs : sortedMap.values()) {
                for (RowData record : recs) {
                    function.accumulate(record);
                    outputRow.setRowKind(RowKind.INSERT);
                    outputRow.replace(record, function.getValue());
                    out.collect(outputRow);
                }
            }
        }
    }
}
