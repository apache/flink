package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.MultiJoinStateHandler;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Streaming multi-way join operator which supports inner join and left/right/full outer join. It
 * eliminates the intermediate state necessary for a chain of multiple binary joins. In other words,
 * it considerable reduces the total amount of state necessary for chained joins. As of time
 * complexity, it performs better in the worst cases where the number of records in the intermediate
 * state is large but worst than reorded binary joins when the number of records in the intermediate
 * state is small.
 */
public class StreamingMultiJoinOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData> {

    private static final long serialVersionUID = 1L;

    private final List<JoinInputSideSpec> inputSpecs;
    private final List<JoinCondition> joinConditions;
    private final MultiJoinCondition multiJoinCondition;
    private final boolean[] filterNulls;
    private final long[] stateRetentionTime;
    private final List<Input> inputs;

    private transient List<MultiJoinStateHandler> stateHandlers;
    private transient ValueState<Long> cleanupTimeState;
    private transient TimestampedCollector<RowData> collector;

    /** Constructor for binary join conditions. */
    public StreamingMultiJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            List<InternalTypeInfo<RowData>> inputTypes,
            List<JoinInputSideSpec> inputSpecs,
            List<JoinCondition> joinConditions,
            boolean[] filterNulls,
            long[] stateRetentionTime) {
        this(
                parameters,
                inputTypes,
                inputSpecs,
                joinConditions,
                null,
                filterNulls,
                stateRetentionTime);
    }

    /**
     * Constructor that supports both binary join conditions and a multi-way join condition. If
     * multiJoinCondition is provided, it will be used instead of binary join conditions.
     */
    public StreamingMultiJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            List<InternalTypeInfo<RowData>> inputTypes,
            List<JoinInputSideSpec> inputSpecs,
            List<JoinCondition> joinConditions,
            MultiJoinCondition multiJoinCondition,
            boolean[] filterNulls,
            long[] stateRetentionTime) {
        super(parameters, inputSpecs.size());

        this.inputSpecs = inputSpecs;
        this.joinConditions = joinConditions;
        this.multiJoinCondition = multiJoinCondition;
        this.filterNulls = filterNulls;
        this.stateRetentionTime = stateRetentionTime;
        this.inputs = new ArrayList<>(inputSpecs.size());
    }

    @Override
    public void open() throws Exception {
        super.open();

        // Initialize collector
        this.collector = new TimestampedCollector<>(output);

        // Initialize state handlers for each input
        this.stateHandlers = new ArrayList<>(inputSpecs.size());

        // Initialize inputs
        for (int i = 0; i < inputSpecs.size(); i++) {
            MultiJoinStateHandler handler =
                    new MultiJoinStateHandler(
                            i,
                            this,
                            this.stateHandler,
                            getOperatorConfig().getConfiguration(),
                            getUserCodeClassloader(),
                            inputSpecs.get(i),
                            stateRetentionTime[i]);
            stateHandlers.add(handler);
            inputs.add(createInput(i + 1));
        }

        // Initialize cleanup time state
        ValueStateDescriptor<Long> cleanupTimeDescriptor =
                new ValueStateDescriptor<>("cleanup-time", Types.LONG);
        this.cleanupTimeState = getRuntimeContext().getState(cleanupTimeDescriptor);
    }

    public void processElement(int inputId, StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        long timestamp = element.getTimestamp();

        processElement(inputId, input, timestamp);
    }

    private void processElement(int inputId, RowData input, long timestamp) throws Exception {
        inputId = inputId - 1; // Convert to 0-based index

        // Update state for current input
        stateHandlers.get(inputId).addRecord(input);

        // Use multi-way join condition if available, otherwise use binary joins
        if (multiJoinCondition != null) {
            performMultiJoin(input, inputId);
        } else {
            performMultiBinaryJoin(input, inputId);
        }

        updateCleanupTime(timestamp);
    }

    /**
     * Performs a multi-way join using a single MultiJoinCondition that evaluates all join
     * conditions at once. This approach can be more efficient than the progressive binary join
     * because: 1. This is a hash join: we're only joining records for each input with matching keys
     * 2. It avoids creating intermediate joined rows 3. It can evaluate complex conditions across
     * all inputs at once 4. It can short-circuit evaluation when any condition fails
     */
    private void performMultiJoin(RowData input, int inputId) throws Exception {
        // Get iterables for all inputs
        List<Iterator<RowData>> allInputRecords = new ArrayList<>(inputSpecs.size());
        for (int i = 0; i < inputSpecs.size(); i++) {
            if (i == inputId) {
                allInputRecords.add(Collections.singleton(input).iterator());
            } else {
                allInputRecords.add(stateHandlers.get(i).getRecords());
            }
        }

        // Create an array to hold current rows in the cartesian product
        RowData[] currentRows = new RowData[inputSpecs.size()];

        // Perform a cartesian product with condition check across all inputs
        recursiveMultiJoin(0, input, currentRows, allInputRecords);
    }

    /**
     * Performs a multi-way join by progressively joining pairs of inputs using binary join
     * conditions. This approach builds the join result incrementally by: 0. This is a hash join:
     * we're only joining records for each input with matching keys 1. Starting with records from
     * the first input 2. Joining with the second input to produce intermediate results 3.
     * Progressively joining intermediate results with each subsequent input 4. Creating new
     * JoinedRowData objects at each step to represent partial results 5. Applying the appropriate
     * binary join condition at each step 6. Terminating early if any join step produces empty
     * results 7. We store one set of intermediate results in memory and keep updating it
     */
    private void performMultiBinaryJoin(RowData input, int inputId) throws Exception {
        // Start with initial records from first input
        List<RowData> intermediateResults = new ArrayList<>();
        collectRecords(
                inputId == 0
                        ? Collections.singleton(input).iterator()
                        : stateHandlers.get(0).getRecords(),
                intermediateResults);

        // Progressive join with each subsequent input
        for (int i = 1; i < inputSpecs.size() && !intermediateResults.isEmpty(); i++) {
            List<RowData> nextResults = new ArrayList<>();
            JoinCondition condition = joinConditions.get(i);

            // Get records from current input
            Iterator<RowData> otherSideRecords =
                    i == inputId
                            ? Collections.singleton(input).iterator()
                            : stateHandlers.get(i).getRecords();

            // Join each left record with matching right records
            for (RowData left : intermediateResults) {
                while (otherSideRecords.hasNext()) {
                    var right = otherSideRecords.next();
                    if (condition.apply(left, right)) {
                        var outRow = new JoinedRowData(left.getRowKind(), left, right);
                        // If we're not at the last input, store the joined row for further joining
                        if (i < inputSpecs.size() - 1) {
                            nextResults.add(outRow);
                        } else {
                            // If we're at the last input, emit the final joined row
                            collector.collect(outRow);
                        }
                    }
                }
            }
            intermediateResults = nextResults;
        }
    }

    /**
     * Recursively builds a cartesian product of all already filtered by key inputs and checks the
     * join condition for all of them in one go. This is a depth-first approach to building all
     * possible combinations of rows.
     */
    private void recursiveMultiJoin(
            int depth,
            RowData input,
            RowData[] currentRows,
            List<Iterator<RowData>> allInputRecords)
            throws Exception {
        if (depth == inputSpecs.size()) {
            // We have a complete combination of rows, check the condition
            if (multiJoinCondition.apply(currentRows)) {
                // Build a joined row by progressively joining the inputs
                RowData joinedRow = currentRows[0];
                for (int i = 1; i < currentRows.length; i++) {
                    joinedRow = new JoinedRowData(input.getRowKind(), joinedRow, currentRows[i]);
                }
                collector.collect(joinedRow);
            }
            return;
        }

        // For the current depth, iterate over all possible rows
        while (allInputRecords.get(depth).hasNext()) {
            currentRows[depth] = allInputRecords.get(depth).next();
            recursiveMultiJoin(depth + 1, input, currentRows, allInputRecords);
        }
    }

    private void collectRecords(Iterator<RowData> records, List<RowData> target) throws Exception {
        while (records.hasNext()) {
            target.add(records.next());
        }
    }

    @Override
    public List<Input> getInputs() {
        return inputs;
    }

    private Input<RowData> createInput(int idx) {
        return new AbstractInput<RowData, RowData>(this, idx) {
            @Override
            public void processElement(StreamRecord<RowData> element) throws Exception {
                ((StreamingMultiJoinOperator) owner).processElement(idx, element);
            }
        };
    }

    private void updateCleanupTime(long timestamp) throws Exception {
        Long currentCleanupTime = cleanupTimeState.value();
        long newCleanupTime = timestamp + getMaxRetentionTime();
        if (currentCleanupTime == null || newCleanupTime > currentCleanupTime) {
            cleanupTimeState.update(newCleanupTime);
        }
    }

    private long getMaxRetentionTime() {
        long maxTime = 0;
        for (long time : stateRetentionTime) {
            maxTime = Math.max(maxTime, time);
        }
        return maxTime;
    }

    @Override
    public void close() throws Exception {
        if (joinConditions != null) {
            for (JoinCondition condition : joinConditions) {
                condition.close();
            }
        }

        if (multiJoinCondition != null) {
            multiJoinCondition.close();
        }

        super.close();
    }
}
