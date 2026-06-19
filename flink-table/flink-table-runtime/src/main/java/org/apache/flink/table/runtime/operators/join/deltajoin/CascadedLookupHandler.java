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

package org.apache.flink.table.runtime.operators.join.deltajoin;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This handler represents one of the lookup actions when performing cascaded lookups on multi
 * dimension tables.
 *
 * <p>For example, if the lookup chain is `[D -> A, A -> B, (A, B) -> C]`, there are three {@link
 * CascadedLookupHandler}, each representing one of the lookup actions within the sequence.
 */
public class CascadedLookupHandler extends LookupHandlerBase {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CascadedLookupHandler.class);

    // used for debug and start with 1
    private final int id;
    private @Nullable final GeneratedFilterCondition generatedRemainingCondition;
    private final RowDataKeySelector streamSideLookupKeySelector;
    private final boolean leftLookupRight;

    private @Nullable transient FilterCondition remainingCondition;
    private transient Map<RowData, List<RowData>> allInputsWithLookupKey;
    private transient Map<RowData, Collection<RowData>> lookupResults;

    private @Nullable transient Integer totalNumShouldBeHandledThisRound = null;
    private @Nullable transient Integer handledNum = null;

    public CascadedLookupHandler(
            int id,
            DataType streamSideType,
            DataType lookupResultType,
            DataType lookupSidePassThroughCalcType,
            RowDataSerializer lookupSidePassThroughCalcRowSerializer,
            @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> lookupSideGeneratedCalc,
            @Nullable GeneratedFilterCondition generatedRemainingCondition,
            RowDataKeySelector streamSideLookupKeySelector,
            int[] ownedSourceOrdinals,
            int ownedLookupOrdinal,
            boolean leftLookupRight) {
        super(
                streamSideType,
                lookupResultType,
                lookupSidePassThroughCalcType,
                lookupSidePassThroughCalcRowSerializer,
                lookupSideGeneratedCalc,
                ownedSourceOrdinals,
                ownedLookupOrdinal,
                "CascadedLookupHandler-" + id);
        this.id = id;
        this.generatedRemainingCondition = generatedRemainingCondition;
        this.streamSideLookupKeySelector = streamSideLookupKeySelector;

        if (leftLookupRight) {
            Preconditions.checkArgument(
                    Arrays.stream(ownedSourceOrdinals).allMatch(i -> i < ownedLookupOrdinal));
        } else {
            Preconditions.checkArgument(
                    Arrays.stream(ownedSourceOrdinals).allMatch(i -> i > ownedLookupOrdinal));
        }
        this.leftLookupRight = leftLookupRight;
    }

    @Override
    public void setNext(@Nullable DeltaJoinHandlerBase next) {
        super.setNext(next);

        Preconditions.checkArgument(
                next != null, "This cascaded lookup handler must have a concrete handler after it");
    }

    @Override
    public void open(OpenContext openContext, DeltaJoinHandlerContext handlerContext)
            throws Exception {
        super.open(openContext, handlerContext);

        RuntimeContext runtimeContext = handlerContext.getRuntimeContext();

        if (generatedRemainingCondition != null) {
            this.remainingCondition =
                    generatedRemainingCondition.newInstance(
                            runtimeContext.getUserCodeClassLoader());
            FunctionUtils.setFunctionRuntimeContext(remainingCondition, runtimeContext);
            FunctionUtils.openFunction(remainingCondition, openContext);
        }

        this.lookupResults = new HashMap<>();
        this.allInputsWithLookupKey = new HashMap<>();

        Preconditions.checkState(
                next != null, "This cascaded lookup handler must have a concrete handler after it");
    }

    @Override
    public void asyncHandle() throws Exception {
        Preconditions.checkState(
                totalNumShouldBeHandledThisRound == null && handledNum == null,
                "This handler is handled without being reset");

        Collection<RowData> allSourceRowData =
                handlerContext.getSharedMultiInputRowDataBuffer().getData(ownedSourceOrdinals);

        for (RowData input : allSourceRowData) {
            RowData lookupKey = streamSideLookupKeySelector.getKey(input);
            allInputsWithLookupKey.compute(
                    lookupKey,
                    (k, v) -> {
                        if (v == null) {
                            v = new ArrayList<>();
                        }
                        v.add(input);
                        return v;
                    });
        }

        totalNumShouldBeHandledThisRound = allInputsWithLookupKey.size();
        handledNum = 0;

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Begin to lookup from {} to {}, total round: {}, current round: {}",
                    Arrays.toString(ownedSourceOrdinals),
                    ownedLookupOrdinal,
                    totalNumShouldBeHandledThisRound,
                    handledNum);
        }

        if (allSourceRowData.isEmpty()) {
            finish();
            return;
        }

        for (List<RowData> inputsOnThisLookupKey : allInputsWithLookupKey.values()) {
            // pick the first row as input to lookup
            RowData chosenToLookupInput = inputsOnThisLookupKey.get(0);
            fetcher.asyncInvoke(chosenToLookupInput, createLookupResultFuture(chosenToLookupInput));
        }
    }

    @Override
    protected void completeResultsInMailbox(RowData input, Collection<RowData> result) {
        Preconditions.checkState(
                totalNumShouldBeHandledThisRound != null && handledNum != null,
                "This handler is completed without being handled");

        lookupResults.put(input, result);

        handledNum++;

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "End to lookup from {} to {}, total round: {}, next round: {}",
                    Arrays.toString(ownedSourceOrdinals),
                    ownedLookupOrdinal,
                    totalNumShouldBeHandledThisRound,
                    handledNum);
        }

        if (noFurtherInput()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "There is no further input when looking up from {} to {}, total round: {}, next round: {}",
                        Arrays.toString(ownedSourceOrdinals),
                        ownedLookupOrdinal,
                        totalNumShouldBeHandledThisRound,
                        handledNum);
            }
            try {
                finish();
            } catch (Exception t) {
                LOG.error("Error happened in the lookup chain when finishing", t);
                completeExceptionally(t);
            }
        }
    }

    private boolean noFurtherInput() {
        return Objects.equals(handledNum, totalNumShouldBeHandledThisRound);
    }

    private void finish() throws Exception {
        Preconditions.checkState(noFurtherInput());
        totalNumShouldBeHandledThisRound = null;
        handledNum = null;

        Set<RowData> finalLookupResults = new HashSet<>();

        JoinedRowData reusedRowData = new JoinedRowData();

        // the time complexity is O(inputs * lookup results)
        for (RowData lookupKey : allInputsWithLookupKey.keySet()) {
            List<RowData> allInputsOnThisLookupKey = allInputsWithLookupKey.get(lookupKey);
            RowData chosenToLookupInput = allInputsOnThisLookupKey.get(0);

            Collection<RowData> lookupResult = lookupResults.get(chosenToLookupInput);
            for (RowData input : allInputsOnThisLookupKey) {
                for (RowData lookedUp : lookupResult) {
                    if (remainingCondition != null) {
                        if (leftLookupRight) {
                            reusedRowData.replace(input, lookedUp);
                        } else {
                            reusedRowData.replace(lookedUp, input);
                        }
                        if (!remainingCondition.apply(
                                FilterCondition.Context.INVALID_CONTEXT, reusedRowData)) {
                            continue;
                        }
                    }

                    finalLookupResults.add(lookedUp);
                }
            }
        }

        MultiInputRowDataBuffer sharedMultiInputRowDataBuffer =
                handlerContext.getSharedMultiInputRowDataBuffer();
        sharedMultiInputRowDataBuffer.setRowData(finalLookupResults, ownedLookupOrdinal);

        if (next != null) {
            next.asyncHandle();
        }
    }

    @Override
    public CascadedLookupHandler copyInternal() {
        return new CascadedLookupHandler(
                id,
                streamSideType,
                lookupResultType,
                lookupSidePassThroughCalcType,
                lookupSidePassThroughCalcRowSerializer,
                lookupSideGeneratedCalc,
                generatedRemainingCondition,
                streamSideLookupKeySelector.copy(),
                ownedSourceOrdinals,
                ownedLookupOrdinal,
                leftLookupRight);
    }

    @Override
    public void reset() {
        this.allInputsWithLookupKey.clear();
        this.lookupResults.clear();

        this.totalNumShouldBeHandledThisRound = null;
        this.handledNum = null;

        super.reset();
    }

    @Override
    public void close() throws Exception {
        if (remainingCondition != null) {
            FunctionUtils.closeFunction(remainingCondition);
        }

        if (this.allInputsWithLookupKey != null) {
            this.allInputsWithLookupKey.clear();
        }

        if (this.lookupResults != null) {
            this.lookupResults.clear();
        }

        super.close();
    }
}
