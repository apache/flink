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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * A lookup handler implement to do a binary lookup on a dim table.
 *
 * <p>Different with {@code CascadedLookupHandler}, the {@link BinaryLookupHandler} only looks up a
 * single table and do not be chained with other {@link DeltaJoinHandlerBase}s. Therefore, it allows
 * more optimizations compared to the {@code CascadedLookupHandler}. Specifically:
 *
 * <ol>
 *   <li>There only be a single input row.
 *   <li>There is no need to deduplicate results using upsert keys.
 *   <li>There is no need to filter the result with remaining conditions, and the lookup results
 *       will be directly returned.
 * </ol>
 */
public class BinaryLookupHandler extends LookupHandlerBase {

    private static final long serialVersionUID = 1L;

    public BinaryLookupHandler(
            DataType streamSideType,
            DataType lookupResultType,
            DataType lookupSidePassThroughCalcType,
            RowDataSerializer lookupSidePassThroughCalcRowSerializer,
            @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> lookupSideGeneratedCalc,
            int[] ownedSourceOrdinals,
            int ownedLookupOrdinal) {
        super(
                streamSideType,
                lookupResultType,
                lookupSidePassThroughCalcType,
                lookupSidePassThroughCalcRowSerializer,
                lookupSideGeneratedCalc,
                ownedSourceOrdinals,
                ownedLookupOrdinal,
                "BinaryLookupHandler");
    }

    @Override
    public void setNext(@Nullable DeltaJoinHandlerBase next) {
        super.setNext(next);

        Preconditions.checkArgument(
                next == null, "This binary handler should not have a concrete handler after it");
    }

    @Override
    public void asyncHandle() throws Exception {
        Collection<RowData> allSourceRowData =
                handlerContext.getSharedMultiInputRowDataBuffer().getData(ownedSourceOrdinals);

        Preconditions.checkState(allSourceRowData.size() == 1);

        RowData input = allSourceRowData.stream().findFirst().get();

        fetcher.asyncInvoke(input, createLookupResultFuture(input));
    }

    @Override
    protected void completeResultsInMailbox(RowData input, Collection<RowData> lookupResult) {
        handlerContext.getRealOutputResultFuture().complete(lookupResult);
    }

    @Override
    protected DeltaJoinHandlerBase copyInternal() {
        return new BinaryLookupHandler(
                streamSideType,
                lookupResultType,
                lookupSidePassThroughCalcType,
                lookupSidePassThroughCalcRowSerializer,
                lookupSideGeneratedCalc,
                ownedSourceOrdinals,
                ownedLookupOrdinal);
    }
}
