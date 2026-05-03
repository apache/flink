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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.join.lookup.CalcCollectionCollector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link DeltaJoinHandlerBase} that used to lookup dim tables.
 *
 * <p>This type of {@link DeltaJoinHandlerBase} always holds a {@link AsyncFunction} fetcher to do
 * the lookup and get the lookup result by {@link #complete}.
 *
 * <p>The {@link #ownedSourceOrdinals} is the source ordinal of this lookup handler on the driving
 * side, and the {@link #ownedLookupOrdinal} is the target table ordinal that is used to lookup.
 *
 * <p>Take the following pattern as an example. Image that we are in the top `DeltaJoin2`.
 *
 * <pre>{@code
 *       DeltaJoin2
 *      /        \
 *   DeltaJoin1  #2 C
 *    /      \
 * #0 A     #1 B
 * }</pre>
 *
 * <p>If the stream side is `C`, and the two lookup handlers are `[(C -> A), (A -> B)]`, then in
 * these two lookup handlers, {@link #ownedSourceOrdinals} and {@link #ownedLookupOrdinal} are `[2],
 * 0` and `[0], 1`.
 *
 * <p>If the stream side is `DeltaJoin1`, the lookup handler is [(A, B -> C)], and the {@link
 * #ownedSourceOrdinals} and {@link #ownedLookupOrdinal} is `[0, 1], 2`.
 */
public abstract class LookupHandlerBase extends DeltaJoinHandlerBase {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(LookupHandlerBase.class);

    @Nullable
    protected final GeneratedFunction<FlatMapFunction<RowData, RowData>> lookupSideGeneratedCalc;

    protected final DataType streamSideType;
    protected final DataType lookupResultType;
    protected final DataType lookupSidePassThroughCalcType;
    protected final RowDataSerializer lookupSidePassThroughCalcRowSerializer;

    protected final int[] ownedSourceOrdinals;
    protected final int ownedLookupOrdinal;

    // used for debug
    private final String lookupHandlerDescription;

    protected transient AsyncFunction<RowData, Object> fetcher;
    protected @Nullable transient FlatMapFunction<RowData, RowData> lookupSideCalc;

    public LookupHandlerBase(
            DataType streamSideType,
            DataType lookupResultType,
            DataType lookupSidePassThroughCalcType,
            RowDataSerializer lookupSidePassThroughCalcRowSerializer,
            @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> lookupSideGeneratedCalc,
            int[] ownedSourceOrdinals,
            int ownedLookupOrdinal,
            String lookupHandlerDescription) {
        this.lookupSideGeneratedCalc = lookupSideGeneratedCalc;
        this.streamSideType = streamSideType;
        this.lookupResultType = lookupResultType;
        this.lookupSidePassThroughCalcType = lookupSidePassThroughCalcType;
        this.lookupSidePassThroughCalcRowSerializer = lookupSidePassThroughCalcRowSerializer;
        this.ownedSourceOrdinals = ownedSourceOrdinals;
        this.ownedLookupOrdinal = ownedLookupOrdinal;
        this.lookupHandlerDescription = lookupHandlerDescription;
    }

    @Override
    public void open(OpenContext openContext, DeltaJoinHandlerContext handlerContext)
            throws Exception {
        super.open(openContext, handlerContext);

        RuntimeContext runtimeContext = handlerContext.getRuntimeContext();
        ClassLoader cl = runtimeContext.getUserCodeClassLoader();

        fetcher = handlerContext.getLookupFunction(ownedLookupOrdinal);

        if (lookupSideGeneratedCalc != null) {
            lookupSideCalc = lookupSideGeneratedCalc.newInstance(cl);
            FunctionUtils.setFunctionRuntimeContext(lookupSideCalc, runtimeContext);
            FunctionUtils.openFunction(lookupSideCalc, openContext);
        }
    }

    /**
     * Complete the result after lookup.
     *
     * <p>Different with {@link #completeResultsInMailbox}, this method is called by lookup source.
     * It is likely to be executed within the connector's own thread pool.
     *
     * @param input the input row used to trigger lookup
     * @param lookupResult the lookup result
     */
    public final void complete(RowData input, Collection<RowData> lookupResult) {
        handlerContext
                .getMailboxExecutor()
                .execute(
                        () -> completeResultsInMailbox(input, lookupResult),
                        "Lookup results in LookupHandlerBase with input %s are %s",
                        input,
                        lookupResult);
    }

    /**
     * Complete the result after lookup in mailbox thread.
     *
     * <p>Different with {@link #complete}, this method is executed in mailbox thread.
     *
     * @param input the input row used to trigger lookup
     * @param lookupResult the lookup result
     */
    protected abstract void completeResultsInMailbox(
            RowData input, Collection<RowData> lookupResult);

    protected Object2RowDataConverterResultFuture createLookupResultFuture(RowData input)
            throws Exception {
        return new Object2RowDataConverterResultFuture(
                input,
                createConverter(
                        lookupResultType,
                        handlerContext.getRuntimeContext().getUserCodeClassLoader()),
                createCalcFunction(),
                createCalcCollector(),
                this,
                lookupHandlerDescription,
                streamSideType,
                lookupSidePassThroughCalcType);
    }

    @Nullable
    private FlatMapFunction<RowData, RowData> createCalcFunction() throws Exception {
        FlatMapFunction<RowData, RowData> calc = null;
        if (lookupSideGeneratedCalc != null) {
            calc =
                    lookupSideGeneratedCalc.newInstance(
                            handlerContext.getRuntimeContext().getUserCodeClassLoader());
            FunctionUtils.setFunctionRuntimeContext(calc, handlerContext.getRuntimeContext());
            FunctionUtils.openFunction(calc, openContext);
        }
        return calc;
    }

    @Nullable
    private CalcCollectionCollector createCalcCollector() {
        CalcCollectionCollector calcCollector = null;
        if (lookupSideGeneratedCalc != null) {
            calcCollector = new CalcCollectionCollector(lookupSidePassThroughCalcRowSerializer);
            calcCollector.reset();
        }
        return calcCollector;
    }

    @VisibleForTesting
    public AsyncFunction<RowData, Object> getFetcher() {
        return fetcher;
    }

    /**
     * A result future helps to convert {@link Object} to {@link RowData} after looking up and apply
     * the calc on the lookup table if necessary.
     */
    @VisibleForTesting
    public static class Object2RowDataConverterResultFuture implements ResultFuture<Object> {

        private final RowData input;
        private final DataStructureConverter<RowData, Object> lookupResultConverter;
        private final @Nullable FlatMapFunction<RowData, RowData> calcFunction;
        private final @Nullable CalcCollectionCollector calcCollector;
        private final LookupHandlerBase lookupResultCallBack;

        // used for debug
        private final String lookupHandlerDescription;
        private final DataType streamSideType;
        private final DataType lookupSidePassThroughCalcType;

        private @Nullable transient DataStructureConverter<RowData, Object> streamSideConverter;
        private @Nullable transient DataStructureConverter<RowData, Object>
                lookupSidePassThroughCalcConverter;

        private Object2RowDataConverterResultFuture(
                RowData input,
                DataStructureConverter<RowData, Object> lookupResultConverter,
                @Nullable FlatMapFunction<RowData, RowData> calcFunction,
                @Nullable CalcCollectionCollector calcCollector,
                LookupHandlerBase lookupResultCallBack,
                String lookupHandlerDescription,
                DataType streamSideType,
                DataType lookupSidePassThroughCalcType) {
            this.input = input;
            this.lookupResultConverter = lookupResultConverter;
            this.calcFunction = calcFunction;
            this.calcCollector = calcCollector;
            this.lookupResultCallBack = lookupResultCallBack;

            this.lookupHandlerDescription = lookupHandlerDescription;
            this.streamSideType = streamSideType;
            this.lookupSidePassThroughCalcType = lookupSidePassThroughCalcType;

            Preconditions.checkArgument(
                    (calcFunction == null && calcCollector == null)
                            || (calcFunction != null && calcCollector != null));
        }

        @Override
        public void complete(Collection<Object> result) {
            if (result == null) {
                result = Collections.emptyList();
            }
            Collection<RowData> lookupRows = convertToInternalData(lookupResultConverter, result);
            Collection<RowData> lookupRowsAfterCalc = passThroughCalc(lookupRows);

            printDebugMessageIfNecessary(lookupRows, lookupRowsAfterCalc);

            lookupResultCallBack.complete(input, lookupRowsAfterCalc);
        }

        @Override
        public void completeExceptionally(Throwable error) {
            lookupResultCallBack.completeExceptionally(error);
        }

        /**
         * Unsupported, because the containing classes are AsyncFunctions which don't have access to
         * the mailbox to invoke from the caller thread.
         */
        @Override
        public void complete(CollectionSupplier<Object> supplier) {
            throw new UnsupportedOperationException();
        }

        @VisibleForTesting
        @Nullable
        public CalcCollectionCollector getCalcCollector() {
            return calcCollector;
        }

        @VisibleForTesting
        @Nullable
        public FlatMapFunction<RowData, RowData> getCalcFunction() {
            return calcFunction;
        }

        private Collection<RowData> passThroughCalc(Collection<RowData> data) {
            if (calcFunction == null) {
                return data;
            }
            for (RowData row : data) {
                try {
                    calcFunction.flatMap(row, calcCollector);
                } catch (Exception e) {
                    completeExceptionally(e);
                    return Collections.emptyList();
                }
            }
            return calcCollector.getCollection();
        }

        private void printDebugMessageIfNecessary(
                Collection<RowData> lookupRows, Collection<RowData> lookupRowsAfterCalc) {
            if (!LOG.isDebugEnabled()) {
                return;
            }

            ClassLoader cl =
                    lookupResultCallBack
                            .handlerContext
                            .getRuntimeContext()
                            .getUserCodeClassLoader();

            // init converter for first debug
            if (null == streamSideConverter) {
                streamSideConverter = createConverter(streamSideType, cl);
            }
            if (null == lookupSidePassThroughCalcConverter) {
                lookupSidePassThroughCalcConverter =
                        createConverter(lookupSidePassThroughCalcType, cl);
            }

            LOG.debug(
                    DebugInfo.of(
                                    lookupHandlerDescription,
                                    lookupResultCallBack.handlerContext.inLeft2RightLookupChain(),
                                    convertToExternalData(streamSideConverter, input),
                                    convertToExternalData(lookupResultConverter, lookupRows),
                                    convertToExternalData(
                                            lookupSidePassThroughCalcConverter,
                                            lookupRowsAfterCalc))
                            .toString());
        }
    }

    private static DataStructureConverter<RowData, Object> createConverter(
            DataType dataType, ClassLoader cl) {
        DataStructureConverter<?, ?> lookupResultFetcherConverter =
                DataStructureConverters.getConverter(dataType);
        lookupResultFetcherConverter.open(cl);
        return (DataStructureConverter) lookupResultFetcherConverter;
    }

    private static Collection<RowData> convertToInternalData(
            DataStructureConverter<RowData, Object> converter, Collection<Object> data) {
        if (converter.isIdentityConversion()) {
            return (Collection) data;
        } else {
            Collection<RowData> result = new ArrayList<>(data.size());
            for (Object element : data) {
                result.add(converter.toInternal(element));
            }
            return result;
        }
    }

    private static Object convertToExternalData(
            DataStructureConverter<RowData, Object> converter, RowData data) {
        return convertToExternalData(converter, Collections.singletonList(data)).iterator().next();
    }

    private static Collection<Object> convertToExternalData(
            DataStructureConverter<RowData, Object> converter, Collection<RowData> data) {
        Collection<Object> result = new ArrayList<>(data.size());
        for (RowData element : data) {
            result.add(converter.toExternal(element));
        }
        return result;
    }

    /** A class used to format debug information in a structured manner when printed. */
    private static class DebugInfo {
        private final String lookupHandlerDescription;
        private final boolean inLeftSideLookupChain;
        private final Object input;
        private final Collection<Object> lookupResults;
        private final Collection<Object> lookupRowsAfterCalc;

        public DebugInfo(
                String lookupHandlerDescription,
                boolean inLeftSideLookupChain,
                Object input,
                Collection<Object> lookupResults,
                Collection<Object> lookupRowsAfterCalc) {
            this.lookupHandlerDescription = lookupHandlerDescription;
            this.inLeftSideLookupChain = inLeftSideLookupChain;
            this.input = input;
            this.lookupResults = lookupResults;
            this.lookupRowsAfterCalc = lookupRowsAfterCalc;
        }

        public static DebugInfo of(
                String lookupHandlerDescription,
                boolean inLeftSideLookupChain,
                Object input,
                Collection<Object> lookupResults,
                Collection<Object> lookupRowsAfterCalc) {
            return new DebugInfo(
                    lookupHandlerDescription,
                    inLeftSideLookupChain,
                    input,
                    lookupResults,
                    lookupRowsAfterCalc);
        }

        @Override
        public String toString() {
            return "DebugInfo{chainPosition=["
                    + (inLeftSideLookupChain ? "left" : "right")
                    + "], lookupHandlerDescription=["
                    + lookupHandlerDescription
                    + "], input=["
                    + input
                    + "], lookupResult=["
                    + lookupResults
                    + "], lookupRowsAfterCalc=["
                    + lookupRowsAfterCalc
                    + "]}";
        }
    }
}
