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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Wrapper to adapt single lookup function to batch lookup interface.
 * This is a temporary solution until proper batch code generation is implemented.
 * 
 * <p>This wrapper allows existing single-record lookup functions to work with
 * the batch lookup join infrastructure without requiring immediate changes to
 * the code generation framework. It processes each record in the batch individually
 * and collects the results.
 * 
 * <p><strong>Note:</strong> This is not the most efficient implementation as it
 * doesn't take full advantage of batch processing at the connector level.
 * Future versions should implement proper batch code generation for optimal performance.
 */
public class BatchLookupFunctionWrapper extends GeneratedFunction<AsyncFunction<List<RowData>, Object>> {
    
    private final GeneratedFunction<AsyncFunction<RowData, Object>> singleLookupFunction;
    
    public BatchLookupFunctionWrapper(GeneratedFunction<AsyncFunction<RowData, Object>> singleLookupFunction) {
        super(singleLookupFunction.getClassName(), singleLookupFunction.getCode(), singleLookupFunction.getReferences());
        this.singleLookupFunction = singleLookupFunction;
    }
    
    @Override
    public AsyncFunction<List<RowData>, Object> newInstance(ClassLoader classLoader) {
        AsyncFunction<RowData, Object> singleFunction = singleLookupFunction.newInstance(classLoader);
        return new BatchAsyncFunctionAdapter(singleFunction);
    }
    
    private static class BatchAsyncFunctionAdapter implements AsyncFunction<List<RowData>, Object> {
        
        private final AsyncFunction<RowData, Object> singleFunction;
        
        public BatchAsyncFunctionAdapter(AsyncFunction<RowData, Object> singleFunction) {
            this.singleFunction = singleFunction;
        }
        
        @Override
        public void asyncInvoke(List<RowData> input, ResultFuture<Object> resultFuture) throws Exception {
            List<Object> results = new ArrayList<>();
            BatchResultCollector collector = new BatchResultCollector(results, input.size(), resultFuture);
            
            // Process each row in the batch
            for (int i = 0; i < input.size(); i++) {
                RowData row = input.get(i);
                SingleResultFuture singleResultFuture = new SingleResultFuture(collector, i);
                singleFunction.asyncInvoke(row, singleResultFuture);
            }
        }
    }
    
    private static class BatchResultCollector {
        private final List<Object> results;
        private final int expectedCount;
        private final ResultFuture<Object> resultFuture;
        private int completedCount = 0;
        
        public BatchResultCollector(List<Object> results, int expectedCount, ResultFuture<Object> resultFuture) {
            this.results = results;
            this.expectedCount = expectedCount;
            this.resultFuture = resultFuture;
            // Initialize results list with nulls
            for (int i = 0; i < expectedCount; i++) {
                results.add(null);
            }
        }
        
        public synchronized void complete(int index, Collection<Object> result) {
            if (result != null && !result.isEmpty()) {
                results.set(index, result.iterator().next());
            }
            completedCount++;
            
            if (completedCount == expectedCount) {
                resultFuture.complete(results);
            }
        }
        
        public synchronized void completeExceptionally(Throwable error) {
            resultFuture.completeExceptionally(error);
        }
    }
    
    private static class SingleResultFuture implements ResultFuture<Object> {
        private final BatchResultCollector collector;
        private final int index;
        
        public SingleResultFuture(BatchResultCollector collector, int index) {
            this.collector = collector;
            this.index = index;
        }
        
        @Override
        public void complete(Collection<Object> result) {
            collector.complete(index, result);
        }
        
        @Override
        public void completeExceptionally(Throwable error) {
            collector.completeExceptionally(error);
        }
    }
}