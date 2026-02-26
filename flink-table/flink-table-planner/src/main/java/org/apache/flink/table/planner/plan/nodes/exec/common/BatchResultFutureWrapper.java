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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Wrapper to adapt single result future to batch result future interface.
 * This is a temporary solution until proper batch code generation is implemented.
 */
public class BatchResultFutureWrapper extends GeneratedResultFuture<TableFunctionResultFuture<List<RowData>>> {
    
    private final GeneratedResultFuture<TableFunctionResultFuture<RowData>> singleResultFuture;
    
    public BatchResultFutureWrapper(GeneratedResultFuture<TableFunctionResultFuture<RowData>> singleResultFuture) {
        super(singleResultFuture.getClassName(), singleResultFuture.getCode(), singleResultFuture.getReferences());
        this.singleResultFuture = singleResultFuture;
    }
    
    @Override
    public TableFunctionResultFuture<List<RowData>> newInstance(ClassLoader classLoader) {
        TableFunctionResultFuture<RowData> singleFuture = singleResultFuture.newInstance(classLoader);
        return new BatchTableFunctionResultFutureAdapter(singleFuture);
    }
    
    private static class BatchTableFunctionResultFutureAdapter extends TableFunctionResultFuture<List<RowData>> {
        
        private final TableFunctionResultFuture<RowData> singleFuture;
        
        public BatchTableFunctionResultFutureAdapter(TableFunctionResultFuture<RowData> singleFuture) {
            this.singleFuture = singleFuture;
        }
        
        @Override
        public void setInput(Object input) {
            // For batch processing, input should be a List<RowData>
            if (input instanceof List) {
                @SuppressWarnings("unchecked")
                List<RowData> batchInput = (List<RowData>) input;
                // For simplicity, we'll process the first row
                // In a full implementation, this would need to handle all rows
                if (!batchInput.isEmpty()) {
                    singleFuture.setInput(batchInput.get(0));
                }
            } else {
                singleFuture.setInput(input);
            }
        }
        
        @Override
        public void setResultFuture(org.apache.flink.streaming.api.functions.async.ResultFuture<?> resultFuture) {
            singleFuture.setResultFuture(resultFuture);
        }
        
        @Override
        public void complete(java.util.Collection<List<RowData>> result) {
            // Convert batch result to single result format
            List<RowData> singleResult = new ArrayList<>();
            if (result != null) {
                for (List<RowData> batch : result) {
                    if (batch != null && !batch.isEmpty()) {
                        singleResult.add(batch.get(0));
                    }
                }
            }
            singleFuture.complete(singleResult);
        }
        
        @Override
        public void completeExceptionally(Throwable error) {
            singleFuture.completeExceptionally(error);
        }
        
        @Override
        public void close() throws Exception {
            singleFuture.close();
        }
    }
}