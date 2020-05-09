/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.StreamSink;

import java.util.concurrent.CompletableFuture;

/**
 * A {@link StreamSink} that collects query results and sends them back to the client.
 *
 * @param <IN> type of results to be written into the sink.
 */
public class CollectSinkOperator<IN> extends StreamSink<IN> implements OperatorEventHandler {

	private final CollectSinkFunction<IN> sinkFunction;
	// we need operator id to identify the coordinator of this operator,
	// this is only used for in clients so no need to serialize
	private final transient CompletableFuture<OperatorID> operatorIdFuture;

	public CollectSinkOperator(
			TypeSerializer<IN> serializer,
			int maxResultsPerBatch,
			String accumulatorName) {
		super(new CollectSinkFunction<>(serializer, maxResultsPerBatch, accumulatorName));
		this.sinkFunction = (CollectSinkFunction<IN>) getUserFunction();
		this.operatorIdFuture = new CompletableFuture<>();
	}

	@Override
	public void handleOperatorEvent(OperatorEvent evt) {
		// nothing to handle
	}

	@Override
	public void close() throws Exception {
		sinkFunction.accumulateFinalResults();
		super.close();
	}

	public CompletableFuture<OperatorID> getOperatorIdFuture() {
		return operatorIdFuture;
	}

	void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
		sinkFunction.setOperatorEventGateway(operatorEventGateway);
	}
}
