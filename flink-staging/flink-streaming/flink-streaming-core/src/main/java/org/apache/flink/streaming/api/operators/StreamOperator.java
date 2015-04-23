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

package org.apache.flink.streaming.api.operators;

import java.io.IOException;
import java.io.Serializable;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointCommitter;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.runtime.io.IndexedReaderIterator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.runtime.tasks.StreamTaskContext;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The StreamOperator represents the base class for all operators in the
 * streaming topology.
 * 
 * @param <OUT>
 *            The output type of the operator
 */
public abstract class StreamOperator<IN, OUT> implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(StreamOperator.class);

	protected StreamTaskContext<OUT> taskContext;

	protected ExecutionConfig executionConfig = null;

	protected IndexedReaderIterator<StreamRecord<IN>> recordIterator;
	protected StreamRecordSerializer<IN> inSerializer;
	protected TypeSerializer<IN> objectSerializer;
	protected StreamRecord<IN> nextRecord;
	protected IN nextObject;

	public Collector<OUT> collector;
	protected Function userFunction;
	protected volatile boolean isRunning;

	private ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

	public StreamOperator(Function userFunction) {
		this.userFunction = userFunction;
	}

	/**
	 * Initializes the {@link StreamOperator} for input and output handling
	 * 
	 * @param taskContext
	 *            StreamTaskContext representing the vertex
	 */
	public void setup(StreamTaskContext<OUT> taskContext) {
		this.collector = taskContext.getOutputCollector();
		this.recordIterator = taskContext.getIndexedInput(0);
		this.inSerializer = taskContext.getInputSerializer(0);
		if (this.inSerializer != null) {
			this.nextRecord = inSerializer.createInstance();
			this.objectSerializer = inSerializer.getObjectSerializer();
		}
		this.taskContext = taskContext;
		this.executionConfig = taskContext.getExecutionConfig();
	}

	/**
	 * Method that will be called when the operator starts, should encode the
	 * processing logic
	 */
	public abstract void run() throws Exception;

	/*
	 * Reads the next record from the reader iterator and stores it in the
	 * nextRecord variable
	 */
	protected StreamRecord<IN> readNext() throws IOException {
		this.nextRecord = inSerializer.createInstance();
		try {
			nextRecord = recordIterator.next(nextRecord);
			try {
				nextObject = nextRecord.getObject();
			} catch (NullPointerException e) {
				// end of stream
			}
			return nextRecord;
		} catch (IOException e) {
			if (isRunning) {
				throw new RuntimeException("Could not read next record", e);
			} else {
				// Task already cancelled do nothing
				return null;
			}
		} catch (IllegalStateException e) {
			if (isRunning) {
				throw new RuntimeException("Could not read next record", e);
			} else {
				// Task already cancelled do nothing
				return null;
			}
		}
	}

	/**
	 * The call of the user implemented function should be implemented here
	 */
	protected void callUserFunction() throws Exception {
	}

	/**
	 * Method for logging exceptions thrown during the user function call
	 */
	protected void callUserFunctionAndLogException() {
		try {
			callUserFunction();
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Calling user function failed", e);
			}
			throw new RuntimeException(e);
		}
	}

	/**
	 * Open method to be used if the user defined function extends the
	 * RichFunction class
	 * 
	 * @param parameters
	 *            The configuration parameters for the operator
	 */
	public void open(Configuration parameters) throws Exception {
		isRunning = true;
		FunctionUtils.openFunction(userFunction, parameters);
	}

	/**
	 * Close method to be used if the user defined function extends the
	 * RichFunction class
	 * 
	 */
	public void close() {
		isRunning = false;
		collector.close();
		try {
			FunctionUtils.closeFunction(userFunction);
		} catch (Exception e) {
			throw new RuntimeException("Error when closing the function", e);
		}
	}

	public void cancel() {
		isRunning = false;
	}

	public void setRuntimeContext(RuntimeContext t) {
		FunctionUtils.setFunctionRuntimeContext(userFunction, t);
	}

	protected IN copy(IN record) {
		return objectSerializer.copy(record);
	}

	public void setChainingStrategy(ChainingStrategy strategy) {
		if (strategy == ChainingStrategy.ALWAYS) {
			if (!(this instanceof ChainableStreamOperator)) {
				throw new RuntimeException("Operator needs to extend ChainableOperator to be chained");
			}
		}
		this.chainingStrategy = strategy;
	}

	public ChainingStrategy getChainingStrategy() {
		return chainingStrategy;
	}

	/**
	 * Defines the chaining scheme for the operator. By default <b>ALWAYS</b> is used,
	 * which means operators will be eagerly chained whenever possible, for
	 * maximal performance. It is generally a good practice to allow maximal
	 * chaining and increase operator parallelism. </p> When the strategy is set
	 * to <b>NEVER</b>, the operator will not be chained to the preceding or succeeding
	 * operators.</p> <b>HEAD</b> strategy marks a start of a new chain, so that the
	 * operator will not be chained to preceding operators, only succeding ones.
	 * 
	 */
	public static enum ChainingStrategy {
		ALWAYS, NEVER, HEAD
	}

	public Function getUserFunction() {
		return userFunction;
	}
	
	// ------------------------------------------------------------------------
	//  Checkpoints and Checkpoint Confirmations
	// ------------------------------------------------------------------------
	
	// NOTE - ALL OF THIS CODE WORKS ONLY FOR THE FIRST OPERATOR IN THE CHAIN
	// IT NEEDS TO BE EXTENDED TO SUPPORT CHAINS
	
	public void restoreInitialState(Serializable state) throws Exception {
		if (userFunction instanceof Checkpointed) {
			setStateOnFunction(state, userFunction);
		}
		else {
			throw new IllegalStateException("Trying to restore state of a non-checkpointed function");
		}
	}
	
	public Serializable getStateSnapshotFromFunction(long checkpointId, long timestamp) throws Exception {
		if (userFunction instanceof Checkpointed) {
			return ((Checkpointed<?>) userFunction).snapshotState(checkpointId, timestamp);
		}
		else {
			return null;
		}
	}
	
	public void confirmCheckpointCompleted(long checkpointId, long timestamp) throws Exception {
		if (userFunction instanceof CheckpointCommitter) {
			try {
				((CheckpointCommitter) userFunction).commitCheckpoint(checkpointId);
			}
			catch (Exception e) {
				throw new Exception("Error while confirming checkpoint " + checkpointId + " to the stream function", e);
			}
		}
	}
	
	private static <T extends Serializable> void setStateOnFunction(Serializable state, Function function) {
		@SuppressWarnings("unchecked")
		T typedState = (T) state;
		@SuppressWarnings("unchecked")
		Checkpointed<T> typedFunction = (Checkpointed<T>) function;
		
		typedFunction.restoreState(typedState);
	}
}
