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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FailingIdentityMapper<T> extends RichMapFunction<T,T> implements
		Checkpointed<Integer>, CheckpointListener, Runnable {
	
	private static final Logger LOG = LoggerFactory.getLogger(FailingIdentityMapper.class);
	
	private static final long serialVersionUID = 6334389850158707313L;
	
	public static volatile boolean failedBefore;
	public static volatile boolean hasBeenCheckpointedBeforeFailure;

	private final int failCount;
	private int numElementsTotal;
	private int numElementsThisTime;
	
	private boolean failer;
	private boolean hasBeenCheckpointed;
	
	private Thread printer;
	private volatile boolean printerRunning = true;

	public FailingIdentityMapper(int failCount) {
		this.failCount = failCount;
	}

	@Override
	public void open(Configuration parameters) {
		failer = getRuntimeContext().getIndexOfThisSubtask() == 0;
		printer = new Thread(this, "FailingIdentityMapper Status Printer");
		printer.start();
	}

	@Override
	public T map(T value) throws Exception {
		numElementsTotal++;
		numElementsThisTime++;
		
		if (!failedBefore) {
			Thread.sleep(10);
			
			if (failer && numElementsTotal >= failCount) {
				hasBeenCheckpointedBeforeFailure = hasBeenCheckpointed;
				failedBefore = true;
				throw new Exception("Artificial Test Failure");
			}
		}
		return value;
	}

	@Override
	public void close() throws Exception {
		printerRunning = false;
		if (printer != null) {
			printer.interrupt();
			printer = null;
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		this.hasBeenCheckpointed = true;
	}

	@Override
	public Integer snapshotState(long checkpointId, long checkpointTimestamp) {
		return numElementsTotal;
	}

	@Override
	public void restoreState(Integer state) {
		numElementsTotal = state;
	}

	@Override
	public void run() {
		while (printerRunning) {
			try {
				Thread.sleep(5000);
			}
			catch (InterruptedException e) {
				// ignore
			}
			LOG.info("============================> Failing mapper  {}: count={}, totalCount={}",
					getRuntimeContext().getIndexOfThisSubtask(),
					numElementsThisTime, numElementsTotal);
		}
	}
}
