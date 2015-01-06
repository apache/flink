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

package org.apache.flink.tez.runtime.input;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.util.InstantiationUtil;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class FlinkInput extends AbstractLogicalInput {

	private static final Log LOG = LogFactory.getLog(FlinkInput.class);
	
	private InputSplit split;
	private boolean splitIsCreated;
	private final ReentrantLock rrLock = new ReentrantLock();
	private final Condition rrInited = rrLock.newCondition();

	public FlinkInput(InputContext inputContext, int numPhysicalInputs) {
		super(inputContext, numPhysicalInputs);
		getContext().requestInitialMemory(0l, null); // mandatory call
		split = null;
	}

	@Override
	public void handleEvents(List<Event> inputEvents) throws Exception {
		
		LOG.info("Received " + inputEvents.size() + " events (should be = 1)");
		
		Event event = inputEvents.iterator().next();
		
		Preconditions.checkArgument(event instanceof InputDataInformationEvent,
				getClass().getSimpleName()
						+ " can only handle a single event of type: "
						+ InputDataInformationEvent.class.getSimpleName());

		initSplitFromEvent ((InputDataInformationEvent)event);
	}

	private void initSplitFromEvent (InputDataInformationEvent e) throws Exception {
		rrLock.lock();

		try {
			ByteString byteString = ByteString.copyFrom(e.getUserPayload());
			this.split =  (InputSplit) InstantiationUtil.deserializeObject(byteString.toByteArray(), getClass().getClassLoader());
			this.splitIsCreated = true;
			
			LOG.info ("Initializing input split " + split.getSplitNumber() + ": " + split.toString() + " from event (" + e.getSourceIndex() + "," + e.getTargetIndex() + "): " + e.toString());
			
			rrInited.signal();
		}
		catch (Exception ex) {
			throw new IOException(
					"Interrupted waiting for InputSplit initialization");
		}
		finally {
			rrLock.unlock();
		}
	}

	@Override
	public List<Event> close() throws Exception {
		return null;
	}

	@Override
	public void start() throws Exception {
	}

	@Override
	public Reader getReader() throws Exception {
		throw new RuntimeException("FlinkInput does not contain a Reader. Should use getSplit instead");
	}

	@Override
	public List<Event> initialize() throws Exception {
		return null;
	}

	public InputSplit getSplit () throws Exception {

		rrLock.lock();
		try {
			if (!splitIsCreated) {
				checkAndAwaitSplitInitialization();
			}
		}
		finally {
			rrLock.unlock();
		}
		if (split == null) {
			LOG.info("Input split has not been created. This should not happen");
			throw new RuntimeException("Input split has not been created. This should not happen");
		}
		return split;
	}

	void checkAndAwaitSplitInitialization() throws IOException {
		assert rrLock.getHoldCount() == 1;
		rrLock.lock();
		try {
			rrInited.await();
		} catch (Exception e) {
			throw new IOException(
					"Interrupted waiting for InputSplit initialization");
		} finally {
			rrLock.unlock();
		}
	}
}
