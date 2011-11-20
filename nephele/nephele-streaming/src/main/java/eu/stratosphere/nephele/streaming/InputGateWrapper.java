/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.streaming;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.InputGateListener;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.FileInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkInputChannel;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

public final class InputGateWrapper<T extends Record> implements InputGate<T> {

	private final TaskWrapper taskWrapper;

	private final InputGate<T> wrappedInputGate;

	InputGateWrapper(final TaskWrapper taskWrapper, final InputGate<T> wrappedInputGate) {

		this.taskWrapper = taskWrapper;
		this.wrappedInputGate = wrappedInputGate;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publishEvent(final AbstractTaskEvent event) throws IOException, InterruptedException {

		this.wrappedInputGate.publishEvent(event);
	}

	@Override
	public T readRecord(final T target) throws IOException, InterruptedException {

		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfInputChannels() {

		return this.wrappedInputGate.getNumberOfInputChannels();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void notifyRecordIsAvailable(final int channelIndex) {

		throw new IllegalStateException("notifyRecordIsAvailable is called on InputGateWrapper");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribeToEvent(final EventListener eventListener, final Class<? extends AbstractTaskEvent> eventType) {

		this.wrappedInputGate.subscribeToEvent(eventListener, eventType);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unsubscribeFromEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {

		this.wrappedInputGate.unsubscribeFromEvent(eventListener, eventType);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deliverEvent(final AbstractTaskEvent event) {

		throw new IllegalStateException("deliverEvent is called on InputGateWrapper");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.wrappedInputGate.getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelType getChannelType() {

		return this.wrappedInputGate.getChannelType();
	}

	@Override
	public GateID getGateID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseAllChannelResources() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isClosed() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isInputGate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void activateInputChannels() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public AbstractInputChannel<T> getInputChannel(int pos) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void registerInputGateListener(InputGateListener inputGateListener) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setChannelType(ChannelType channelType) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DistributionPattern getDistributionPattern() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NetworkInputChannel<T> createNetworkInputChannel(ChannelID channelID, CompressionLevel compressionLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FileInputChannel<T> createFileInputChannel(ChannelID channelID, CompressionLevel compressionLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InMemoryInputChannel<T> createInMemoryInputChannel(ChannelID channelID, CompressionLevel compressionLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getIndex() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void removeAllInputChannels() {
		// TODO Auto-generated method stub
		
	}
}
