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
import java.util.List;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.OutputGateListener;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.FileOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkOutputChannel;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

public final class OutputGateWrapper<T extends Record> implements OutputGate<T> {

	private final TaskWrapper taskWrapper;

	private final OutputGate<T> wrappedOutputGate;

	OutputGateWrapper(final TaskWrapper taskWrapper, final OutputGate<T> wrappedOutputGate) {

		this.taskWrapper = taskWrapper;
		this.wrappedOutputGate = wrappedOutputGate;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribeToEvent(final EventListener eventListener, final Class<? extends AbstractTaskEvent> eventType) {

		this.wrappedOutputGate.subscribeToEvent(eventListener, eventType);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unsubscribeFromEvent(final EventListener eventListener,
			final Class<? extends AbstractTaskEvent> eventType) {

		this.wrappedOutputGate.unsubscribeFromEvent(eventListener, eventType);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publishEvent(final AbstractTaskEvent event) throws IOException, InterruptedException {

		this.wrappedOutputGate.publishEvent(event);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deliverEvent(final AbstractTaskEvent event) {

		throw new IllegalStateException("deliverEvent is called on OutputGateWrapper");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.wrappedOutputGate.getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelType getChannelType() {

		return this.wrappedOutputGate.getChannelType();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<T> getType() {

		return this.wrappedOutputGate.getType();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(final T record) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.wrappedOutputGate.writeRecord(record);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AbstractOutputChannel<T>> getOutputChannels() {

		return this.wrappedOutputGate.getOutputChannels();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flush() throws IOException, InterruptedException {

		this.wrappedOutputGate.flush();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void channelCapacityExhausted(final int channelIndex) {

		this.wrappedOutputGate.channelCapacityExhausted(channelIndex);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isBroadcast() {

		return this.wrappedOutputGate.isBroadcast();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfOutputChannels() {

		return this.wrappedOutputGate.getNumberOfOutputChannels();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelSelector<T> getChannelSelector() {

		return this.wrappedOutputGate.getChannelSelector();
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
	public AbstractOutputChannel<T> getOutputChannel(int pos) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void requestClose() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeAllOutputChannels() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setChannelType(ChannelType channelType) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public NetworkOutputChannel<T> createNetworkOutputChannel(ChannelID channelID, CompressionLevel compressionLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FileOutputChannel<T> createFileOutputChannel(ChannelID channelID, CompressionLevel compressionLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InMemoryOutputChannel<T> createInMemoryOutputChannel(ChannelID channelID, CompressionLevel compressionLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getIndex() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void registerOutputGateListener(OutputGateListener outputGateListener) {
		// TODO Auto-generated method stub
		
	}
}
