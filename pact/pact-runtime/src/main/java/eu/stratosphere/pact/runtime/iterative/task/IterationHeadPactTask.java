/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.AbstractRecordWriter;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypePairComparator;
import eu.stratosphere.pact.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.io.InputViewIterator;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannel;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannelBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.Broker;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationAggregatorBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.SolutionsetBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.SuperstepBarrier;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.event.WorkerDoneEvent;
import eu.stratosphere.pact.runtime.iterative.io.SerializedUpdateBuffer;
//import eu.stratosphere.pact.runtime.iterative.monitoring.IterationMonitoring;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;

/**
 * The head is responsible for coordinating an iteration and can run a
 * {@link eu.stratosphere.pact.runtime.task.PactDriver} inside. It will read
 * the initial input and establish a {@link BlockingBackChannel} to the iteration's tail. After successfully processing
 * the input, it will send {@link EndOfSuperstepEvent} events to its outputs. It must also be connected to a
 * synchronization task and after each superstep, it will wait
 * until it receives an {@link AllWorkersDoneEvent} from the sync, which signals that all other heads have also finished
 * their iteration. Starting with
 * the second iteration, the input for the head is the output of the tail, transmitted through the backchannel. Once the
 * iteration is done, the head
 * will send a {@link TerminationEvent} to all it's connected tasks, signaling them to shutdown.
 * <p>
 * Assumption on the ordering of the outputs: - The first n output gates write to channels that go to the tasks of the
 * step function. - The next m output gates to to the tasks that consume the final solution. - The last output gate
 * connects to the synchronization task.
 * 
 * @param <X> The type of the bulk partial solution / solution set and the final output.
 * @param <Y> The type of the feed-back data set (bulk partial solution / workset). For bulk iterations, {@code Y} is the same as {@code X}
 */
public class IterationHeadPactTask<X, Y, S extends Stub, OT> extends AbstractIterativePactTask<S, OT> {
	
	private static final Log log = LogFactory.getLog(IterationHeadPactTask.class);

	
	private Collector<X> finalOutputCollector;
	
	private List<AbstractRecordWriter<?>> finalOutputWriters;
	
	private TypeSerializer<Y> feedbackTypeSerializer;
	
	private TypeSerializer<X> solutionTypeSerializer;
	
	private RecordWriter<?> toSync;
	
	private boolean isWorksetIteration;
	
	private int initialSolutionSetInput;	// undefined for bulk iterations
	
	private int feedbackDataInput;	// workset or bulk partial solution
	
	private RuntimeAggregatorRegistry aggregatorRegistry;

	// --------------------------------------------------------------------------------------------
	
	@Override
	protected int getNumTaskInputs() {
		// this task has an additional input in the workset case for the initial solution set
		boolean isWorkset = config.isWorksetIteration();
		return driver.getNumberOfInputs() + (isWorkset ? 1 : 0);
	}
	
	@Override
	protected void initOutputs() throws Exception {
		// initialize the regular outputs first (the ones into the step function).
		super.initOutputs();
		
		// at this time, the outputs to the step function are created
		// add the outputs for the final solution
		this.finalOutputWriters = new ArrayList<AbstractRecordWriter<?>>();
		final TaskConfig finalOutConfig = this.config.getIterationHeadFinalOutputConfig();
		this.finalOutputCollector = RegularPactTask.getOutputCollector(this, finalOutConfig,
				this.userCodeClassLoader, this.finalOutputWriters, finalOutConfig.getNumOutputs());
		
		// sanity check the setup
		final int writersIntoStepFunction = this.eventualOutputs.size();
		final int writersIntoFinalResult = this.finalOutputWriters.size();
		final int syncGateIndex = this.config.getIterationHeadIndexOfSyncOutput();
		
		if (writersIntoStepFunction + writersIntoFinalResult != syncGateIndex) {
			throw new Exception("Error: Inconsist head task setup - wrong mapping of output gates.");
		}
		// now, we can instantiate the sync gate
		this.toSync = new RecordWriter<Record>(this, Record.class);
	}
	
	/**
	 * the iteration head prepares the backchannel: it allocates memory, instantiates a {@link BlockingBackChannel} and
	 * hands it to the iteration tail via a {@link Broker} singleton
	 **/
	private BlockingBackChannel initBackChannel() throws Exception {

		/* get the size of the memory available to the backchannel */
		long backChannelMemorySize = this.config.getBackChannelMemory();

		/* allocate the memory available to the backchannel */
		List<MemorySegment> segments = new ArrayList<MemorySegment>();
		int segmentSize = getMemoryManager().getPageSize();
		getMemoryManager().allocatePages(this, segments, backChannelMemorySize);

		/* instantiate the backchannel */
		BlockingBackChannel backChannel = new BlockingBackChannel(new SerializedUpdateBuffer(segments, segmentSize,
			getIOManager()));

		/* hand the backchannel over to the iteration tail */
		Broker<BlockingBackChannel> broker = BlockingBackChannelBroker.instance();
		broker.handIn(brokerKey(), backChannel);

		return backChannel;
	}

	private <BT, PT> MutableHashTable<BT, PT> initHashTable() throws Exception {
		// get some memory
		long hashjoinMemorySize =  config.getSolutionSetMemory();

		TypeSerializerFactory<BT> solutionTypeSerializerFactory = config.getSolutionSetSerializer(userCodeClassLoader);
		TypeSerializerFactory<PT> probeSideSerializerFactory = config.getSolutionSetProberSerializer(userCodeClassLoader);
		TypeComparatorFactory<BT> solutionTypeComparatorFactory = config.getSolutionSetComparator(userCodeClassLoader);
		TypeComparatorFactory<PT> probeSideComparatorFactory = config.getSolutionSetProberComparator(userCodeClassLoader);
		TypePairComparatorFactory<BT, PT> pairComparatorFactory = config.getSolutionSetPairComparatorFactory(userCodeClassLoader);
		
		TypeSerializer<BT> solutionTypeSerializer = solutionTypeSerializerFactory.getSerializer();
		TypeSerializer<PT> probeSideSerializer = probeSideSerializerFactory.getSerializer();
		TypeComparator<BT> solutionTypeComparator = solutionTypeComparatorFactory.createComparator();
		TypeComparator<PT> probeSideComparator = probeSideComparatorFactory.createComparator();
		TypePairComparator<PT, BT> pairComparator = pairComparatorFactory.createComparator21(solutionTypeComparator, probeSideComparator);
	
		MutableHashTable<BT, PT> hashTable = null;
		List<MemorySegment> memSegments = null;
		boolean success = false;
		try {
			memSegments = getMemoryManager().allocatePages(getOwningNepheleTask(), hashjoinMemorySize);
			hashTable = new MutableHashTable<BT, PT>(solutionTypeSerializer, probeSideSerializer, solutionTypeComparator,
			probeSideComparator, pairComparator, memSegments, getIOManager());
			success = true;
			return hashTable;
		}
		finally {
			if (!success) {
				if (hashTable != null) {
					try {
						hashTable.close();
					} catch (Throwable t) {
						log.error("Error closing the solution set hash table after unsuccessful creation.", t);
					}
				}
				if (memSegments != null) {
					try {
						getMemoryManager().release(memSegments);
					} catch (Throwable t) {
						log.error("Error freeing memory after error during solution set hash table creation.", t);
					}
				}
			}
		}
	}
	
	private <T> void readInitialSolutionSet(MutableHashTable<X, T> solutionSet, MutableObjectIterator<X> solutionSetInput) throws IOException {
		MutableObjectIterator<T> emptyInput = EmptyMutableObjectIterator.get();
		solutionSet.open(solutionSetInput, emptyInput);
	}

	private SuperstepBarrier initSuperstepBarrier() {
		SuperstepBarrier barrier = new SuperstepBarrier();
		this.toSync.subscribeToEvent(barrier, AllWorkersDoneEvent.class);
		this.toSync.subscribeToEvent(barrier, TerminationEvent.class);
		return barrier;
	}

	@Override
	public void run() throws Exception {

		final String brokerKey = brokerKey();
		final int workerIndex = getEnvironment().getIndexInSubtaskGroup();
		
		MutableHashTable<X, ?> solutionSet = null; // if workset iteration

		try {
			/* used for receiving the current iteration result from iteration tail */
			BlockingBackChannel backChannel = initBackChannel();
			SuperstepBarrier barrier = initSuperstepBarrier();
			
			isWorksetIteration = config.isWorksetIteration();
			
			feedbackDataInput = config.getIterationHeadPartialSolutionOrWorksetInputIndex();
			feedbackTypeSerializer = getInputSerializer(feedbackDataInput);
			excludeFromReset(feedbackDataInput);
			
			if (isWorksetIteration) {
				initialSolutionSetInput = config.getIterationHeadSolutionSetInputIndex();
				TypeSerializerFactory<X> solutionTypeSerializerFactory = config.getSolutionSetSerializer(userCodeClassLoader);
				solutionTypeSerializer = solutionTypeSerializerFactory.getSerializer();
				
				// setup the index for the solution set
				solutionSet = initHashTable();
				
				// read the initial solution set
				@SuppressWarnings("unchecked")
				MutableObjectIterator<X> solutionSetInput = (MutableObjectIterator<X>) createInputIterator(
					initialSolutionSetInput, inputReaders[initialSolutionSetInput], solutionTypeSerializer);
				readInitialSolutionSet(solutionSet, solutionSetInput);
				
				Broker<MutableHashTable<?, ?>> solutionsetBroker = SolutionsetBroker.instance();
				solutionsetBroker.handIn(brokerKey(), solutionSet);
			} else {
				// bulk iteration case
				initialSolutionSetInput = -1;
				
				@SuppressWarnings("unchecked")
				TypeSerializer<X> solSer = (TypeSerializer<X>) feedbackTypeSerializer;
				solutionTypeSerializer = solSer;
			}
			
			// instantiate all aggregators and register them at the iteration global registry
			aggregatorRegistry = new RuntimeAggregatorRegistry(config.getIterationAggregators());
			IterationAggregatorBroker.instance().handIn(brokerKey(), aggregatorRegistry);
			
			DataInputView superstepResult = null;
	
			while (this.running && !terminationRequested()) {
	
	//			notifyMonitor(IterationMonitoring.Event.HEAD_STARTING);
				if (log.isInfoEnabled()) {
					log.info(formatLogString("starting iteration [" + currentIteration() + "]"));
				}
	
				barrier.setup();
	
	//			notifyMonitor(IterationMonitoring.Event.HEAD_PACT_STARTING);
				if (!inFirstIteration()) {
					feedBackSuperstepResult(superstepResult);
				}
	
				super.run();
	//			notifyMonitor(IterationMonitoring.Event.HEAD_PACT_FINISHED);
	
				EndOfSuperstepEvent endOfSuperstepEvent = new EndOfSuperstepEvent();
	
				// signal to connected tasks that we are done with the superstep
				sendEventToAllIterationOutputs(endOfSuperstepEvent);
	
				// blocking call to wait for the result
				superstepResult = backChannel.getReadEndAfterSuperstepEnded();
				if (log.isInfoEnabled()) {
					log.info(formatLogString("finishing iteration [" + currentIteration() + "]"));
				}
	
				sendEventToSync(new WorkerDoneEvent(workerIndex, aggregatorRegistry.getAllAggregators()));
	
	//			notifyMonitor(IterationMonitoring.Event.HEAD_FINISHED);
	
	//			notifyMonitor(IterationMonitoring.Event.HEAD_WAITING_FOR_OTHERS);
				if (log.isInfoEnabled()) {
					log.info(formatLogString("waiting for other workers in iteration [" + currentIteration() + "]"));
				}
	
				barrier.waitForOtherWorkers();
	
				if (barrier.terminationSignaled()) {
					if (log.isInfoEnabled()) {
						log.info(formatLogString("head received termination request in iteration [" + currentIteration()
							+ "]"));
					}
					requestTermination();
					sendEventToAllIterationOutputs(new TerminationEvent());
				} else {
					incrementIterationCounter();
					
					String[] globalAggregateNames = barrier.getAggregatorNames();
					Value[] globalAggregates = barrier.getAggregates();
					aggregatorRegistry.updateGlobalAggregatesAndReset(globalAggregateNames, globalAggregates);
				}
			}
	
			if (log.isInfoEnabled()) {
				log.info(formatLogString("streaming out final result after [" + currentIteration() + "] iterations"));
			}
	
			if (isWorksetIteration) {
				streamSolutionSetToFinalOutput(solutionSet);
			} else {
				streamOutFinalOutputBulk(new InputViewIterator<X>(superstepResult, this.solutionTypeSerializer));
			}
			
		} finally {
			// make sure we unregister everything from the broker:
			// - backchannel
			// - aggregator registry
			// - solution set index
			IterationAggregatorBroker.instance().remove(brokerKey);
			BlockingBackChannelBroker.instance().remove(brokerKey);
			if (isWorksetIteration) {
				SolutionsetBroker.instance().remove(brokerKey);
			}
			if (solutionSet != null) {
				solutionSet.close();
				solutionSet = null;
			}
		}
	}


	private void streamOutFinalOutputBulk(MutableObjectIterator<X> results) throws IOException {
		final Collector<X> out = this.finalOutputCollector;
		final X record = this.solutionTypeSerializer.createInstance();
//		int recordsPut = 0;
		
		while (results.next(record)) {
			out.collect(record);
//			recordsPut++;
		}

//		if (log.isInfoEnabled()) {
//			log.info(formatLogString("Sent [" + recordsPut + "] records to final output"));
//		}
	}

	private void streamSolutionSetToFinalOutput(MutableHashTable<X, ?> hashTable) throws IOException, InterruptedException {
		final MutableObjectIterator<X> results = hashTable.getPartitionEntryIterator();
		final Collector<X> output = this.finalOutputCollector;
		final X record = solutionTypeSerializer.createInstance();
//		int recordsPut = 0;
		
		while (results.next(record)) {
			output.collect(record);
//			recordsPut++;
		}
		
//		if (log.isInfoEnabled()) {
//			log.info(formatLogString("Sent [" + recordsPut + "] records from solution set to final output"));
//		}
	}

	private void feedBackSuperstepResult(DataInputView superstepResult) {
		this.inputs[this.feedbackDataInput] = 
				new InputViewIterator<Y>(superstepResult, this.feedbackTypeSerializer);
	}

	private void sendEventToAllIterationOutputs(AbstractTaskEvent event) throws IOException, InterruptedException {
		if (log.isInfoEnabled()) {
			log.info(formatLogString("sending " + event.getClass().getSimpleName() + " to all iteration outputs"));
		}

		for (int outputIndex = 0; outputIndex < this.eventualOutputs.size(); outputIndex++) {
			this.eventualOutputs.get(outputIndex).publishEvent(event);
		}
	}

	private void sendEventToSync(WorkerDoneEvent event) throws IOException, InterruptedException {
		if (log.isInfoEnabled()) {
			log.info(formatLogString("sending " + WorkerDoneEvent.class.getSimpleName() + " to sync"));
		}
		this.toSync.publishEvent(event);
	}

}
