/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.accumulators.LongCounter;
import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.accumulators.AccumulatorEvent;
import eu.stratosphere.pact.runtime.hash.CompactingHashTable;
import eu.stratosphere.pact.runtime.io.InputViewIterator;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannel;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannelBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.Broker;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationAccumulatorBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.SolutionSetBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.SolutionSetUpdateBarrier;
import eu.stratosphere.pact.runtime.iterative.concurrent.SolutionSetUpdateBarrierBroker;
import eu.stratosphere.pact.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.event.WorkerDoneEvent;
import eu.stratosphere.pact.runtime.iterative.io.SerializedUpdateBuffer;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.runtime.io.api.BufferWriter;
import eu.stratosphere.runtime.io.channels.EndOfSuperstepEvent;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

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
 * @param <X>
 *        The type of the bulk partial solution / solution set and the final output.
 * @param <Y>
 *        The type of the feed-back data set (bulk partial solution / workset). For bulk iterations, {@code Y} is the
 *        same as {@code X}
 */
public class IterationHeadPactTask<X, Y, S extends Function, OT> extends AbstractIterativePactTask<S, OT> {
	
	// constants for thread synchronization
	public static final int TERMINATION_REQUEST = 1;
	public static final int NEXT_SUPERSTEP_REQUEST = 2;
	private static final int AWAITING_REQUEST = 3;

	private static final Log log = LogFactory.getLog(IterationHeadPactTask.class);

	private Collector<X> finalOutputCollector;

	private List<BufferWriter> finalOutputWriters;

	private TypeSerializerFactory<Y> feedbackTypeSerializer;

	private TypeSerializerFactory<X> solutionTypeSerializer;

	private int initialSolutionSetInput; // undefined for bulk iterations

	private int feedbackDataInput; // workset or bulk partial solution

	private RuntimeAccumulatorRegistry accumulatorRegistry;
	
	private final AtomicInteger instructionSynchronizer = new AtomicInteger(3);
	
	private AllWorkersDoneEvent lastGlobalState = null;

	// --------------------------------------------------------------------------------------------

	@Override
	protected int getNumTaskInputs() {
		// this task has an additional input in the workset case for the initial solution set
		boolean isWorkset = config.getIsWorksetIteration();
		return driver.getNumberOfInputs() + (isWorkset ? 1 : 0);
	}

	@Override
	protected void initOutputs() throws Exception {
		// initialize the regular outputs first (the ones into the step function).
		super.initOutputs();

		// at this time, the outputs to the step function are created
		// add the outputs for the final solution
		this.finalOutputWriters = new ArrayList<BufferWriter>();
		final TaskConfig finalOutConfig = this.config.getIterationHeadFinalOutputConfig();
		this.finalOutputCollector = RegularPactTask.getOutputCollector(this, finalOutConfig,
			this.userCodeClassLoader, this.finalOutputWriters, finalOutConfig.getNumOutputs());
	}

	/**
	 * the iteration head prepares the backchannel: it allocates memory, instantiates a {@link BlockingBackChannel} and
	 * hands it to the iteration tail via a {@link Broker} singleton
	 **/
	private BlockingBackChannel initBackChannel() throws Exception {

		/* get the size of the memory available to the backchannel */
		int backChannelMemoryPages = getMemoryManager().computeNumberOfPages(this.config.getBackChannelMemory());

		/* allocate the memory available to the backchannel */
		List<MemorySegment> segments = new ArrayList<MemorySegment>();
		int segmentSize = getMemoryManager().getPageSize();
		getMemoryManager().allocatePages(this, segments, backChannelMemoryPages);

		/* instantiate the backchannel */
		BlockingBackChannel backChannel = new BlockingBackChannel(new SerializedUpdateBuffer(segments, segmentSize,
			getIOManager()));

		/* hand the backchannel over to the iteration tail */
		Broker<BlockingBackChannel> broker = BlockingBackChannelBroker.instance();
		broker.handIn(brokerKey(), backChannel);

		return backChannel;
	}
	
	private <BT> CompactingHashTable<BT> initCompactingHashTable() throws Exception {
		// get some memory
		long hashjoinMemorySize = config.getSolutionSetMemory();

		TypeSerializerFactory<BT> solutionTypeSerializerFactory = config.getSolutionSetSerializer(userCodeClassLoader);
		TypeComparatorFactory<BT> solutionTypeComparatorFactory = config.getSolutionSetComparator(userCodeClassLoader);
	
		TypeSerializer<BT> solutionTypeSerializer = solutionTypeSerializerFactory.getSerializer();
		TypeComparator<BT> solutionTypeComparator = solutionTypeComparatorFactory.createComparator();

		CompactingHashTable<BT> hashTable = null;
		List<MemorySegment> memSegments = null;
		boolean success = false;
		try {
			int numPages = getMemoryManager().computeNumberOfPages(hashjoinMemorySize);
			memSegments = getMemoryManager().allocatePages(getOwningNepheleTask(), numPages);
			hashTable = new CompactingHashTable<BT>(solutionTypeSerializer, solutionTypeComparator, memSegments);
			success = true;
			return hashTable;
		} finally {
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
	
	private void readInitialSolutionSet(CompactingHashTable<X> solutionSet, MutableObjectIterator<X> solutionSetInput) throws IOException {
		solutionSet.open();
		solutionSet.buildTable(solutionSetInput);
	}

	@Override
	public void run() throws Exception {
		// initialize the serializers (one per channel) of the record writers
		RegularPactTask.initOutputWriters(this.finalOutputWriters);

		final String brokerKey = brokerKey();
		final int workerIndex = getEnvironment().getIndexInSubtaskGroup();

		//MutableHashTable<X, ?> solutionSet = null; // if workset iteration
		CompactingHashTable<X> solutionSet = null; // if workset iteration
		
		boolean waitForSolutionSetUpdate = config.getWaitForSolutionSetUpdate();
		boolean isWorksetIteration = config.getIsWorksetIteration();

		try {
			/* used for receiving the current iteration result from iteration tail */
			BlockingBackChannel backChannel = initBackChannel();
			SolutionSetUpdateBarrier solutionSetUpdateBarrier = null;

			feedbackDataInput = config.getIterationHeadPartialSolutionOrWorksetInputIndex();
			feedbackTypeSerializer = this.<Y>getInputSerializer(feedbackDataInput);
			excludeFromReset(feedbackDataInput);

			if (isWorksetIteration) {
				initialSolutionSetInput = config.getIterationHeadSolutionSetInputIndex();
				TypeSerializerFactory<X> solutionTypeSerializerFactory = config.getSolutionSetSerializer(userCodeClassLoader);
				solutionTypeSerializer = solutionTypeSerializerFactory;

				// setup the index for the solution set
				solutionSet = initCompactingHashTable();

				// read the initial solution set
				@SuppressWarnings("unchecked")
				MutableObjectIterator<X> solutionSetInput = (MutableObjectIterator<X>) createInputIterator(inputReaders[initialSolutionSetInput], solutionTypeSerializer);
				readInitialSolutionSet(solutionSet, solutionSetInput);

				SolutionSetBroker.instance().handIn(brokerKey, solutionSet);

				if (waitForSolutionSetUpdate) {
					solutionSetUpdateBarrier = new SolutionSetUpdateBarrier();
					SolutionSetUpdateBarrierBroker.instance().handIn(brokerKey, solutionSetUpdateBarrier);
				}

			} else {
				// bulk iteration case
				initialSolutionSetInput = -1;

				@SuppressWarnings("unchecked")
				TypeSerializerFactory<X> solSer = (TypeSerializerFactory<X>) feedbackTypeSerializer;
				solutionTypeSerializer = solSer;
				
				// = termination Criterion tail
				if (waitForSolutionSetUpdate) {
					solutionSetUpdateBarrier = new SolutionSetUpdateBarrier();
					SolutionSetUpdateBarrierBroker.instance().handIn(brokerKey, solutionSetUpdateBarrier);
				}
			}

			// register the global accumulator registry
			accumulatorRegistry = new RuntimeAccumulatorRegistry();
			IterationAccumulatorBroker.instance().handIn(brokerKey, accumulatorRegistry);
			
			// setup workset accumulator
			if (isWorksetIteration) {
				worksetAccumulator = new LongCounter();
				getIterationAccumulators().addAccumulator(WorksetEmptyConvergenceCriterion.ACCUMULATOR_NAME, worksetAccumulator);
			}

			DataInputView superstepResult = null;

			while (this.running && !terminationRequested()) {

				if (log.isInfoEnabled()) {
					log.info(formatLogString("starting iteration [" + currentIteration() + "]"));
				}

				if (waitForSolutionSetUpdate) {
					solutionSetUpdateBarrier.setup();
				}

				if (!inFirstIteration()) {
					feedBackSuperstepResult(superstepResult);
				}

				super.run();

				// signal to connected tasks that we are done with the superstep
				sendEndOfSuperstepToAllIterationOutputs();

				if (waitForSolutionSetUpdate) {
					solutionSetUpdateBarrier.waitForSolutionSetUpdate();
				}

				// blocking call to wait for the result
				superstepResult = backChannel.getReadEndAfterSuperstepEnded();
				if (log.isInfoEnabled()) {
					log.info(formatLogString("finishing iteration [" + currentIteration() + "]"));
				}

				// Report end of superstep to JobManager
				TaskConfig taskConfig = new TaskConfig(getTaskConfiguration());
				synchronized (getEnvironment().getIterationReportProtocolProxy()) {
					try {
						getEnvironment().getIterationReportProtocolProxy().reportEndOfSuperstep(
								new WorkerDoneEvent(taskConfig.getIterationId(), workerIndex, 
										new AccumulatorEvent(getEnvironment().getJobID(), getIterationAccumulators().getAllAccumulators(), true)
								));
					} catch (IOException e) {
						throw new RuntimeException("Communication with JobManager is broken. Could not send end of superstep. Often this exception is the result of a missing default constructor for an accumulator.", e);
					}
				}
				
				
				if (log.isInfoEnabled()) {
					log.info(formatLogString("waiting for other workers in iteration [" + currentIteration() + "]"));
				}
				
				// wait for signaling of next action from JobManager
				synchronized (instructionSynchronizer) {	
					// make sure the TaskManager thread was not faster
					if(instructionSynchronizer.get() == AWAITING_REQUEST ) {
						try {
							this.instructionSynchronizer.wait();
						}
						catch(InterruptedException e) {
						}
					}
				}
				
				if(this.instructionSynchronizer.get() == TERMINATION_REQUEST) {
					if (log.isInfoEnabled()) {
						log.info(formatLogString("head received termination request in iteration ["
							+ currentIteration()
							+ "]"));
					}

					requestTermination();
					
				} else if(this.instructionSynchronizer.get() == NEXT_SUPERSTEP_REQUEST) {
					if(lastGlobalState == null) {
						throw new RuntimeException("This should not happen. AllWorkersDoneEvent must be received to continue with the next superstep");
					}
					
					incrementIterationCounter();
					accumulatorRegistry.updateGlobalAccumulatorsAndReset(lastGlobalState.getAccumulators());
					lastGlobalState = null;
				}
				
				// reset instructionSynchronizer
				this.instructionSynchronizer.set(AWAITING_REQUEST);
			}

			if (log.isInfoEnabled()) {
				log.info(formatLogString("streaming out final result after [" + currentIteration() + "] iterations"));
			}

			if (isWorksetIteration) {
				streamSolutionSetToFinalOutput(solutionSet);
			} else {
				streamOutFinalOutputBulk(new InputViewIterator<X>(superstepResult, this.solutionTypeSerializer.getSerializer()));
			}

			this.finalOutputCollector.close();

		} finally {
			// make sure we unregister everything from the broker:
			// - backchannel
			// - aggregator registry
			// - solution set index
			//IterationAccumulatorBroker.instance().remove(brokerKey);
			BlockingBackChannelBroker.instance().remove(brokerKey);
			if (isWorksetIteration) {
				SolutionSetBroker.instance().remove(brokerKey);
				if (waitForSolutionSetUpdate) {
					SolutionSetUpdateBarrierBroker.instance().remove(brokerKey);
				}
			}
			if (solutionSet != null) {
				solutionSet.close();
				solutionSet = null;
			}
		}
	}

	private void streamOutFinalOutputBulk(MutableObjectIterator<X> results) throws IOException {
		final Collector<X> out = this.finalOutputCollector;
		X record = this.solutionTypeSerializer.getSerializer().createInstance();

		while ((record = results.next(record)) != null) {
			out.collect(record);
		}
	}
	
	private void streamSolutionSetToFinalOutput(CompactingHashTable<X> hashTable) throws IOException {
		final MutableObjectIterator<X> results = hashTable.getEntryIterator();
		final Collector<X> output = this.finalOutputCollector;
		X record = solutionTypeSerializer.getSerializer().createInstance();

		while ((record = results.next(record)) != null) {
			output.collect(record);
		}
	}

	private void feedBackSuperstepResult(DataInputView superstepResult) {
		this.inputs[this.feedbackDataInput] =
			new InputViewIterator<Y>(superstepResult, this.feedbackTypeSerializer.getSerializer());
	}

	private void sendEndOfSuperstepToAllIterationOutputs() throws IOException, InterruptedException {
		if (log.isDebugEnabled()) {
			log.debug(formatLogString("Sending end-of-superstep to all iteration outputs."));
		}

		for (int outputIndex = 0; outputIndex < this.eventualOutputs.size(); outputIndex++) {
			this.eventualOutputs.get(outputIndex).sendEndOfSuperstep();
		}
	}

	public AtomicInteger getInstructionSynchronizer() {
			return instructionSynchronizer;
	}

	public AllWorkersDoneEvent getLastGlobalState() {
		return lastGlobalState;
	}

	public void setLastGlobalState(AllWorkersDoneEvent lastGlobalState) {
		this.lastGlobalState = lastGlobalState;
	}

}
