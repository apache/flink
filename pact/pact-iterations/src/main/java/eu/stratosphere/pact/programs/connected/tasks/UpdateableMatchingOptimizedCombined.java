package eu.stratosphere.pact.programs.connected.tasks;

import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.iterative.nephele.util.OutputCollectorV2;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdate;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdateAccessor;
import eu.stratosphere.pact.programs.connected.types.LazyTransitiveClosureEntry;
import eu.stratosphere.pact.programs.connected.types.TransitiveClosureEntry;
import eu.stratosphere.pact.programs.connected.types.TransitiveClosureEntryAccessors;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable.LazyHashBucketIterator;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.sort.AsynchronousPartialSorterCollector;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger.InputDataCollector;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import eu.stratosphere.pact.runtime.util.KeyComparator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;

public class UpdateableMatchingOptimizedCombined extends IterationHead {
	
	protected static final Log LOG = LogFactory.getLog(UpdateableMatchingOptimizedCombined.class);
	
	private MutableHashTable<Value, ComponentUpdate> table;
	
	private int[] keyPos;
	private Class<? extends Key>[] keyClasses;
	private Comparator<Key>[] comparators;
	
	private ReduceStub stub = new UpdateReduceStub();
	
	private long sortMem  = 0;
	private long matchMem  = 0;

	private InputDataCollector inputCollector;

	private eu.stratosphere.pact.programs.connected.tasks.UpdateableMatchingOptimizedCombined.CombinerThread combinerThread;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void initTask() {
		super.initTask();
		outputAccessors[numInternalOutputs] = new ComponentUpdateAccessor();
		
		keyPos = new int[] {0};
		keyClasses =  new Class[] {PactLong.class};
		
		// create the comparators
		comparators = new Comparator[keyPos.length];
		final KeyComparator kk = new KeyComparator();
		for (int i = 0; i < comparators.length; i++) {
			comparators[i] = kk;
		}
	}
	@Override
	public void finish(MutableObjectIterator<Value> iter,
			OutputCollectorV2 output) throws Exception {
		//TODO: Properly output state
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void processInput(MutableObjectIterator<Value> iter,
			OutputCollectorV2 output) throws Exception {
		sortMem = memorySize*2 / 6;
		matchMem = memorySize*4 / 6;
		// Load build side into table		
		int chunckSize = UpdateableMatching.MATCH_CHUNCK_SIZE;
		List<MemorySegment> joinMem = memoryManager.allocateStrict(this, (int) (matchMem/chunckSize), chunckSize);
		
		TypeAccessorsV2 buildAccess = new TransitiveClosureEntryAccessors();
		TypeAccessorsV2 probeAccess = new ComponentUpdateAccessor();
		TypeComparator comp = new MatchComparator();
		
		table = new MutableHashTable<Value, ComponentUpdate>(buildAccess, probeAccess, comp, 
				joinMem, ioManager, 128);
		table.open(inputs[1], EmptyMutableObjectIterator.<ComponentUpdate>get());
		
		// Process input as normally
		processUpdates(iter, output);
	}

	@Override
	public void processUpdates(MutableObjectIterator<Value> iter,
			OutputCollectorV2 output) throws Exception {
		LazyTransitiveClosureEntry state = new LazyTransitiveClosureEntry();
		
		ComponentUpdate probe = new ComponentUpdate();
		PactRecord update = new PactRecord();
		PactLong vid = new PactLong();
		PactLong cid = new PactLong();
		
		AsynchronousPartialSorterCollector sorter = 
				new AsynchronousPartialSorterCollector(memoryManager,
				ioManager, sortMem, comparators, keyPos, keyClasses, this);
		this.inputCollector = sorter.getInputCollector();
		
		this.combinerThread = new CombinerThread(sorter, keyPos, keyClasses, this.stub, new PactRecordToUpdateCollector(output));
		this.combinerThread.start();
		
		
		int countUpdated = 0;
		int countUnchanged = 0;
		int countPreCombine = 0;
		
		while(iter.next(probe)) {
			LazyHashBucketIterator<Value, ComponentUpdate> tableIter = table.getLazyMatchesFor(probe);
			if(tableIter.next(state)) {
				long oldCid = state.getCid();
				long updateCid = probe.getCid();
				
				if(updateCid < oldCid) {
					state.setCid(updateCid);
					
					int numNeighbours = state.getNumNeighbors();
					long[] neighbourIds = state.getNeighbors();
					
					cid.setValue(updateCid);
					update.setField(1, cid);
					for (int i = 0; i < numNeighbours; i++) {
						vid.setValue(neighbourIds[i]);
						update.setField(0, vid);
						countPreCombine++;
						inputCollector.collect(update);
					}
					countUpdated++;
				} else {
					countUnchanged++;
				}
			}
			if(tableIter.next(state)) {
				throw new RuntimeException("there should only be one");
			}
		}
		
		inputCollector.close();
		while (this.combinerThread.isAlive()) {
			try {
				this.combinerThread.join();
			}
			catch (InterruptedException iex) {}
		}
		
		sorter.close();
		
		sendCounter("iteration.match.updated", countUpdated);
		sendCounter("iteration.match.unchanged", countUnchanged);
		sendCounter("iteration.combine.inputCount", countPreCombine);
	}
	
	@Override
	public int getNumberOfInputs() {
		return 2;
	}

	private static final class MatchComparator implements TypeComparator<ComponentUpdate, TransitiveClosureEntry>
	{
		private long key;

		@Override
		public void setReference(ComponentUpdate reference, 
				TypeAccessorsV2<ComponentUpdate> accessor) {
			this.key = reference.getVid();
		}

		@Override
		public boolean equalToReference(TransitiveClosureEntry candidate, TypeAccessorsV2<TransitiveClosureEntry> accessor) {
			return this.key == candidate.getVid();
		}
	}
	
	private final class CombinerThread extends Thread
	{
		private final AsynchronousPartialSorterCollector sorter;
		
		private final int[] keyPositions;
		
		private final Class<? extends Key>[] keyClasses;
		
		private final ReduceStub stub;
		
		private final Collector output;
		
		private volatile boolean running;
		
		
		private CombinerThread(AsynchronousPartialSorterCollector sorter,
				int[] keyPositions, Class<? extends Key>[] keyClasses, 
				ReduceStub stub, Collector output)
		{
			super("Combiner Thread");
			setDaemon(true);
			
			this.sorter = sorter;
			this.keyPositions = keyPositions;
			this.keyClasses = keyClasses;
			this.stub = stub;
			this.output = output;
			this.running = true;
		}

		public void run()
		{
			try {
				MutableObjectIterator<PactRecord> iterator = null;
				while (iterator == null) {
					try {
						iterator = this.sorter.getIterator();
					}
					catch (InterruptedException iex) {
						if (!this.running)
							return;
					}
				}
				
				final KeyGroupedIterator keyIter = new KeyGroupedIterator(iterator, this.keyPositions, this.keyClasses);
				
				// cache references on the stack
				final ReduceStub stub = this.stub;
				final Collector output = this.output;

				// run stub implementation
				while (this.running && keyIter.nextKey()) {
					stub.combine(keyIter.getValues(), output);
				}
			}
			catch (Throwable t) {
				throw new RuntimeException("Errör");
			}
		}
	}
	
	public class PactRecordToUpdateCollector implements Collector {

		private OutputCollectorV2 collector;
		private ComponentUpdate update = new ComponentUpdate();
		private PactLong number = new PactLong();

		public PactRecordToUpdateCollector(OutputCollectorV2 collector) {
			this.collector = collector;
		}
		
		@Override
		public void collect(PactRecord record) {
			update.setVid(record.getField(0, number).getValue());
			update.setCid(record.getField(1, number).getValue());
			collector.collect(update);
		}

		@Override
		public void close() {
			collector.close();
		}

	}
}
