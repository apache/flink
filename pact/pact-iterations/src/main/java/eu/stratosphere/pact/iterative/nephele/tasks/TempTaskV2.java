package eu.stratosphere.pact.iterative.nephele.tasks;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.iterative.nephele.util.OutputCollectorV2;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIteratorV2;

/**
 */
public class TempTaskV2 extends AbstractMinimalTask
{
	// the minimal amount of memory required for the temp to work
	private static final long MIN_REQUIRED_MEMORY = 512 * 1024;

	// spilling thread
	private SpillingResettableMutableObjectIteratorV2<Value> tempIterator;
	
	

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	public void prepare() throws Exception
	{
		// set up memory and I/O parameters
		final long availableMemory = this.config.getMemorySize();
		
		if (availableMemory < MIN_REQUIRED_MEMORY) {
			throw new RuntimeException("The temp task was initialized with too little memory: " + availableMemory +
				". Required is at least " + MIN_REQUIRED_MEMORY + " bytes.");
		}

		// obtain the TaskManager's MemoryManager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain the TaskManager's IOManager
		final IOManager ioManager = getEnvironment().getIOManager();
		
		tempIterator = new SpillingResettableMutableObjectIteratorV2<Value>(memoryManager, ioManager, 
				inputs[0], (TypeAccessorsV2<Value>) accessors[0], availableMemory, this);
		
		tempIterator.open();
		
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception {		
		// cache references on the stack
		final SpillingResettableMutableObjectIteratorV2<Value> iter = this.tempIterator;
		final OutputCollectorV2 output = this.output;
		
		Value record = accessors[0].createInstance();
		// run stub implementation
		while (iter.next(record))
		{
			// forward pair to output writer
			output.collect(record);
		}
			
		if (this.tempIterator != null) {
			this.tempIterator.close();
			this.tempIterator = null;
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cancel()
	 */
	@Override
	public void cancel() throws Exception
	{
		super.cancel();
		if (this.tempIterator != null) {
			tempIterator.abort();
		}
	}


	@Override
	protected void initTask() {
	}


	@Override
	public int getNumberOfInputs() {
		return 1;
	}
}
