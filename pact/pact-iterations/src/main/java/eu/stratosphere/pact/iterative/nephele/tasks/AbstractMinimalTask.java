package eu.stratosphere.pact.iterative.nephele.tasks;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.BipartiteDistributionPattern;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.MutableReader;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.util.OutputCollectorV2;
import eu.stratosphere.pact.iterative.nephele.util.OutputEmitterV2;
import eu.stratosphere.pact.runtime.plugable.PactRecordAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public abstract class AbstractMinimalTask extends AbstractTask {
	
	public static final String TYPE = "minimal.task.type.accessor";
	protected static final Log LOG = LogFactory.getLog(AbstractMinimalTask.class);

	protected MutableObjectIterator<Value>[] inputs;
	protected TypeAccessorsV2<? extends Value>[] accessors;
	protected TypeAccessorsV2<? extends Value>[] outputAccessors;
	protected TaskConfig config;
	protected OutputCollectorV2 output;
	protected ClassLoader classLoader;
	protected MemoryManager memoryManager;
	protected IOManager ioManager;
	
	protected long memorySize;
	
	
	@Override
	public final void registerInputOutput() {
		this.config = new TaskConfig(getRuntimeConfiguration());
		try {
			this.classLoader = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
		} catch (IOException ioe) {
			throw new RuntimeException("The ClassLoader for the user code could not be instantiated from the library cache.", ioe);
		}
		
		memorySize = config.getMemorySize();
		System.out.println("Assigned Memory: " + memorySize / 1024 / 1024);
		
		initInputs();
		initOutputs();
		initInternal();
		initTask();
	}
	
	protected void initInternal() {
	}
	
	protected abstract void initTask();
	
	/**
	 * Creates the record readers for the number of inputs as defined by {@link #getNumberOfInputs()}.
	 */
	protected void initInputs()
	{
		int numInputs = getNumberOfInputs();
		
		@SuppressWarnings("unchecked")
		final MutableObjectIterator<Value>[] inputs = new MutableObjectIterator[numInputs];
		@SuppressWarnings("unchecked")
		final TypeAccessorsV2<? extends Value>[] accessors = new TypeAccessorsV2[numInputs];
		
		for (int i = 0; i < numInputs; i++)
		{
			final ShipStrategy shipStrategy = config.getInputShipStrategy(i);
			DistributionPattern dp = null;
			
			switch (shipStrategy)
			{
			case FORWARD:
			case PARTITION_LOCAL_HASH:
			case PARTITION_LOCAL_RANGE:
				dp = new PointwiseDistributionPattern();
				break;
			case PARTITION_HASH:
			case PARTITION_RANGE:
			case BROADCAST:
			case SFR:
				dp = new BipartiteDistributionPattern();
				break;
			default:
				throw new RuntimeException("Invalid input ship strategy provided for input " + 
					i + ": " + shipStrategy.name());
			}
			
			inputs[i] = new RecordReaderIterator(new MutableRecordReader<Value>(this, dp));
			
			//Get type accessor for that input
			@SuppressWarnings("unchecked")
			Class<? extends TypeAccessorsV2<? extends Value>> clsAccessor = 
				(Class<? extends TypeAccessorsV2<? extends Value>>)
				getRuntimeConfiguration().getClass(TYPE+i, PactRecordAccessorsV2.class);
			
			try {
				if(clsAccessor == PactRecordAccessorsV2.class) {
					int[] pos = config.getLocalStrategyKeyPositions(i);
					Class<? extends Key>[] clss = 
							config.getLocalStrategyKeyClasses(classLoader);
					
					pos = pos!=null?pos:new int[] {};
					clss = clss!=null?clss:new Class[] {};
					
					accessors[i] = new PactRecordAccessorsV2(
							pos,
							clss);
				} else {
					accessors[i] = clsAccessor.newInstance();
				}
			} catch (Exception e) {
				throw new RuntimeException("Error instantiating accessor", e);
			}
		}
		
		this.accessors = accessors;
		this.inputs = inputs;
	}
	
	@Override
	public final void invoke() throws Exception {		
		try {
			initEnvironmentManagers();
			setOutputAccessors();
			
			prepareInternal();
			
			prepare();
			
			run();
		} catch (Throwable ex) {
			LOG.error(ex, ex);
			throw new Exception("Error during execution of task ", ex);
		} finally {
			//cleanup();
			output.close();
		}
	}
	
	protected void prepareInternal() throws Exception {
		
	}
	
	public void prepare() throws Exception {
		
	}
	
	public abstract void run() throws Exception;
	
	public void cleanup() throws Exception {
		
	}
	
	@Override
	public void cancel() throws Exception {
		cleanup();
	}
	/**
	 * Creates a writer for each output. Creates an OutputCollector which forwards its input to all writers.
	 * The output collector applies the configured shipping strategies for each writer.
	 */
	@SuppressWarnings("unchecked")
	protected void initOutputs()
	{
		int numOutputs = this.config.getNumOutputs();
		
		OutputCollectorV2 output = new OutputCollectorV2();
		outputAccessors = new TypeAccessorsV2[numOutputs];
		
		// create a writer for each output
		for (int i = 0; i < numOutputs; i++)
		{
			initOutputAccessor(i);
			
			// create the OutputEmitter from output ship strategy			
			final OutputEmitterV2 oe = new OutputEmitterV2(
					config.getOutputShipStrategy(i), 
					getEnvironment().getJobID(), 
					(TypeAccessorsV2<Value>) outputAccessors[i]);
			
			output.addWriter(new RecordWriter<Value>((AbstractTask) this, Value.class, oe));
		}
		
		this.output = output;
	}
	
	protected void initOutputAccessor(int outputIndex) {
		ClassLoader cl = getClass().getClassLoader();
		
		// create the OutputEmitter from output ship strategy
		int[] keyPositions = config.getOutputShipKeyPositions(outputIndex);
		Class<? extends Key>[] keyClasses;
		try {
			keyClasses = config.getOutputShipKeyTypes(outputIndex, cl);
		}
		catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The classes for the keys determining the partitioning for output " + 
				outputIndex + "  could not be loaded.");
		}
		
		keyPositions = keyPositions!=null?keyPositions:new int[] {};
		keyClasses = keyClasses!=null?keyClasses:new Class[] {};
		outputAccessors[outputIndex] = new PactRecordAccessorsV2(keyPositions, keyClasses);
		
	}
	
	protected void setOutputAccessors() {
		int numOutputs = this.config.getNumOutputs();
		
		// create a writer for each output
		for (int i = 0; i < numOutputs; i++)
		{
			ChannelSelector selector =
					output.getWriters().get(i).getOutputGate().getChannelSelector();
			((OutputEmitterV2) selector).setTypeAccessor(
					(TypeAccessorsV2<Value>) outputAccessors[i]);
		}
	}
	
	protected void waitForPreviousTask(MutableObjectIterator<PactRecord> input) throws IOException {
		PactRecord tmp = new PactRecord();
		while(input.next(tmp)) {
			
		}
	}
	
	protected Class<? extends Key>[] loadKeyClasses() {
		try {
			return config.getLocalStrategyKeyClasses(classLoader);
		} catch (Exception ex) {
			throw new RuntimeException("Error loading key classes", ex);
		}
	}
	
	protected <T> T initStub(Class<T> stubSuperClass) {
		try {
			@SuppressWarnings("unchecked")
			Class<T> stubClass = (Class<T>) this.config.getStubClass(stubSuperClass, classLoader);
			
			return InstantiationUtil.instantiate(stubClass, stubSuperClass);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("The stub implementation class was not found.", cnfe);
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("The stub class is not a proper subclass of " + stubSuperClass.getName(), ccex); 
		}
	}
	
	protected void initEnvironmentManagers() {
		Environment env = getEnvironment();
		memoryManager = env.getMemoryManager();
		ioManager = env.getIOManager();
	}
	
	/**
	 * Gets the number of inputs (= Nephele Gates and Readers) that the task has.
	 * 
	 * @return The number of inputs.
	 */
	public abstract int getNumberOfInputs();

	private static final class RecordReaderIterator implements MutableObjectIterator<Value>
	{
		private final MutableReader<Value> reader;		// the source

		/**
		 * Creates a new iterator, wrapping the given reader.
		 * 
		 * @param reader The reader to wrap.
		 */
		public RecordReaderIterator(MutableReader<Value> reader)
		{
			this.reader = reader;
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.util.MutableObjectIterator#next(java.lang.Object)
		 */
		@Override
		public boolean next(Value target) throws IOException
		{
			try {
				return this.reader.next(target);
			}
			catch (InterruptedException iex) {
				throw new IOException("Reader was interrupted.");
			}
		}
	}
}
