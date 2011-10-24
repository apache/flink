package eu.stratosphere.nephele.io;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.template.AbstractTask;

/**
 * Dummy Task for RecordReader and UnionRecordReader tests.
 * We just need the Environment instance.
 * 
 * @author mjsax
 */
public class DummyTask extends AbstractTask {
	/**
	 * Initializes DummyTask with an new empty Environment instance.
	 *
	 */
	public DummyTask() {
		setEnvironment(new Environment());
	}
	
	@Override
	public void registerInputOutput() { /* empty */ }
	@Override
	public void invoke() throws Exception { /* empty */ }
}
