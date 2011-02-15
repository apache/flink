package eu.stratosphere.pact.runtime.test.util;


import eu.stratosphere.nephele.template.AbstractInvokable;


/**
 * An invokable that does nothing.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class DummyInvokable extends AbstractInvokable {

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#registerInputOutput()
	 */
	@Override
	public void registerInputOutput() {}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#invoke()
	 */
	@Override
	public void invoke() throws Exception {}

}
