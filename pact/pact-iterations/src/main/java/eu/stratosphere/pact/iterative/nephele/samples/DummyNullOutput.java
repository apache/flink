package eu.stratosphere.pact.iterative.nephele.samples;

import eu.stratosphere.nephele.template.AbstractOutputTask;

public class DummyNullOutput extends AbstractOutputTask {

    @Override
    public void registerInputOutput() {}

    @Override
    public void invoke() throws Exception {}

}
