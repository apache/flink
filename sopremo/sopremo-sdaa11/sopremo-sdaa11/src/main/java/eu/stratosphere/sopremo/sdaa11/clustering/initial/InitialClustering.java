package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.sdaa11.Annotator;

public class InitialClustering extends CompositeOperator<InitialClustering> {

	@Override
	public SopremoModule asElementaryOperators() {
		SopremoModule module = new SopremoModule(getName(), 1, 1);
		
		Operator<?> input = module.getInput(0);
		Annotator annotator = new Annotator().withInputs(input);
		SequentialClustering sequentialClustering = new SequentialClustering().withInputs(annotator);
		
		module.getOutput(0).setInput(0, sequentialClustering);
		
		return module;
	}

}
