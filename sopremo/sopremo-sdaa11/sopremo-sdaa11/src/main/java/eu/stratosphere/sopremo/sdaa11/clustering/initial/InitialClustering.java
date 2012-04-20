package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.sdaa11.Annotator;

public class InitialClustering extends CompositeOperator<InitialClustering> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9084919057903474256L;

	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.getName(), 1, 1);

		final Operator<?> input = module.getInput(0);
		final Annotator annotator = new Annotator().withInputs(input);
		final SequentialClustering sequentialClustering = new SequentialClustering()
				.withInputs(annotator);

		module.getOutput(0).setInput(0, sequentialClustering);

		return module;
	}

}
