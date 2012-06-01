package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.sdaa11.Annotator;

/**
 * Inputs:<br>
 * <ol>
 * <li>Sample points</li>
 * </ol>
 * Outputs:<br>
 * <ol>
 * <li>Clusters</li>
 * </ol>
 * 
 * @author skruse
 * 
 */
public class InitialClustering extends CompositeOperator<InitialClustering> {

	private static final long serialVersionUID = 9084919057903474256L;

	/** The maximum radius of a cluster. */
	private int maxRadius = SequentialClustering.DEFAULT_MAX_RADIUS;

	/** The maximum number of points of a cluster. */
	private int maxSize = SequentialClustering.DEFAULT_MAX_SIZE;

	public int getMaxRadius() {
		return this.maxRadius;
	}

	public void setMaxRadius(final int maxRadius) {
		this.maxRadius = maxRadius;
	}

	public int getMaxSize() {
		return this.maxSize;
	}

	public void setMaxSize(final int maxSize) {
		this.maxSize = maxSize;
	}

	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final ElementarySopremoModule module = new ElementarySopremoModule(
				this.getName(), 1, 1);

		final Operator<?> input = module.getInput(0);
		final Annotator annotator = new Annotator().withInputs(input);
		final SequentialClustering sequentialClustering = new SequentialClustering()
				.withInputs(annotator);
		sequentialClustering.setMaxRadius(this.maxRadius);
		sequentialClustering.setMaxSize(this.maxSize);

		module.getOutput(0).setInput(0, sequentialClustering);

		return module;
	}

}
