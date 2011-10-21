package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.JsonNode;

public class FusionContext extends EvaluationContext {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3830001019910981066L;

	private JsonNode[] contextNodes;

	private int[] sourceIndexes;

	private double[] weights;

	public FusionContext(final EvaluationContext context) {
		super(context);
	}

	public JsonNode[] getContextNodes() {
		return this.contextNodes;
	}

	public int[] getSourceIndexes() {
		return this.sourceIndexes;
	}

	public double[] getWeights() {
		return this.weights;
	}

	public void setContextNodes(final JsonNode[] contextNode) {
		if (contextNode == null)
			throw new NullPointerException("contextNode must not be null");

		this.contextNodes = contextNode;
	}

	public void setSourceIndexes(final int[] sourceIndexes) {
		if (sourceIndexes == null)
			throw new NullPointerException("sourceIndexes must not be null");

		this.sourceIndexes = sourceIndexes;
	}

	public void setWeights(final double[] weights) {
		if (weights == null)
			throw new NullPointerException("weights must not be null");

		this.weights = weights;
	}

}
