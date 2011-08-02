package eu.stratosphere.sopremo.cleansing;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class Fusion extends CompositeOperator {
	public EvaluationExpression getWeightExpression() {
		return weightExpression;
	}

	public void setWeightExpression(EvaluationExpression weightExpression) {
		if (weightExpression == null)
			throw new NullPointerException("weightExpression must not be null");

		this.weightExpression = weightExpression;
	}

	private EvaluationExpression clusterMerger;

	private EvaluationExpression weightExpression;

	public Fusion(EvaluationExpression clusterMerger, JsonStream input) {
		super(input);
		this.clusterMerger = clusterMerger;
	}

	@Override
	public SopremoModule asElementaryOperators() {
		return SopremoModule.valueOf(getName(), new Projection(clusterMerger, getInput(0)));
	}
}
