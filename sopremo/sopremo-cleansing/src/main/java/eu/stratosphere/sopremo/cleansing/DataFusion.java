package eu.stratosphere.sopremo.cleansing;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class DataFusion extends CompositeOperator {
	private EvaluationExpression clusterMerger;

	public DataFusion(EvaluationExpression clusterMerger, JsonStream input) {
		super(input);
		this.clusterMerger = clusterMerger;
	}

	@Override
	public SopremoModule asElementaryOperators() {
		return SopremoModule.valueOf(getName(), new Projection(clusterMerger, getInput(0)));
	}
}
