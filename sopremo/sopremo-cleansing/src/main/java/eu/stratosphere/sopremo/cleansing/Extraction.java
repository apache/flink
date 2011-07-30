package eu.stratosphere.sopremo.cleansing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class Extraction extends CompositeOperator {
	private List<EvaluationExpression> expressions = new ArrayList<EvaluationExpression>();

	public Extraction(EvaluationExpression firstExpression, JsonStream input) {
		super(input);
		this.expressions.add(firstExpression);
	}

	public Extraction(EvaluationExpression[] expressions, JsonStream input) {
		super(input);
		this.expressions.addAll(Arrays.asList(expressions));
		setNumberOfOutputs(this.expressions.size());
	}

	public Extraction(List<? extends EvaluationExpression> expressions, JsonStream input) {
		super(input);
		this.expressions.addAll(expressions);
		setNumberOfOutputs(this.expressions.size());
	}

	public Extraction add(EvaluationExpression expression) {
		this.expressions.add(expression);
		setNumberOfOutputs(this.expressions.size());
		return this;
	}

	@Override
	public SopremoModule asElementaryOperators() {
		List<Projection> projections = new ArrayList<Projection>();
		for (EvaluationExpression expression : this.expressions)
			projections.add(new Projection(expression, getInput(0)));

		return SopremoModule.valueOf(getName(), projections);
	}
}
