package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class SchemaMapping extends CompositeOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5817110603520085487L;

	private EvaluationExpression projection = EvaluationExpression.VALUE;

	private List<ValidationRule> rules = new ArrayList<ValidationRule>();

	public EvaluationExpression getProjection() {
		return this.projection;
	}

	public void setProjection(EvaluationExpression projection) {
		if (projection == null)
			throw new NullPointerException("projection must not be null");

		this.projection = projection;
	}
	
	public SchemaMapping withProjection(EvaluationExpression projection) {
		setProjection(projection);
		return this;
	}

	public void addRule(final ValidationRule e) {
		this.rules.add(e);
	}

	@Override
	public SopremoModule asElementaryOperators() {
		final Validation validation = new Validation(new Projection(this.projection, null));
		validation.setRules(this.rules);
		return SopremoModule.valueOf(this.getName(), validation);
	}

	public List<ValidationRule> getRules() {
		return this.rules;
	}

	public boolean removeRule(final Object o) {
		return this.rules.remove(o);
	}

}
