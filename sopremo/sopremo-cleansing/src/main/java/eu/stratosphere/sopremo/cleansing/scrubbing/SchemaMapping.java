package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class SchemaMapping extends CompositeOperator {
	private EvaluationExpression projection;

	private List<ValidationRule> rules = new ArrayList<ValidationRule>();

	public SchemaMapping(EvaluationExpression projection, JsonStream input) {
		super(input);
		this.projection = projection;
	}

	public boolean addRule(ValidationRule e) {
		return this.rules.add(e);
	}

	public boolean removeRule(Object o) {
		return this.rules.remove(o);
	}

	public List<ValidationRule> getRules() {
		return this.rules;
	}

	@Override
	public SopremoModule asElementaryOperators() {
		Validation validation = new Validation(new Projection(projection, getInput(0)));
		validation.setRules(rules);
		return SopremoModule.valueOf(getName(), validation);
	}

}
