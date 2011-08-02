package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.cleansing.conflict_resolution.UnresolvableEvalatuationException;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.pact.SopremoMap;

public class Validation extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3979039050900230817L;

	private List<ValidationRule> rules = new ArrayList<ValidationRule>();

	public Validation(JsonStream input) {
		super(input);
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

	public void setRules(List<ValidationRule> rules) {
		if (rules == null)
			throw new NullPointerException("rules must not be null");

		this.rules = rules;
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		if (rules.isEmpty())
			return new PactModule(getName(), 1, 1);
		return super.asPactModule(context);
	}

	public static class Implementation extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
		private List<ValidationRule> rules = new ArrayList<ValidationRule>();

		@Override
		protected void map(JsonNode key, JsonNode contextNode, JsonCollector out) {
			try {
				JsonNode resultingNode = contextNode;
				EvaluationContext context = this.getContext();
				for (ValidationRule rule : this.rules) {
					ObjectAccess[] targetPath = rule.getTargetPath();

					if (targetPath.length == 0) {
						if (!rule.validate(resultingNode, contextNode, context))
							resultingNode = rule.fix(resultingNode, contextNode, context);
					} else {
						JsonNode parent = resultingNode;
						for (int index = 0; index < targetPath.length - 1; index++)
							parent = targetPath[index].evaluate(parent, context);

						ObjectAccess lastSegment = targetPath[targetPath.length - 1];
						JsonNode validationValue = lastSegment.evaluate(parent, context);
						if (!rule.validate(key, validationValue, context))
							lastSegment.set(parent, rule.fix(key, validationValue, context), context);
					}
				}
				out.collect(key, resultingNode);
			} catch (UnresolvableEvalatuationException e) {
				// do not emit invalid record
			}
		}
	}
}
