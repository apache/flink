package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.List;
import java.util.Map.Entry;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.cleansing.fusion.UnresolvableEvaluationException;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.expressions.MethodPointerExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.function.SimpleMacro;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

@Name(verb = "scrub")
public class Scrubbing extends ElementaryOperator<Scrubbing> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3979039050900230817L;

	private RuleManager ruleManager = new RuleManager();

	private final static DefaultRuleFactory RuleFactory = new DefaultRuleFactory();

	{
		RuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(MethodPointerExpression.class),
			new SimpleMacro<MethodPointerExpression>() {
				private static final long serialVersionUID = -8260133422163585840L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(MethodPointerExpression inputExpr, EvaluationContext context) {
					return new MethodCall(inputExpr.getFunctionName(), ((RewriteContext) context).getRewritePath());
				}
			});
		RuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(JsonStreamExpression.class),
			new SimpleMacro<JsonStreamExpression>() {
				private static final long serialVersionUID = 111389216483477521L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(JsonStreamExpression inputExpr, EvaluationContext context) {
					if (inputExpr.getStream() == context.getCurrentOperator()) {
						PathExpression path = ((RewriteContext) context).getRewritePath();
						path.add(0, new ArrayAccess(1));
						return path;
					}
					return new ArrayAccess(1);
				}
			});
		RuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(CleansingRule.class),
			new SimpleMacro<CleansingRule<?>>() {
				private static final long serialVersionUID = 111389216483477521L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(CleansingRule<?> rule, EvaluationContext context) {
					// PathExpression path = ((RewriteContext) context).getRewritePath();
					// path.add(rule);
					// return path;
					return new ArrayAccess(0);
				}
			});
	}

	public void addRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		this.ruleManager.addRule(this.wrapRuleForDirectAccess(rule), target);
	}

	public void addRule(EvaluationExpression rule, EvaluationExpression... target) {
		this.ruleManager.addRule(this.wrapRuleForDirectAccess(rule), target);
	}

	protected PathExpression wrapRuleForDirectAccess(EvaluationExpression rule) {
		return new PathExpression(new ArrayAccess(0), rule);
	}

	public void removeRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		this.ruleManager.removeRule(this.wrapRuleForDirectAccess(rule), target);
	}

	public void removeRule(EvaluationExpression rule, EvaluationExpression... target) {
		this.ruleManager.removeRule(this.wrapRuleForDirectAccess(rule), target);
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		if (this.ruleManager.isEmpty()) {
			final PactModule pactModule = new PactModule(this.getName(), 1, 1);
			pactModule.getOutput(0).setInput(pactModule.getInput(0));
			return pactModule;
		}
		return super.asPactModule(context);
	}

	@Property
	@Name(preposition = "with")
	public void setRuleExpression(ObjectCreation ruleExpression) {
		this.ruleManager.parse(ruleExpression, this, RuleFactory);
		System.out.println(this.ruleManager);
	}

	public ObjectCreation getRuleExpression() {
		return (ObjectCreation) this.ruleManager.getLastParsedExpression();
	}

	public static class Implementation extends
			SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {
		private RuleManager ruleManager = new RuleManager();

		private transient ValidationContext context;

		@Override
		public void configure(final Configuration parameters) {
			super.configure(parameters);

			this.context = new ValidationContext(this.getContext());
		}

		@Override
		protected void map(final JsonNode key, JsonNode value, final JsonCollector out) {
			try {
				this.context.setContextNode(value);

				for (final Entry<PathExpression, EvaluationExpression> rulePath : this.ruleManager.getRules()) {
					final List<EvaluationExpression> targetPath = rulePath.getKey().getFragments();
					final EvaluationExpression rule = rulePath.getValue();

					if (targetPath.isEmpty())
						value = rule.evaluate(value, this.context);
					else {
						JsonNode parent = value;
						final int lastIndex = targetPath.size() - 1;
						for (int index = 0; index < lastIndex; index++)
							parent = targetPath.get(index).evaluate(parent, this.context);

						final EvaluationExpression lastSegment = targetPath.get(lastIndex);

						final JsonNode validationValue = lastSegment.evaluate(value, this.context);
						JsonNode newValue = rule.evaluate(new ArrayNode(validationValue, value), this.context);
						if (validationValue != newValue)
							lastSegment.set(parent, newValue, this.context);
					}
				}
				out.collect(key, value);
			} catch (final UnresolvableEvaluationException e) {
				// do not emit invalid record
				if (SopremoUtil.LOG.isDebugEnabled())
					SopremoUtil.LOG.debug(String.format("Cannot fix validation rule for tuple %s: %s", value,
						e.getMessage()));
			}
		}
	}
}
