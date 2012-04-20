package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.cleansing.scrubbing.RuleFactory.RuleContext;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.function.SimpleMacro;
import eu.stratosphere.util.Equals.Equaler;

@Name(verb = "extract from")
@InputCardinality(min = 1, max = 1)
@OutputCardinality(min = 0, max = Integer.MAX_VALUE)
public class EntityExtraction extends CompositeOperator<EntityExtraction> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5817110603520085487L;

	private static final DefaultRuleFactory ExtractionRuleFactory = new DefaultRuleFactory();

	{
		ExtractionRuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(ArithmeticExpression.class),
			new SimpleMacro<ArithmeticExpression>() {
				private static final long serialVersionUID = 111389216483477521L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(ArithmeticExpression arithExpr, EvaluationContext context) {
					if (arithExpr.getOperator() != ArithmeticOperator.MULTIPLICATION)
						throw new IllegalArgumentException("unsupported arithmetic expression");

					((RewriteContext) context).parse(arithExpr.getSecondOperand());
					return arithExpr.getFirstOperand();
				}
			});
	}

	private RuleManager extractionRules = new RuleManager();

	public ObjectCreation getExtractions() {
		return (ObjectCreation) this.extractionRules.getLastParsedExpression();
	}

	public void addExtraction(Operator<?> operator) {
		extractionRules.addRule(new JsonStreamExpression(operator),
			new JsonStreamExpression(getOutput(extractionRules.getRules().size())));
	}

	@Property
	@Name(preposition = "into")
	public void setExtractions(ObjectCreation extractions) {
		if (extractions == null)
			throw new NullPointerException("projection must not be null");

		this.extractionRules.parse(extractions, new RuleContext(this, new PathExpression(), extractionRules,
			ExtractionRuleFactory));
	}

	public EntityExtraction withProjections(ObjectCreation projection) {
		this.setExtractions(projection);
		return this;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((extractionRules == null) ? 0 : extractionRules.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		EntityExtraction other = (EntityExtraction) obj;

		return this.extractionRules.customEquals(other.extractionRules, null, new Equaler<JsonStreamExpression>() {
			@Override
			public boolean isEqual(JsonStreamExpression value1, JsonStreamExpression value2) {
				Operator<?>.Output thisSource = value1.getStream().getSource();
				Operator<?>.Output otherSource = value2.getStream().getSource();
				if (thisSource.getIndex() != otherSource.getIndex()
					|| !thisSource.getOperator().equals(otherSource.getOperator()))
					return false;
				return true;
			}
		});
	}

	@Override
	public void toString(StringBuilder builder) {
		super.toString(builder);
		builder.append(" ").append(this.extractionRules);
	}

	@Override
	public SopremoModule asElementaryOperators() {
		SopremoModule module = new SopremoModule(this.getName(), 1, this.extractionRules.getRules().size());
		return module;
	}

}
