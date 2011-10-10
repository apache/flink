package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class RangeRule extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2095485480183527636L;

	private static final ValueCorrection CHOOSE_NEAREST_BOUND = new ValueCorrection() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6017027549741008580L;

		private Object readResolve() {
			return CHOOSE_NEAREST_BOUND;
		}

		@Override
		public JsonNode fix(final JsonNode value, final ValidationContext context) {
			final RangeRule that = (RangeRule) context.getViolatedRule();
			if (that.min.compareTo(value) > 0)
				return that.min;
			return that.max;
		}
	};

	private JsonNode min, max;

	public RangeRule(final JsonNode min, final JsonNode max, final EvaluationExpression... targetPath) {
		super(targetPath);
		this.min = min;
		this.max = max;
		this.setValueCorrection(CHOOSE_NEAREST_BOUND);
	}

	@Override
	protected boolean validate(final JsonNode value, final ValidationContext context) {
		return this.min.compareTo(value) <= 0 && value.compareTo(this.max) <= 0;
	}
}
