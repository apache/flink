package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.pact.JsonNodeComparator;

public class RangeValidationExpression extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2095485480183527636L;

	private static final ValueCorrection CHOOSE_NEAREST_BOUND = new ValueCorrection() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6017027549741008580L;

		@Override
		public JsonNode fix(final JsonNode value, final ValidationContext context) {
			// TODO Auto-generated method stub
			return null;
		}
	};

	private final JsonNode min, max;

	public RangeValidationExpression(final JsonNode min, final JsonNode max, final String... targetPath) {
		super(targetPath);
		this.min = min;
		this.max = max;
		this.setValueCorrection(CHOOSE_NEAREST_BOUND);
	}

	@Override
	protected JsonNode fix(final JsonNode value, final ValidationContext context) {
		if (JsonNodeComparator.INSTANCE.compare(this.min, value) > 0) {
			if (this.getValueCorrection() != DEFAULT_CORRECTION)
				return super.fix(value, context);
			return this.min;
		}
		if (JsonNodeComparator.INSTANCE.compare(value, this.max) > 0) {
			if (this.getValueCorrection() != DEFAULT_CORRECTION)
				return super.fix(value, context);
			return this.max;
		}
		return value;
	}

	@Override
	protected boolean validate(final JsonNode value, final ValidationContext context) {
		return JsonNodeComparator.INSTANCE.compare(this.min, value) <= 0
			&& JsonNodeComparator.INSTANCE.compare(value, this.max) <= 0;
	}
}
