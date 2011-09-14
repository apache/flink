package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.pact.SopremoUtil;

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
			if (JsonNodeComparator.INSTANCE.compare(that.min, value) > 0)
				return that.min;
			return that.max;
		}
	};

	private transient JsonNode min, max;

	public RangeRule(final JsonNode min, final JsonNode max, final EvaluationExpression... targetPath) {
		super(targetPath);
		this.min = min;
		this.max = max;
		this.setValueCorrection(CHOOSE_NEAREST_BOUND);
	}

	private void readObject(final ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();

		final JsonNode array = SopremoUtil.deserializeNode(stream);
		this.min = array.get(0);
		this.max = array.get(1);
	}

	private void writeObject(final ObjectOutputStream stream) throws IOException {
		stream.defaultWriteObject();

		SopremoUtil.serializeNode(stream, JsonUtil.asArray(this.min, this.max));
	}

	@Override
	protected boolean validate(final JsonNode value, final ValidationContext context) {
		return JsonNodeComparator.INSTANCE.compare(this.min, value) <= 0
			&& JsonNodeComparator.INSTANCE.compare(value, this.max) <= 0;
	}
}
