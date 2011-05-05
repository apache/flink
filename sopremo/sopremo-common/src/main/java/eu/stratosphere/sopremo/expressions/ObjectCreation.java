package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationException;

public class ObjectCreation extends EvaluableExpression {
	private ValueAssignment[] assignments;

	public ObjectCreation(ValueAssignment... assignments) {
		this.assignments = assignments;
	}

	public ObjectCreation(List<ValueAssignment> assignments) {
		this.assignments = assignments.toArray(new ValueAssignment[assignments.size()]);
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(Arrays.toString(this.assignments));
	}

	@Override
	public int hashCode() {
		return 53 + Arrays.hashCode(this.assignments);
	}

	@Override
	public JsonNode evaluate(JsonNode node) {
		throw new EvaluationException();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return Arrays.equals(this.assignments, ((ObjectCreation) obj).assignments);
	}
}