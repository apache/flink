package eu.stratosphere.sopremo.serialization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;

public class GeneralSchema implements Schema {

	List<EvaluationExpression> mappings = new ArrayList<EvaluationExpression>();

	public GeneralSchema() {
	}

	public GeneralSchema(EvaluationExpression... mappings) {
		this.mappings = Arrays.asList(mappings);
	}

	public GeneralSchema(Iterable<EvaluationExpression> mappings) {
		for (EvaluationExpression exp : mappings) {
			this.mappings.add(exp);
		}
	}

	public void setMappings(Iterable<EvaluationExpression> mappings) {
		if (mappings == null) {
			throw new NullPointerException("mapping must not be null");
		}

		this.mappings.clear();
		for (EvaluationExpression exp : mappings) {
			this.mappings.add(exp);
		}
	}

	public List<EvaluationExpression> getMappings() {
		return this.mappings;
	}

	@Override
	public Class<? extends Value>[] getPactSchema() {
		Class<? extends Value>[] schema = new Class[this.mappings.size() + 1];

		for (int i = 0; i <= this.mappings.size(); i++) {
			schema[i] = JsonNodeWrapper.class;
		}

		return schema;
	}

	@Override
	public PactRecord jsonToRecord(IJsonNode value, PactRecord target, EvaluationContext context) {

		if (target == null) {
			target = new PactRecord(this.mappings.size() + 1);
		}
		target.setField(this.mappings.size(), value.clone());

		for (int i = 0; i < this.mappings.size(); i++) {
			target.setField(i, this.mappings.get(i).evaluate(value, context));
		}

		return target;
	}

	@Override
	public IJsonNode recordToJson(PactRecord record, IJsonNode target) {
		// TODO [BUG] target node is not used correctly
		target = (IJsonNode) SopremoUtil.unwrap(record.getField(record.getNumFields() - 1, JsonNodeWrapper.class));
		return target;
	}

	@Override
	public int[] indicesOf(EvaluationExpression expression) {
		int index = mappings.indexOf(expression);
		if (index == -1) {
			throw new IllegalArgumentException("Field not found.");
		}
		return new int[] { index };
	}

}
