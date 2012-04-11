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
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * @author Tommy Neubert
 */
public class GeneralSchema implements Schema {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4363364708964922955L;

	List<EvaluationExpression> mappings = new ArrayList<EvaluationExpression>();

	/**
	 * Initializes a new GeneralSchema with empty mappings.
	 */
	public GeneralSchema() {
	}

	/**
	 * Initializes a new GeneralSchema with the provided {@link EvaluationExpression}s in proper sequence.
	 * 
	 * @param mappings
	 *        {@link EvaluationExpression}s which should be set as mappings
	 */
	public GeneralSchema(EvaluationExpression... mappings) {
		this.mappings = Arrays.asList(mappings);
	}

	/**
	 * Initializes a new GeneralSchema with the provided {@link EvaluationExpression}s. The mappings will be set in the
	 * same sequence as the Iterable provides them.
	 * 
	 * @param mappings
	 *        an Iterable over all {@link EvaluationExpression}s which should be set as mappings.
	 */
	public GeneralSchema(Iterable<EvaluationExpression> mappings) {
		for (EvaluationExpression exp : mappings) {
			this.mappings.add(exp);
		}
	}

	/**
	 * Sets this schemas mappings to the provided {@link EvaluationExpression}s.
	 * 
	 * @param mappings
	 *        an Iterable over all {@link EvaluationExpression}s which should be set as mappings.
	 */
	public void setMappings(Iterable<EvaluationExpression> mappings) {
		if (mappings == null) {
			throw new NullPointerException("mapping must not be null");
		}

		this.mappings.clear();
		for (EvaluationExpression exp : mappings) {
			this.mappings.add(exp);
		}
	}

	/**
	 * Returns a {@link List} of all mappings in this schema
	 * 
	 * @return a List of all mappings
	 */
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
			target.setField(i, this.mappings.get(i).evaluate(value, null, context));
		}

		return target;
	}

	@Override
	public IJsonNode recordToJson(PactRecord record, IJsonNode target) {
		IJsonNode source = (IJsonNode) SopremoUtil.unwrap(record.getField(record.getNumFields() - 1,
			JsonNodeWrapper.class));

		if (target == null) {
			return source;
		}
		return this.reuseTargetNode(target, source);
	}

	private IJsonNode reuseTargetNode(IJsonNode target, IJsonNode source) {
		target.clear();
		if (target.isObject()) {
			((IObjectNode) target).putAll((IObjectNode) source);
		} else if (target.isArray()) {
			((IArrayNode) target).addAll((IArrayNode) source);
		} else {
			// target must be a PrimitiveNode
			if (source.getClass() != target.getClass()) {
				target = source;
			} else {
				target = this.reusePrimitive(source, target, source.getClass());
			}
		}

		return target;
	}

	private IJsonNode reusePrimitive(IJsonNode source, IJsonNode target, Class<? extends IJsonNode> clazz) {
		if (clazz.equals(BooleanNode.class) || clazz.equals(NullNode.class)) {
			return source;
		}
		if (clazz.equals(IntNode.class)) {
			((IntNode) target).setValue(((IntNode) source).getIntValue());
		} else if (clazz.equals(DoubleNode.class)) {
			((DoubleNode) target).setValue(((DoubleNode) source).getDoubleValue());
		} else if (clazz.equals(LongNode.class)) {
			((LongNode) target).setValue(((LongNode) source).getLongValue());
		} else if (clazz.equals(DecimalNode.class)) {
			((DecimalNode) target).setValue(((DecimalNode) source).getDecimalValue());
		} else if (clazz.equals(BigIntegerNode.class)) {
			((BigIntegerNode) target).setValue(((BigIntegerNode) source).getBigIntegerValue());
		}
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
