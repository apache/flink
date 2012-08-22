package eu.stratosphere.sopremo.serialization;

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.util.CollectionUtil;

/**
 * This {@link Schema} handles any kind of JsonNode and provides the functionality to save the result of the evaluation
 * of given {@link EvaluationExpression}s explicitly in the resulting PactRecord. The structure of the records is as
 * follows:<br>
 * { &#60result of first expression&#62, &#60result of second expression&#62, ..., &#60source node&#62 }
 * 
 * @author Tommy Neubert
 */
public class GeneralSchema extends AbstractSchema {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4363364708964922955L;

	List<EvaluationExpression> mappings = new ArrayList<EvaluationExpression>();

	/**
	 * Initializes a new GeneralSchema with the provided {@link EvaluationExpression}s in proper sequence.
	 * 
	 * @param mappings
	 *        {@link EvaluationExpression}s that should be set as mappings
	 */
	public GeneralSchema(final EvaluationExpression... mappings) {
		super(mappings.length + 1, CollectionUtil.setRangeFrom(0, mappings.length));
		this.mappings = Arrays.asList(mappings);
	}

	/**
	 * Initializes a new GeneralSchema with the provided {@link EvaluationExpression}s. The mappings will be set in the
	 * same sequence as the Iterable provides them.
	 * 
	 * @param mappings
	 *        an Iterable over all {@link EvaluationExpression}s that should be set as mappings.
	 */
	public GeneralSchema(final List<EvaluationExpression> mappings) {
		super(mappings.size() + 1, CollectionUtil.setRangeFrom(0, mappings.size()));
		for (final EvaluationExpression exp : mappings)
			this.mappings.add(exp);
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
	public IntSet indicesOf(final EvaluationExpression expression) {
		final int index = this.mappings.indexOf(expression);
		if (index == -1)
			throw new IllegalArgumentException("Field not found.");
		return IntSets.singleton(index);
	}

	@Override
	public PactRecord jsonToRecord(final IJsonNode value, PactRecord target, final EvaluationContext context) {

		if (target == null)
			target = new PactRecord(this.mappings.size() + 1);
		target.setField(this.mappings.size(), SopremoUtil.wrap(value));

		for (int i = 0; i < this.mappings.size(); i++)
			target.setField(i, SopremoUtil.wrap(this.mappings.get(i).evaluate(value, null, context)));

		return target;
	}

	@Override
	public IJsonNode recordToJson(final PactRecord record, final IJsonNode target) {
		final IJsonNode source = SopremoUtil.unwrap(record.getField(this.mappings.size(), JsonNodeWrapper.class));

		if (target == null)
			return source;
		return this.reuseTargetNode(target, source);
	}

	private IJsonNode reuseTargetNode(IJsonNode target, final IJsonNode source) {
		target.clear();
		if (target.isObject())
			((IObjectNode) target).putAll((IObjectNode) source);
		else if (target.isArray())
			((IArrayNode) target).addAll((IArrayNode) source);
		else // target must be a PrimitiveNode
		if (source.getClass() != target.getClass())
			target = source;
		else
			target = SopremoUtil.reusePrimitive(source, target);

		return target;
	}

}
