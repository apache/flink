package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.pact.SopremoMap;

public class ValueSplitter extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2967507260239105002L;

	private List<EvaluationExpression> projections = new ArrayList<EvaluationExpression>();

	private EvaluationExpression path, keyProjection = new ArrayAccess(1),
			valueProjection = new ArrayAccess(0);

	private Class<? extends Stub<?, ?>> implementation = ExplicitProjections.class;

	public ValueSplitter(JsonStream input) {
		super(input);
	}

	public ValueSplitter addProjection(EvaluationExpression... projections) {
		this.implementation = ExplicitProjections.class;
		for (EvaluationExpression evaluationExpression : projections)
			this.projections.add(evaluationExpression);
		return this;
	}

	public EvaluationExpression getPath() {
		return this.path;
	}

	public EvaluationExpression getValueProjection() {
		return this.valueProjection;
	}

	public ValueSplitter withArrayProjection(EvaluationExpression arrayPath) {
		this.path = arrayPath;
		this.implementation = ArraySplit.class;
		return this;
	}

	/**
	 * (element, index/fieldName, array/object, node) -&gt; value
	 * 
	 * @param valueProjection
	 * @return this
	 */
	public ValueSplitter withValueProjection(EvaluationExpression valueProjection) {
		setValueProjection(valueProjection);
		return this;
	}

	/**
	 * (element, index/fieldName, array/object, node) -&gt; value
	 * 
	 * @param valueProjection
	 */
	public void setValueProjection(EvaluationExpression valueProjection) {
		this.valueProjection = valueProjection;
	}

	/**
	 * (element, index/fieldName, array/object, node) -&gt; key
	 * 
	 * @param keyProjection
	 * @return
	 */
	public ValueSplitter withKeyProjection(EvaluationExpression keyProjection) {
		this.keyProjection = keyProjection;
		return this;
	}

	public EvaluationExpression getKeyProjection() {
		return this.keyProjection;
	}

	public ValueSplitter setObjectProjection(EvaluationExpression arrayPath) {
		this.path = arrayPath;
		this.implementation = ObjectSplit.class;
		return this;
	}

	@Override
	protected Class<? extends Stub<?, ?>> getStubClass() {
		return implementation;
	}

	public static class ExplicitProjections extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
		private List<EvaluationExpression> projections = new ArrayList<EvaluationExpression>();

		@Override
		protected void map(JsonNode key, JsonNode value, JsonCollector out) {
			for (EvaluationExpression projection : this.projections)
				out.collect(key, projection.evaluate(value, this.getContext()));
		}
	}

	public static class ArraySplit extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
		private EvaluationExpression path, valueProjection, keyProjection;

		@Override
		protected void map(JsonNode key, JsonNode value, JsonCollector out) {
			final JsonNode array = path.evaluate(value, getContext());
			if (!array.isArray())
				throw new EvaluationException("Cannot split non-array");

			int index = 0;
			final EvaluationContext context = this.getContext();
			for (JsonNode element : array) {
				JsonNode indexNode = IntNode.valueOf(index++);
				out.collect(
					keyProjection.evaluate(JsonUtil.asArray(element, indexNode, array, value), context),
					valueProjection.evaluate(JsonUtil.asArray(element, indexNode, array, value), context));
			}
		}
	}

	public static class ObjectSplit extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
		private EvaluationExpression path, valueProjection, keyProjection;

		@Override
		protected void map(JsonNode key, JsonNode value, JsonCollector out) {
			final JsonNode object = path.evaluate(value, getContext());
			if (!object.isObject())
				throw new EvaluationException("Cannot split non-object");

			final Iterator<String> fieldNames = object.getFieldNames();
			final EvaluationContext context = this.getContext();
			while (fieldNames.hasNext()) {
				String field = (String) fieldNames.next();
				final TextNode fieldNode = TextNode.valueOf(field);
				out.collect(
					keyProjection.evaluate(JsonUtil.asArray(value.get(field), fieldNode, object, value), context),
					valueProjection.evaluate(JsonUtil.asArray(value.get(field), fieldNode, object, value), context));
			}
		}
	}
}
