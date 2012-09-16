package eu.stratosphere.sopremo.base;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.SingletonExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

@InputCardinality(1)
public class GlobalEnumeration extends ElementaryOperator<GlobalEnumeration> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8552367347318407324L;

	public static final EvaluationExpression CONCATENATION = new SingletonExpression("String") {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3340948936846733311L;

		@Override
		public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
			return TextNode.valueOf(String.format("%d_%d", ((ArrayNode) node).get(0), ((ArrayNode) node).get(1)));
		}

		@Override
		protected Object readResolve() {
			return CONCATENATION;
		}
	};

	public static final EvaluationExpression LONG_COMBINATION = new SingletonExpression("Long") {
		/**
		 * 
		 */
		private static final long serialVersionUID = -9084196126957908547L;

		@Override
		protected Object readResolve() {
			return LONG_COMBINATION;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode,
		 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
		 */
		@Override
		public IJsonNode evaluate(IJsonNode node, IJsonNode target, EvaluationContext context) {
			return LongNode.valueOf((((LongNode) ((ArrayNode) node).get(0)).getLongValue() << 48)
				+ ((LongNode) ((ArrayNode) node).get(1)).getLongValue());
		}
	};

	public final EvaluationExpression AUTO_ENUMERATION = new EvaluationExpression() {
		private static final long serialVersionUID = -5506784974227617703L;

		@Override
		public IJsonNode set(IJsonNode node, IJsonNode value, EvaluationContext context) {
			if (node.isObject()) {
				((IObjectNode) node).put(GlobalEnumeration.this.idFieldName, value);
				return node;
			}
			ObjectNode objectNode = new ObjectNode();
			objectNode.put(GlobalEnumeration.this.idFieldName, value);
			objectNode.put(GlobalEnumeration.this.valueFieldName, node);
			return objectNode;
		}

		@Override
		public IJsonNode evaluate(IJsonNode node, IJsonNode target, EvaluationContext context) {
			return node;
		}
	};

	private EvaluationExpression enumerationExpression = this.AUTO_ENUMERATION;

	private EvaluationExpression idGeneration = CONCATENATION;

	private String idFieldName = "_ID", valueFieldName = "value";

	public EvaluationExpression getEnumerationExpression() {
		return this.enumerationExpression;
	}

	public String getIdFieldName() {
		return this.idFieldName;
	}

	public EvaluationExpression getIdGeneration() {
		return this.idGeneration;
	}

	public EvaluationExpression getIdAccess() {
		return new ObjectAccess(this.idFieldName);
	}

	public void setEnumerationExpression(final EvaluationExpression enumerationExpression) {
		if (enumerationExpression == null)
			throw new NullPointerException();

		this.enumerationExpression = enumerationExpression;
	}

	public void setIdFieldName(final String enumerationFieldName) {
		if (enumerationFieldName == null)
			throw new NullPointerException();

		this.idFieldName = enumerationFieldName;
	}

	public GlobalEnumeration withIdFieldName(String enumerationFieldName) {
		setIdFieldName(enumerationFieldName);
		return this;
	}

	public GlobalEnumeration withValueFieldName(String valueFieldName) {
		setValueFieldName(valueFieldName);
		return this;
	}

	public GlobalEnumeration withEnumerationExpression(EvaluationExpression enumerationExpression) {
		setEnumerationExpression(enumerationExpression);
		return this;
	}

	public GlobalEnumeration withIdGeneration(EvaluationExpression idGeneration) {
		setIdGeneration(idGeneration);
		return this;
	}

	public void setIdGeneration(final EvaluationExpression idGeneration) {
		if (idGeneration == null)
			throw new NullPointerException("idGeneration must not be null");

		this.idGeneration = idGeneration;
	}

	public String getValueFieldName() {
		return this.valueFieldName;
	}

	public void setValueFieldName(String valueFieldName) {
		if (valueFieldName == null)
			throw new NullPointerException("valueFieldName must not be null");

		this.valueFieldName = valueFieldName;
	}

	public static class Implementation extends SopremoMap {
		private EvaluationExpression enumerationExpression;

		private CachingExpression<IJsonNode> idGeneration;

		private LongNode counter;

		private IArrayNode params;

		@Override
		public void open(Configuration parameters) {
			super.open(parameters);
			final IntNode taskId = new IntNode(parameters.getInteger("pact.parallel.task.id", 0));
			this.counter = LongNode.valueOf(0);
			this.params = JsonUtil.asArray(taskId, this.counter);
		}

		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			this.counter.setValue(this.counter.getLongValue() + 1);
			final IJsonNode id = this.idGeneration.evaluate(this.params, this.getContext());
			out.collect(this.enumerationExpression.set(value, id, this.getContext()));
		}
	}

}
