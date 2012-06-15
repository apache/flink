package eu.stratosphere.sopremo.base;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.SingletonExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

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

	private static final EvaluationExpression AUTO = new EvaluationExpression() {
		private static final long serialVersionUID = -5506784974227617703L;

		public IJsonNode set(IJsonNode node, IJsonNode value, EvaluationContext context) {
			if (node.isObject()) {
				((IObjectNode) node).put("id", value);
				return node;
			}
			ObjectNode objectNode = new ObjectNode();
			objectNode.put("id", value);
			objectNode.put("value", node);
			return objectNode;
		}

		public IJsonNode evaluate(IJsonNode node, IJsonNode target, EvaluationContext context) {
			return node;
		}
	};

	private EvaluationExpression enumerationExpression = AUTO;

	private EvaluationExpression idGeneration = CONCATENATION;

	public EvaluationExpression getEnumerationExpression() {
		return this.enumerationExpression;
	}

	public String getEnumerationFieldName() {
		if (this.enumerationExpression instanceof ObjectCreation
			&& ((ObjectCreation) this.enumerationExpression).getMappingSize() == 2)
			return (String) ((ObjectCreation) this.enumerationExpression).getMapping(1).getTarget();
		return null;
	}

	public EvaluationExpression getIdGeneration() {
		return this.idGeneration;
	}

	public void setEnumerationExpression(final EvaluationExpression enumerationExpression) {
		if (enumerationExpression == null)
			throw new NullPointerException();

		this.enumerationExpression = enumerationExpression;
	}

	public void setEnumerationFieldName(final String field) {
		if (field == null)
			throw new NullPointerException();

		final ObjectCreation objectMerge = new ObjectCreation();
		objectMerge.addMapping(new ObjectCreation.CopyFields(new InputSelection(0)));
		objectMerge.addMapping(field, new InputSelection(1));
		this.enumerationExpression = objectMerge;
	}

	public void setIdGeneration(final EvaluationExpression idGeneration) {
		if (idGeneration == null)
			throw new NullPointerException("idGeneration must not be null");

		this.idGeneration = idGeneration;
	}

	public static class Implementation extends SopremoMap {
		private EvaluationExpression enumerationExpression;

		private CachingExpression<IJsonNode> idGeneration;

		private LongNode counter;

		private ArrayNode params;

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
			out.collect(this.enumerationExpression.set(value, id, getContext()));
		}
	}

}
