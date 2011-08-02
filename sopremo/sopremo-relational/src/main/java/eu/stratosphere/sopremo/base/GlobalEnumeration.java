package eu.stratosphere.sopremo.base;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.TextNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class GlobalEnumeration extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8552367347318407324L;

	public static final EvaluationExpression CONCATENATION = new EvaluationExpression() {
		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			return TextNode.valueOf(String.format("%d_%d", node.get(0), node.get(1)));
		}
	};

	public static final EvaluationExpression LONG_COMBINATION = new EvaluationExpression() {
		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			return LongNode.valueOf(node.get(0).getLongValue() << 48 + node.get(1).getLongValue());
		}
	};

	private EvaluationExpression enumerationExpression = EvaluationExpression.AS_KEY;

	private EvaluationExpression idGeneration = CONCATENATION;

	public GlobalEnumeration(JsonStream input) {
		super(input);
	}

	public EvaluationExpression getEnumerationExpression() {
		return enumerationExpression;
	}

	public String getEnumerationFieldName() {
		if (enumerationExpression instanceof ObjectCreation
			&& ((ObjectCreation) enumerationExpression).getMappingSize() == 2)
			return ((ObjectCreation) enumerationExpression).getMapping(1).getTarget();
		return null;
	}

	public EvaluationExpression getIdGeneration() {
		return idGeneration;
	}

	public void setEnumerationExpression(EvaluationExpression enumerationExpression) {
		if (enumerationExpression == null)
			throw new NullPointerException();

		this.enumerationExpression = enumerationExpression;
	}

	public void setEnumerationFieldName(String field) {
		if (field == null)
			throw new NullPointerException();

		ObjectCreation objectMerge = new ObjectCreation();
		objectMerge.addMapping(new ObjectCreation.CopyFields(new InputSelection(0)));
		objectMerge.addMapping(field, new InputSelection(1));
		this.enumerationExpression = objectMerge;
	}

	public void setIdGeneration(EvaluationExpression idGeneration) {
		if (idGeneration == null)
			throw new NullPointerException("idGeneration must not be null");

		this.idGeneration = idGeneration;
	}

	public static class Implementation extends
			SopremoMap<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
		private EvaluationExpression enumerationExpression, idGeneration;

		private long counter;

		private CompactArrayNode params;

		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			IntNode taskId = new IntNode(parameters.getInteger(AbstractTask.TASK_ID, 0));
			counter = 0;
			params = JsonUtil.asArray(taskId, LongNode.valueOf(counter));
		}

		@Override
		protected void map(JsonNode key, JsonNode value, JsonCollector out) {
			JsonNode id = idGeneration.evaluate(params, getContext());
			params.getChildren()[1] = LongNode.valueOf(counter++);

			if (enumerationExpression == EvaluationExpression.SAME_KEY)
				out.collect(id, value);
			else
				out.collect(key, enumerationExpression.evaluate(JsonUtil.asArray(value, id), getContext()));
		}
	}

}
