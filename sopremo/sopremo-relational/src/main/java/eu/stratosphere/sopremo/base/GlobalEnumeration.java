package eu.stratosphere.sopremo.base;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractTask;
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
	private EvaluationExpression enumerationExpression = EvaluationExpression.AS_KEY;

	public EvaluationExpression getEnumerationExpression() {
		return enumerationExpression;
	}

	public void setEnumerationExpression(EvaluationExpression enumerationExpression) {
		if(enumerationExpression == null)
			throw new NullPointerException();
		
		this.enumerationExpression = enumerationExpression;
	}
	
	public void setEnumerationFieldName(String field) {
		if(field == null)
			throw new NullPointerException();
		
		ObjectCreation objectMerge = new ObjectCreation();
		objectMerge.addMapping(new ObjectCreation.CopyFields(new InputSelection(0)));
		objectMerge.addMapping(field, new InputSelection(1));
		this.enumerationExpression = objectMerge;
	}
	
	public String getEnumerationFieldName() {
		if(enumerationExpression instanceof ObjectCreation && ((ObjectCreation) enumerationExpression).getMappingSize() == 2) 
			return ((ObjectCreation) enumerationExpression).getMapping(1).getTarget();
		return null;
	}

	public GlobalEnumeration(JsonStream input) {
		super(input);
	}

	public static class Implementation extends
			SopremoMap<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
		private EvaluationExpression enumerationExpression;
		private String taskBase;
		private int counter;
		
		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			taskBase = parameters.getString(AbstractTask.TASK_ID, "") + "_";
			counter = 0;
		}
		
		@Override
		protected void map(JsonNode key, JsonNode value, JsonCollector out) {
			TextNode id = new TextNode(taskBase + (counter++));
			if(enumerationExpression == EvaluationExpression.SAME_KEY)
				out.collect(id, value);
			else out.collect(key, enumerationExpression.evaluate(JsonUtil.asArray(value, id), getContext()));
		}
	}
}
