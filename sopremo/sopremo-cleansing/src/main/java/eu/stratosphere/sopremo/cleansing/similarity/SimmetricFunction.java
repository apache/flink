package eu.stratosphere.sopremo.cleansing.similarity;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.DoubleNode;

import uk.ac.shef.wit.simmetrics.similaritymetrics.InterfaceStringMetric;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class SimmetricFunction extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -849616725854431350L;

	private transient InterfaceStringMetric metric;

	private EvaluationExpression leftExpression, rightExpression;

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		String left = getValueAsText(leftExpression.evaluate(node, context));
		String right = getValueAsText(rightExpression.evaluate(node, context));
		return DoubleNode.valueOf(metric.getSimilarity(left, right));
	}

	protected String getValueAsText(JsonNode node) {
		return node.isTextual() ? node.getTextValue() : node.toString();
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		metric = SopremoUtil.deserializeObject(ois, InterfaceStringMetric.class);
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		SopremoUtil.serializeObject(oos, metric);
	}

	public SimmetricFunction(InterfaceStringMetric metric, EvaluationExpression leftExpression,
			EvaluationExpression rightExpression) {
		this.metric = metric;
		this.leftExpression = leftExpression;
		this.rightExpression = rightExpression;
	}

}
