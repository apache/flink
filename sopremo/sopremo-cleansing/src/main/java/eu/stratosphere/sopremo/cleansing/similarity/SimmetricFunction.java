package eu.stratosphere.sopremo.cleansing.similarity;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

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

	private final EvaluationExpression leftExpression, rightExpression;

	public SimmetricFunction(final InterfaceStringMetric metric, final EvaluationExpression leftExpression,
			final EvaluationExpression rightExpression) {
		this.metric = metric;
		this.leftExpression = leftExpression;
		this.rightExpression = rightExpression;
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		final String left = this.getValueAsText(this.leftExpression.evaluate(node, context));
		final String right = this.getValueAsText(this.rightExpression.evaluate(node, context));
		return DoubleNode.valueOf(this.metric.getSimilarity(left, right));
	}

	protected String getValueAsText(final JsonNode node) {
		return node.isTextual() ? node.getTextValue() : node.toString();
	}

	private void readObject(final ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		this.metric = SopremoUtil.deserializeObject(ois, InterfaceStringMetric.class);
	}

	private void writeObject(final ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		SopremoUtil.serializeObject(oos, this.metric);
	}

}
