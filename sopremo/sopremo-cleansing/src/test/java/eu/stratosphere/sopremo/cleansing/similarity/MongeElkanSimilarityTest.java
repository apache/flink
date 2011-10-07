package eu.stratosphere.sopremo.cleansing.similarity;
import static eu.stratosphere.sopremo.JsonUtil.createObjectNode;
import junit.framework.Assert;

import org.junit.Test;

import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.jsondatamodel.DoubleNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;

public class MongeElkanSimilarityTest extends SopremoTest<MongeElkanSimilarity> {
	@Override
	protected MongeElkanSimilarity createDefaultInstance(int index) {
		return new MongeElkanSimilarity(new ConstantExpression(index), new InputSelection(0), new InputSelection(1));
	}
	
	@Test
	public void test() {
		ObjectNode left = createObjectNode("names", new String[]{"Joe", "J.", "Joseph"});
		ObjectNode right = createObjectNode("names2", new String[]{"Joseph"});
		
		SimmetricFunction baseMeasure = new SimmetricFunction(new JaroWinkler(), new InputSelection(0), new InputSelection(1));
		MongeElkanSimilarity mongeElkanSimilarity = new MongeElkanSimilarity(baseMeasure, 
			new PathExpression( new InputSelection(0), new ObjectAccess("names")),
			new PathExpression( new InputSelection(1), new ObjectAccess("names2")));
		
		Assert.assertEquals(0.8, ((DoubleNode)mongeElkanSimilarity.evaluate(JsonUtil.asArray(left, right), new EvaluationContext())).getDoubleValue(), 0.1);
	}
}
