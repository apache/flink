package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.IntNode;
import org.junit.Assert;
import org.junit.Test;

public class OrExpressionTest extends EvaluableExpressionTest<OrExpression>{

	private static final UnaryExpression TRUE = new UnaryExpression(new ConstantExpression(BooleanNode.TRUE));
	private static final UnaryExpression FALSE = new UnaryExpression(new ConstantExpression(BooleanNode.FALSE));
	
	@Override
	protected OrExpression createDefaultInstance(int index) {
		
		switch(index){
		case 0:{
			return new OrExpression(TRUE);
		}
		case 1:{
			return new OrExpression(TRUE,TRUE);
		}
		case 2:{
			return new OrExpression(TRUE,TRUE,TRUE);
		}
		}
		
		return new OrExpression(FALSE);
	}
	
	@Test
	public void shouldBeTrueIfOneExprIsTrue(){
		final JsonNode result = new OrExpression(FALSE,TRUE,FALSE).evaluate(IntNode.valueOf(42), this.context);
		
		Assert.assertEquals(BooleanNode.TRUE, result);
	}
	
	@Test
	public void shouldBeFalseIfNoExprIsTrue(){
		final JsonNode result = new OrExpression(FALSE,FALSE,FALSE).evaluate(IntNode.valueOf(42), this.context);
		
		Assert.assertEquals(BooleanNode.FALSE, result);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfExpressionsAreEmpty(){
		new OrExpression();
		//final JsonNode result = new OrExpression().evaluate(IntNode.valueOf(42), this.context);
	}
	
}
