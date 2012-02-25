package eu.stratosphere.sopremo.type;

import junit.framework.Assert;

import org.junit.Test;

public class ArrayNodeTest extends ArrayNodeBaseTest<ArrayNode> {

	@Override
	public void initArrayNode() {
		this.node = new ArrayNode();
		int numberOfNodes = 10;
		
		for(int i = 0; i < numberOfNodes; i++){
			this.node.add(i, IntNode.valueOf(i));
		}
	}
	
	@Test
	public void shouldReturnCorrectSubarray(){
		int numberOfNodesInSubarray = 5;
		int startIndex = 3;
		IArrayNode result = new ArrayNode();
		
		for(int i = 0; i < numberOfNodesInSubarray; i++){
			result.add(i, IntNode.valueOf(startIndex + i));
		}
		
		Assert.assertEquals(result, this.node.subArray(startIndex, startIndex + numberOfNodesInSubarray));
	}
}
