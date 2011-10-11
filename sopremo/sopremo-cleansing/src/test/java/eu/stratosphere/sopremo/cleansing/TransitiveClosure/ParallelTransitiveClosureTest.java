package eu.stratosphere.sopremo.cleansing.TransitiveClosure;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.cleansing.transitiveClosure.TransitiveClosure;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class ParallelTransitiveClosureTest {

	@Test
	public void shouldDoSimplePartitioningAndFindTransitiveClosureInIt() {
		final TransitiveClosure transitiveClosure = new TransitiveClosure();
		// transitiveClosure.setClosureMode(ClosureMode.LINKS);
		// transitiveClosure.setIdProjection(new ObjectAccess("id"));
		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(transitiveClosure);
		
		String input = SopremoTest.getResourcePath("transitiveClosure.json");

		sopremoTestPlan
			.getInput(0).load(input);
		
		String output = SopremoTest.getResourcePath("transitiveClosureResult.json");
		sopremoTestPlan.getExpectedOutput(0).load(output);
//			.
//			add(createArrayNode(createObjectNode("id", 11, "name", "a", "partition", 1),
//				createObjectNode("id", 22, "name", "b", "partition", 1)))
//			.
//
//			add(createArrayNode(createObjectNode("id", 11, "name", "a", "partition", 1),
//				createObjectNode("id", 23, "name", "c", "partition", 1)))
//			.
//
//			add(createArrayNode(createObjectNode("id", 14, "name", "e", "partition", 2),
//				createObjectNode("id", 25, "name", "d", "partition", 2)))
//			.
//			add(createArrayNode(createObjectNode("id", 16, "name", "a", "partition", 2),
//				createObjectNode("id", 26, "name", "f", "partition", 2)));

		// Object[] constants4 = { createObjectNode("id", 11, "name", "a"), createObjectNode("id", 22, "name", "b") };
		// Object[] constants5 = { createObjectNode("id", 11, "name", "a"), createObjectNode("id", 23, "name", "c") };
		// Object[] constants6 = { createObjectNode("id", 22, "name", "b"), createObjectNode("id", 23, "name", "c") };
		// Object[] constants7 = { createObjectNode("id", 14, "name", "e"), createObjectNode("id", 25, "name", "d") };
		// Object[] constants8 = { createObjectNode("id", 16, "name", "a"), createObjectNode("id", 25, "name", "d") };
		// Object[] constants9 = { createObjectNode("id", 14, "name", "e"), createObjectNode("id", 16, "name", "a") };

		// for (KeyValuePair<JsonNode, JsonNode> kv : sopremoTestPlan.getInput(0)) {
		// sopremoTestPlan.getExpectedOutput(0).add(kv.getValue());
		// }
		// sopremoTestPlan.getExpectedOutput(0).add(
		// createArrayNode(createObjectNode("id", 11, "name", "a", "partition", 1),
		// createObjectNode("id", 23, "name", "c", "partition", 1)));
		// sopremoTestPlan.getExpectedOutput(0).
		// add(createArrayNode(constants4)).
		// add(createArrayNode(constants5)).
		// add(createArrayNode(constants6)).
		// add(createArrayNode(constants7)).
		// add(createArrayNode(constants8)).
		// add(createArrayNode(constants9));
		sopremoTestPlan.run();
	}

}
