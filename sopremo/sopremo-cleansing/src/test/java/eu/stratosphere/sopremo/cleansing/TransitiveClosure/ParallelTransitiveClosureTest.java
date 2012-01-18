package eu.stratosphere.sopremo.cleansing.TransitiveClosure;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.cleansing.record_linkage.ClosureMode;
import eu.stratosphere.sopremo.cleansing.transitiveClosure.BinarySparseMatrix;
import eu.stratosphere.sopremo.cleansing.transitiveClosure.TransitiveClosure;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class ParallelTransitiveClosureTest {

	@Ignore
	@Test
	public void shouldDoSimplePartitioningAndFindTransitiveClosureInIt() {
		final TestTransitiveClosure transitiveClosure = new TestTransitiveClosure();
		transitiveClosure.setPhase(1);

		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(transitiveClosure);
		String nullInput = SopremoTest.getResourcePath("null.json");

		String input = SopremoTest.getResourcePath("phase1.json");

		sopremoTestPlan.getInput(1).load(nullInput);
		sopremoTestPlan
			.getInput(0).load(input);

		String output = SopremoTest.getResourcePath("phase1Result.json");
		sopremoTestPlan.getExpectedOutput(0).load(output);
		sopremoTestPlan.trace();

		sopremoTestPlan.run();
	}

	@Ignore
	@Test
	public void shouldFindTransitiveClosureWithinRowsAndColumns() {
		final TestTransitiveClosure transitiveClosure = new TestTransitiveClosure();
		transitiveClosure.setPhase(2);
		transitiveClosure.setNumberOfPartitions(2);

		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(transitiveClosure);
		String nullInput = SopremoTest.getResourcePath("null.json");
		String input = SopremoTest.getResourcePath("phase2.json");

		sopremoTestPlan
			.getInput(0).load(input);
		sopremoTestPlan.getInput(1).load(nullInput);

		String output = SopremoTest.getResourcePath("phase2Result.json");
		sopremoTestPlan.getExpectedOutput(0).load(output);

		sopremoTestPlan.trace();
		sopremoTestPlan.run();
	}

	@Test
	public void shouldFindTransitiveClosureInWholeMatrix() {
		final TransitiveClosure transitiveClosure = new TransitiveClosure();
//		transitiveClosure.setPhase(3);
		transitiveClosure.setNumberOfPartitions(2);
		
		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(transitiveClosure);
		String nullInput = SopremoTest.getResourcePath("null.json");
		String input = SopremoTest.getResourcePath("gen.json");

		sopremoTestPlan.getInput(0).load(input);
		sopremoTestPlan.getInput(1).load(nullInput);

		// String output = SopremoTest.getResourcePath("gen.json");
		// sopremoTestPlan.getExpectedOutput(0).load(output);

		final eu.stratosphere.sopremo.cleansing.record_linkage.TransitiveClosure transClos2 = new eu.stratosphere.sopremo.cleansing.record_linkage.TransitiveClosure()
			.withClosureMode(ClosureMode.LINKS);
		final SopremoTestPlan sopremoTestPlan2 = new SopremoTestPlan(transClos2);
		sopremoTestPlan2.getInput(0).load(input);

		// sopremoTestPlan.trace();
		// sopremoTestPlan.toString();
		long started = System.currentTimeMillis();
		System.out.println("###starting plan 2 (warshall)###");
		sopremoTestPlan2.run();
		System.out.println("###finished plan 2 (warshall), duration: " + (System.currentTimeMillis() - started ) /1000 + "s ###" );
		started = System.currentTimeMillis();
		System.out.println("###starting plan 1 (sopremo)###");
		sopremoTestPlan.run();
		System.out.println("###finished plan 1 (sopremo), duration: " + (System.currentTimeMillis() - started ) /1000 + "s ###" );
		
		System.out.println("###comparing results###");
		sopremoTestPlan.getActualOutput(0).assertEquals(sopremoTestPlan2
			.getActualOutput(0));
		System.out.println("###finished###");
	}

	/**
	 * Tests the algorithm only
	 */
	@Test
	public void testWarshall() {
		final BinarySparseMatrix matrix = new BinarySparseMatrix();
		final JsonNode[] nodes = new JsonNode[6];
		for (int index = 0; index < nodes.length; index++)
			nodes[index] = IntNode.valueOf(index);

		matrix.set(nodes[0], nodes[1]);
		matrix.set(nodes[1], nodes[2]);
		matrix.set(nodes[2], nodes[3]);

		matrix.set(nodes[4], nodes[5]);
		matrix.makeSymmetric();

		TransitiveClosure.warshall(matrix);

		final BinarySparseMatrix expected = new BinarySparseMatrix();
		expected.set(nodes[0], nodes[1]);
		expected.set(nodes[1], nodes[2]);
		expected.set(nodes[2], nodes[3]);
		// newly found pairs
		expected.set(nodes[0], nodes[2]);
		expected.set(nodes[0], nodes[3]);
		expected.set(nodes[1], nodes[3]);

		expected.set(nodes[4], nodes[5]);
		expected.makeSymmetric();

		Assert.assertEquals(expected, matrix);
	}
}
