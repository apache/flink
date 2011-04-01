package eu.stratosphere.sopremo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.dag.DirectedAcyclicGraphPrinter;
import eu.stratosphere.dag.Navigator;


public class Plan {
	private Collection<Operator> sinks;

	public Plan(Operator... sinks) {
		this.sinks = Arrays.asList(sinks);
	}

	public Plan(Collection<Operator> sinks) {
		this.sinks = sinks;
	}

	public static class PlanPrinter extends DirectedAcyclicGraphPrinter<Operator> {
		public PlanPrinter(Plan plan) {
			super(new Navigator<Operator>() {
				@Override
				public Iterable<Operator> getConnectedNodes(Operator node) {
					return node.getInputs();
				}
			}, plan.getAllNodes());
		}
	}

	@Override
	public String toString() {
		return new PlanPrinter(this).toString(80);
	}

	private Collection<Operator> getAllNodes() {
		List<Operator> nodes = new ArrayList<Operator>();
		enumerateNodes(nodes, sinks);
		return nodes;
	}

	private void enumerateNodes(Collection<Operator> seen, Collection<Operator> collection) {
		for (Operator node : collection) {
			if (!seen.contains(node)) {
				seen.add(node);
				enumerateNodes(seen, node.getInputs());
			}
		}
	}

	public static void main(String[] args) throws IOException {
		// Operator a = new Operator("A");
		// Operator c = new Operator("C", a, new Operator("B"));
		// Operator e = new Operator("E", c, new Operator("D"));
		// Operator f = new Operator("F", c, e, a);
		// Plan plan = new Plan(f);
		//
		// new PlanPrinter(plan).print(System.out, 10);
	}

	// public static class Source extends Node {
	// public Source() {
	// super();
	// }
	// }
	//
	// public static class Sink extends Node {
	// public Sink(Node... inputs) {
	// super(inputs);
	// }
	// }
	//
	// public static class
}
