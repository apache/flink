package eu.stratosphere.sopremo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.dag.Printer;
import eu.stratosphere.dag.TraverseListener;
import eu.stratosphere.dag.Traverser;

public class Plan {
	private Collection<Operator> sinks;

	public Plan(Operator... sinks) {
		this.sinks = Arrays.asList(sinks);
	}

	public Plan(Collection<Operator> sinks) {
		this.sinks = sinks;
	}

	public static class PlanPrinter extends Printer<Operator> {
		public PlanPrinter(Plan plan) {
			super(new OperatorNavigator(), plan.getAllNodes());
		}
	}

	public Collection<Operator> getSinks() {
		return sinks;
	}

	@Override
	public String toString() {
		return new PlanPrinter(this).toString(80);
	}

	public List<Operator> getAllNodes() {
		final List<Operator> nodes = new ArrayList<Operator>();
		new Traverser<Operator>(new OperatorNavigator(), sinks).traverse(new TraverseListener<Operator>() {
			@Override
			public void nodeTraversed(Operator node) {
				nodes.add(node);
			}
		});
		return nodes;
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
