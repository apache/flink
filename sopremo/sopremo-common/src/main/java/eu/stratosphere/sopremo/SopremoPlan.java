package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.base.Sink;
import eu.stratosphere.sopremo.base.Source;
import eu.stratosphere.sopremo.function.BuiltinFunctions;
import eu.stratosphere.util.dag.DependencyAwareGraphTraverser;
import eu.stratosphere.util.dag.GraphPrinter;
import eu.stratosphere.util.dag.GraphTraverseListener;

/**
 * Encapsulate a complete query in Sopremo and translates it to a Pact {@link Plan}.
 * 
 * @author Arvid Heise
 */
public class SopremoPlan {
	private SopremoModule module;

	private EvaluationContext context = new EvaluationContext();

	/**
	 * Initializes SopremoPlan.
	 * 
	 * @param sinks
	 */
	public SopremoPlan(Collection<Sink> sinks) {
		this.module = new SopremoModule(0, 0);
		for (Sink sink : sinks) 
			this.module.addInternalOutput(sink);
		this.context.getFunctionRegistry().register(BuiltinFunctions.class);
	}

	public SopremoPlan(Sink... sinks) {
		this(Arrays.asList(sinks));
	}

	public Plan asPactPlan() {
		return this.module.asPactPlan(context);
	}

	public Collection<Contract> assemblePact() {
		return this.module.assemblePact(context);
	}

	public Iterable<? extends Operator> getReachableNodes() {
		return module.getReachableNodes();
	}

	
}
