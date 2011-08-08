package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.Collection;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.plan.Plan;

/**
 * Encapsulate a complete query in Sopremo and translates it to a Pact {@link Plan}.
 * 
 * @author Arvid Heise
 */
public class SopremoPlan {
	private final SopremoModule module;

	private EvaluationContext context = new EvaluationContext();

	/**
	 * Initializes SopremoPlan using the given list of {@link Sink}s.
	 * 
	 * @param sinks
	 *        the sinks of the Sopremo plan
	 */
	public SopremoPlan(final Collection<Sink> sinks) {
		this.module = new SopremoModule("plan", 0, 0);
		for (final Sink sink : sinks)
			this.module.addInternalOutput(sink);
	}

	/**
	 * Initializes SopremoPlan using the given list of {@link Sink}s.
	 * 
	 * @param sinks
	 *        the sinks of the Sopremo plan
	 */
	public SopremoPlan(final Sink... sinks) {
		this(Arrays.asList(sinks));
	}

	/**
	 * Converts the Sopremo module to a Pact {@link Plan}.
	 * 
	 * @return the converted Pact plan
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Plan asPactPlan() {
		return new Plan((Collection) this.module.assemblePact(this.context));
	}

	/**
	 * Assembles the Pacts of the contained Sopremo operators and returns a list of all Pact sinks. These sinks may
	 * either be directly a {@link DataSinkContract} or an unconnected {@link Contract}.
	 * 
	 * @return a list of Pact sinks
	 */
	public Collection<Contract> assemblePact() {
		return this.module.assemblePact(this.context);
	}

	/**
	 * Returns all operators that are either (internal) {@link Sink}s or included in the reference graph.
	 * 
	 * @return all operators in this module
	 */
	public Iterable<? extends Operator> getContainedOperators() {
		return this.module.getReachableNodes();
	}

	/**
	 * Returns the evaluation context of this plan.
	 * 
	 * @return the evaluation context
	 */
	public EvaluationContext getContext() {
		return this.context;
	}

	/**
	 * Sets the evaluation context of this plan.
	 * 
	 * @param context
	 *        the evaluation context
	 */
	public void setContext(final EvaluationContext context) {
		if (context == null)
			throw new NullPointerException("context must not be null");

		this.context = context;
	}

	@Override
	public String toString() {
		return this.module.toString();
	}
}
