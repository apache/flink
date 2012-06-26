package eu.stratosphere.sopremo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.plan.PactModule;

/**
 * A composite operator may be composed of several {@link ElementaryOperator}s and other CompositeOperators.<br>
 * This class should always be used as a base for new operators which would be translated to more than one PACT,
 * especially if some kind of projection or selection is used.
 * 
 * @author Arvid Heise
 */
public abstract class CompositeOperator<Self extends CompositeOperator<Self>> extends Operator<Self> {
	private static final Log LOG = LogFactory.getLog(CompositeOperator.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = -9172753270465124102L;

	/**
	 * Initializes the CompositeOperator with the given number of outputs.
	 * 
	 * @param numberOfOutputs
	 *        the number of outputs
	 */
	public CompositeOperator(final int numberOfInputs, final int numberOfOutputs) {
		super(numberOfInputs, numberOfInputs, numberOfOutputs, numberOfOutputs);
	}

	/**
	 * Initializes the CompositeOperator with the given number of inputs.
	 * 
	 * @param minInputs
	 *        the minimum number of inputs
	 * @param maxInputs
	 *        the maximum number of inputs
	 */
	public CompositeOperator(int minInputs, int maxInputs, int minOutputs, int maxOutputs) {
		super(minInputs, maxInputs, minOutputs, maxOutputs);
	}

	/**
	 * Initializes the CompositeOperator with the number of outputs set to 1.
	 */
	public CompositeOperator() {
		super();
	}

	/**
	 * Returns a {@link SopremoModule} that consists entirely of {@link ElementaryOperator}s. The module can be seen as
	 * an expanded version of this operator where all CompositeOperators are recursively translated to
	 * ElementaryOperators.
	 * 
	 * @return a module of ElementaryOperators
	 */
	@Override
	public abstract ElementarySopremoModule asElementaryOperators();

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		if (LOG.isTraceEnabled())
			LOG.trace("Transforming\n" + this);
		final ElementarySopremoModule elementaryPlan = this.asElementaryOperators();
		if (LOG.isTraceEnabled())
			LOG.trace(" to elementary plan\n" + elementaryPlan);
		context.pushOperator(this);
		final PactModule pactModule = elementaryPlan.asPactModule(context);
		context.popOperator();
		return pactModule;
	}
}
