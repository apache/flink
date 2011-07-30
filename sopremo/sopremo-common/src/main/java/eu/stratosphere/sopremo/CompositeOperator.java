package eu.stratosphere.sopremo;

import java.util.List;

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
public abstract class CompositeOperator extends Operator {
	private static final Log LOG = LogFactory.getLog(CompositeOperator.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = -9172753270465124102L;

	/**
	 * Initializes the CompositeOperator with the given number of outputs and the given input {@link JsonStream}s. A
	 * JsonStream is either the output of another operator or the operator itself.
	 * 
	 * @param numberOfOutputs
	 *        the number of outputs
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	public CompositeOperator(int numberOfOutputs, JsonStream... inputs) {
		super(numberOfOutputs, inputs);
	}

	/**
	 * Initializes the CompositeOperator with the given number of outputs and the given input {@link JsonStream}s. A
	 * JsonStream is either the output of another operator or the operator itself.
	 * 
	 * @param numberOfOutputs
	 *        the number of outputs
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	public CompositeOperator(int numberOfOutputs, List<? extends JsonStream> inputs) {
		super(numberOfOutputs, inputs);
	}

	/**
	 * Initializes the CompositeOperator with the given input {@link JsonStream}s. A JsonStream is
	 * either the output of another operator or the operator itself. The number of outputs is set to 1.
	 * 
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	public CompositeOperator(JsonStream... inputs) {
		super(inputs);
	}

	/**
	 * Initializes the CompositeOperator with the given input {@link JsonStream}s. A JsonStream is
	 * either the output of another operator or the operator itself. The number of outputs is set to 1.
	 * 
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	public CompositeOperator(List<? extends JsonStream> inputs) {
		super(inputs);
	}

	/**
	 * Returns a {@link SopremoModule} that consists entirely of {@link ElementaryOperator}s. The module can be seen as
	 * an expanded version of this operator where all CompositeOperators are recursively translated to
	 * ElementaryOperators.
	 * 
	 * @return a module of ElementaryOperators
	 */
	public abstract SopremoModule asElementaryOperators();

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		if (LOG.isTraceEnabled())
			LOG.trace("Transforming\n" + this);
		SopremoModule elementaryPlan = this.asElementaryOperators();
		if (LOG.isTraceEnabled())
			LOG.trace(" to elementary plan\n" + elementaryPlan);
		return elementaryPlan.asPactModule(context);
	}
}
