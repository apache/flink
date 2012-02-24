package eu.stratosphere.sopremo;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.util.CollectionUtil;
import eu.stratosphere.util.ConversionIterator;
import eu.stratosphere.util.IteratorUtil;
import eu.stratosphere.util.WrappingIterable;

/**
 * A composite operator may be composed of several {@link ElementaryOperator}s and other CompositeOperators.<br>
 * This class should always be used as a base for new operators which would be translated to more than one PACT,
 * especially if some kind of projection or selection is used.
 * 
 * @author Arvid Heise
 */
@InputCardinality(min = 1, max = Integer.MAX_VALUE)
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
	public CompositeOperator(final int numberOfOutputs) {
		super(numberOfOutputs);
	}

	/**
	 * Initializes the CompositeOperator with the number of outputs set to 1.
	 */
	public CompositeOperator() {
		super();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.Operator#getKeyExpression()
	 */
	@Override
	public Iterable<EvaluationExpression> getKeyExpression() {
		final SopremoModule elementaryPlan = this.asElementaryOperators();		
		return CollectionUtil.mergeUnique(new WrappingIterable<Operator, Iterable<EvaluationExpression>>() {
			/* (non-Javadoc)
			 * @see eu.stratosphere.util.WrappingIterable#wrap(java.util.Iterator)
			 */
			@Override
			protected Iterator<Iterable<EvaluationExpression>> wrap(Iterator<Operator> iterator) {
				return new ConversionIterator<Operator, Iterable<EvaluationExpression>>() {
					/* (non-Javadoc)
					 * @see eu.stratosphere.util.ConversionIterator#convert(java.lang.Object)
					 */
					@Override
					protected Iterable<EvaluationExpression> convert(Operator op) {
						return op.getKeyExpression();
					}
				};
			}
		});
	}

	/**
	 * Returns a {@link SopremoModule} that consists entirely of {@link ElementaryOperator}s. The module can be seen as
	 * an expanded version of this operator where all CompositeOperators are recursively translated to
	 * ElementaryOperators.
	 * 
	 * @return a module of ElementaryOperators
	 */
	public abstract SopremoModule asElementaryOperators();

	// @Override
	// public SopremoModule toElementaryOperators() {
	// SopremoModule elementary = asElementaryOperators().asElementary();
	// System.out.println(getName() + " -> "+ elementary);
	// return elementary;
	// }

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		if (LOG.isTraceEnabled())
			LOG.trace("Transforming\n" + this);
		final SopremoModule elementaryPlan = this.asElementaryOperators();
		if (LOG.isTraceEnabled())
			LOG.trace(" to elementary plan\n" + elementaryPlan);
		context.pushOperator(this);
		final PactModule pactModule = elementaryPlan.asPactModule(context);
		context.popOperator();
		return pactModule;
	}
}
