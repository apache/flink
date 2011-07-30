package eu.stratosphere.sopremo;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.plan.ContractUtil;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * An ElementaryOperator is an {@link Operator} that directly translates to a PACT. Such an operator has at most one
 * output.<br>
 * By convention, the first inner class of implementing operators that inherits from {@link Stub} is assumed to be
 * the implementation of this operator. The following example demonstrates a minimalistic operator implementation.
 * 
 * <pre>
 * public static class TwoInputIntersection extends ElementaryOperator {
 * 	public TwoInputIntersection(JsonStream input1, JsonStream input2) {
 * 		super(input1, input2);
 * 	}
 * 
 * 	public static class Implementation extends
 * 			SopremoCoGroup&lt;PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject&gt; {
 * 		&#064;Override
 * 		public void coGroup(PactJsonObject.Key key, Iterator&lt;PactJsonObject&gt; values1,
 * 					Iterator&lt;PactJsonObject&gt; values2,
 * 					Collector&lt;PactJsonObject.Key, PactJsonObject&gt; out) {
 * 			if (values1.hasNext() &amp;&amp; values2.hasNext())
 * 				out.collect(key, values1.next());
 * 		}
 * 	}
 * }
 * </pre>
 * 
 * To exert more control, several hooks are available that are called in fixed order.
 * <ul>
 * <li>{@link #getStubClass()} allows to choose a different Stub than the first inner class inheriting from {@link Stub}.
 * <li>{@link #getContract()} instantiates a contract matching the stub class resulting from the previous callback. This
 * callback is especially useful if a PACT stub is chosen that is not supported in Sopremo yet.
 * <li>{@link #configureContract(Contract, Configuration, EvaluationContext)} is a callback used to set parameters of
 * the {@link Configuration} of the stub.
 * <li>{@link #asPactModule(EvaluationContext)} gives complete control over the creation of the {@link PactModule}.
 * </ul>
 * 
 * @author Arvid Heise
 */
public class ElementaryOperator extends Operator {
	private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(ElementaryOperator.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 4504792171699882490L;

	/**
	 * Initializes the ElementaryOperator with the given input {@link JsonStream}s. A JsonStream is
	 * either the output of another operator or the operator itself.
	 * 
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	public ElementaryOperator(JsonStream... inputs) {
		super(inputs);
	}

	/**
	 * Initializes the ElementaryOperator with the given input {@link JsonStream}s. A JsonStream is
	 * either the output of another operator or the operator itself.
	 * 
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	public ElementaryOperator(List<? extends JsonStream> inputs) {
		super(inputs);
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		Contract contract = this.getContract();
		this.configureContract(contract, contract.getStubParameters(), context);
		Contract[] inputs = ContractUtil.getInputs(contract);
		PactModule module = new PactModule(this.toString(), inputs.length, 1);
		ContractUtil.setInputs(contract, module.getInputs());
		module.getOutput(0).setInput(contract);
		return module;
	}

	/**
	 * Callback to add parameters to the stub configuration.<br>
	 * The default implementation adds the context and all non-transient, non-final, non-static fields.
	 * 
	 * @param contract
	 *        the contract to configure
	 * @param stubConfiguration
	 *        the configuration of the stub
	 * @param context
	 *        the context in which the {@link PactModule} is created and evaluated
	 */
	protected void configureContract(Contract contract, Configuration stubConfiguration, EvaluationContext context) {
		SopremoUtil.setContext(stubConfiguration, context);

		for (Field stubField : contract.getStubClass().getDeclaredFields())
			if ((stubField.getModifiers() & (Modifier.TRANSIENT | Modifier.FINAL | Modifier.STATIC)) == 0) {
				Field thisField;
				try {
					thisField = this.getClass().getDeclaredField(stubField.getName());
					thisField.setAccessible(true);
					SopremoUtil.serialize(stubConfiguration, stubField.getName(), thisField.get(this));
				} catch (NoSuchFieldException e) {
					// ignore field of stub if the field does not exist in this operator
				} catch (Exception e) {
					LOG.error(String.format("Could not serialize field %s of class %s: %s", stubField.getName(),
						contract.getClass(), e));
				}
			}
	}

	/**
	 * Creates the {@link Contract} that represents this operator.
	 * 
	 * @return the contract representing this operator
	 */
	protected Contract getContract() {
		Class<? extends Stub<?, ?>> stubClass = this.getStubClass();
		if (stubClass == null)
			throw new IllegalStateException("no implementing stub found");
		Class<? extends Contract> contractClass = ContractUtil.getContractClass(stubClass);
		if (contractClass == null)
			throw new IllegalStateException("no associated contract found");
		try {
			return ReflectUtil.newInstance(contractClass, stubClass, this.toString());
		} catch (Exception e) {
			throw new IllegalStateException("Cannot create contract from stub " + stubClass, e);
		}
	}

	/**
	 * Returns the stub class that represents the functionality of this operator.<br>
	 * This method returns the first static inner class found with {@link Class#getDeclaredClasses()} that is
	 * extended from {@link Stub} by default.
	 * 
	 * @return the stub class
	 */
	@SuppressWarnings("unchecked")
	protected Class<? extends Stub<?, ?>> getStubClass() {
		for (Class<?> stubClass : this.getClass().getDeclaredClasses())
			if ((stubClass.getModifiers() & Modifier.STATIC) != 0 && Stub.class.isAssignableFrom(stubClass))
				return (Class<? extends Stub<?, ?>>) stubClass;
		return null;
	}
}
