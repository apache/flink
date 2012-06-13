package eu.stratosphere.sopremo;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.IdentityMap;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.ContractUtil;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.util.CollectionUtil;
import eu.stratosphere.util.IdentityList;
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
 * 				Iterator&lt;PactJsonObject&gt; values2,
 * 				Collector&lt;PactJsonObject.Key, PactJsonObject&gt; out) {
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
@OutputCardinality(min = 1, max = 1)
public abstract class ElementaryOperator<Self extends ElementaryOperator<Self>> extends Operator<Self> {
	private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(ElementaryOperator.class);

	private List<List<? extends EvaluationExpression>> keyExpressions =
		new ArrayList<List<? extends EvaluationExpression>>();

	/**
	 * 
	 */
	private static final long serialVersionUID = 4504792171699882490L;

	/**
	 * Initializes the ElementaryOperator with the number of outputs set to 1. The {@link InputCardinality} annotation
	 * must be set with this constructor.
	 */
	public ElementaryOperator() {
		super();
	}

	/**
	 * Initializes the ElementaryOperator with the given number of inputs.
	 * 
	 * @param minInputs
	 *        the minimum number of inputs
	 * @param maxInputs
	 *        the maximum number of inputs
	 */
	public ElementaryOperator(int minInputs, int maxInputs) {
		super(minInputs, maxInputs, 1, 1);
	}

	/**
	 * Initializes the ElementaryOperator with the given number of inputs.
	 * 
	 * @param inputs
	 *        the number of inputs
	 */
	public ElementaryOperator(int inputs) {
		this(inputs, inputs);
	}

	{
		for (int index = 0; index < getMinInputs(); index++)
			this.keyExpressions.add(new ArrayList<EvaluationExpression>());
	}

	/**
	 * Returns the key expressions of the given input.
	 * 
	 * @param inputIndex
	 *        the index of the input
	 * @return the key expressions of the given input
	 */
	@SuppressWarnings("unchecked")
	public List<? extends EvaluationExpression> getKeyExpressions(int inputIndex) {
		if (inputIndex >= this.keyExpressions.size())
			return Collections.EMPTY_LIST;
		final List<? extends EvaluationExpression> expressions = this.keyExpressions.get(inputIndex);
		if (expressions == null)
			return Collections.EMPTY_LIST;
		return expressions;
	}

	/**
	 * Sets the keyExpressions to the specified value.
	 * 
	 * @param keyExpressions
	 *        the keyExpressions to set
	 * @param inputIndex
	 *        the index of the input
	 */
	@Property(hidden = true)
	public void setKeyExpressions(int inputIndex, List<? extends EvaluationExpression> keyExpressions) {
		if (keyExpressions == null)
			throw new NullPointerException("keyExpressions must not be null");
		CollectionUtil.ensureSize(this.keyExpressions, inputIndex + 1);
		this.keyExpressions.set(inputIndex, keyExpressions);
	}

	/**
	 * Sets the keyExpressions of the given input to the specified value.
	 * 
	 * @param keyExpressions
	 *        the keyExpressions to set
	 */
	public void setKeyExpressions(int index, EvaluationExpression... keyExpressions) {
		if (keyExpressions.length == 0)
			throw new IllegalArgumentException("keyExpressions must not be null");

		setKeyExpressions(index, Arrays.asList(keyExpressions));
	}

	/**
	 * Sets the keyExpressions of the given input to the specified value.
	 * 
	 * @param keyExpressions
	 *        the keyExpressions to set
	 * @param inputIndex
	 *        the index of the input
	 * @return this
	 */
	public Self withKeyExpressions(int index, EvaluationExpression... keyExpressions) {
		setKeyExpressions(index, keyExpressions);
		return self();
	}

	/**
	 * Sets the keyExpressions of the given input to the specified value.
	 * 
	 * @param keyExpressions
	 *        the keyExpressions to set
	 * @param inputIndex
	 *        the index of the input
	 * @return this
	 */
	public Self withKeyExpressions(int index, List<? extends EvaluationExpression> keyExpressions) {
		setKeyExpressions(index, keyExpressions);
		return self();
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		final Contract contract = this.getContract(context.getInputSchema(0));
		this.configureContract(contract, contract.getParameters(), context);

		final List<List<Contract>> inputLists = ContractUtil.getInputs(contract);
		final List<Contract> distinctInputs = new IdentityList<Contract>();
		for (final List<Contract> inputs : inputLists) {
			// assume at least one input for each contract input slot
			if (inputs.isEmpty())
				inputs.add(new MapContract(IdentityMap.class));
			for (final Contract input : inputs)
				if (!distinctInputs.contains(input))
					distinctInputs.add(input);
		}
		final PactModule module = new PactModule(this.toString(), distinctInputs.size(), 1);
		for (final List<Contract> inputs : inputLists)
			for (int index = 0; index < inputs.size(); index++)
				inputs.set(index, module.getInput(distinctInputs.indexOf(inputs.get(index))));
		ContractUtil.setInputs(contract, inputLists);

		module.getOutput(0).addInput(contract);
		return module;
	}

	/**
	 * Creates a module that delegates all input directly to the output.
	 * 
	 * @return a short circuit module
	 */
	protected PactModule createShortCircuitModule() {
		final PactModule module = new PactModule("Short circuit", 1, 1);
		module.getOutput(0).setInput(module.getInput(0));
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
	protected void configureContract(final Contract contract, final Configuration stubConfiguration,
			final EvaluationContext context) {
		context.pushOperator(this);
		SopremoUtil.serialize(stubConfiguration, SopremoUtil.CONTEXT, context);
		context.popOperator();

		for (final Field stubField : contract.getUserCodeClass().getDeclaredFields())
			if ((stubField.getModifiers() & (Modifier.TRANSIENT | Modifier.FINAL | Modifier.STATIC)) == 0) {
				Class<?> clazz = this.getClass();
				do {
					Field thisField;
					try {
						thisField = clazz.getDeclaredField(stubField.getName());
						thisField.setAccessible(true);
						SopremoUtil.serialize(stubConfiguration, stubField.getName(),
							(Serializable) thisField.get(this));
					} catch (final NoSuchFieldException e) {
						// ignore field of stub if the field does not exist in this operator
					} catch (final Exception e) {
						LOG.error(String.format("Could not serialize field %s of class %s: %s", stubField.getName(),
							contract.getClass(), e));
					}
				} while ((clazz = clazz.getSuperclass()) != ElementaryOperator.class);
			}
	}

	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final ElementarySopremoModule module =
			new ElementarySopremoModule(this.getName(), this.getInputs().size(), this.getOutputs()
				.size());
		final Operator<Self> clone = this.clone();
		for (int index = 0; index < this.getInputs().size(); index++)
			clone.setInput(index, module.getInput(index));
		final List<JsonStream> outputs = clone.getOutputs();
		for (int index = 0; index < outputs.size(); index++)
			module.getOutput(index).setInput(index, outputs.get(index));
		return module;
	}

	/**
	 * Creates the {@link Contract} that represents this operator.
	 * 
	 * @return the contract representing this operator
	 */
	@SuppressWarnings("unchecked")
	protected Contract getContract(final Schema globalSchema) {
		final Class<? extends Stub> stubClass = this.getStubClass();
		if (stubClass == null)
			throw new IllegalStateException("no implementing stub found");
		final Class<? extends Contract> contractClass = ContractUtil.getContractClass(stubClass);
		if (contractClass == null)
			throw new IllegalStateException("no associated contract found");

		try {
			if (contractClass == ReduceContract.class) {
				int[] keyIndices = getKeyIndices(globalSchema, this.getKeyExpressions(0));
				return new ReduceContract((Class<? extends ReduceStub>) stubClass,
					getKeyClasses(globalSchema, keyIndices), keyIndices, this.toString());
			}
			else if (contractClass == CoGroupContract.class) {
				int[] keyIndices1 = getKeyIndices(globalSchema, getKeyExpressions(0));
				int[] keyIndices2 = getKeyIndices(globalSchema, getKeyExpressions(1));
				return new CoGroupContract((Class<? extends CoGroupStub>) stubClass,
					getCommonKeyClasses(globalSchema, keyIndices1, keyIndices2),
					keyIndices1, keyIndices2, this.toString());
			}
			else if (contractClass == MatchContract.class) {
				int[] keyIndices1 = getKeyIndices(globalSchema, getKeyExpressions(0));
				int[] keyIndices2 = getKeyIndices(globalSchema, getKeyExpressions(1));
				return new MatchContract((Class<? extends MatchStub>) stubClass,
					getCommonKeyClasses(globalSchema, keyIndices1, keyIndices2),
					keyIndices1, keyIndices2, this.toString());
			}
			return ReflectUtil.newInstance(contractClass, stubClass, this.toString());
		} catch (final Exception e) {
			throw new IllegalStateException("Cannot create contract from stub " + stubClass, e);
		}
	}

	private Class<? extends Key>[] getCommonKeyClasses(Schema globalSchema, int[] keyIndices1, int[] keyIndices2) {
		Class<? extends Key>[] keyClasses1 = getKeyClasses(globalSchema, keyIndices1);
		Class<? extends Key>[] keyClasses2 = getKeyClasses(globalSchema, keyIndices2);
		if (!Arrays.equals(keyClasses1, keyClasses2))
			throw new IllegalStateException(String.format(
				"The key classes are not compatible (schema %s; indices %s %s; key classes: %s %s)",
				globalSchema, keyIndices1, keyIndices2, keyClasses1, keyClasses2));
		return keyClasses1;
	}

	// protected Iterable<? extends EvaluationExpression> getKeyExpressionsForInput(final int index) {
	// final Iterable<? extends EvaluationExpression> keyExpressions = this.getKeyExpressions();
	// if (keyExpressions == ALL_KEYS)
	// return keyExpressions;
	// return new FilteringIterable<EvaluationExpression>(keyExpressions, new Predicate<EvaluationExpression>() {
	// @Override
	// public boolean isTrue(EvaluationExpression expression) {
	// return SopremoUtil.getInputIndex(expression) == index;
	// };
	// });
	// }
	//
	public List<List<? extends EvaluationExpression>> getAllKeyExpressions() {
		final ArrayList<List<? extends EvaluationExpression>> allKeys =
			new ArrayList<List<? extends EvaluationExpression>>();
		final List<JsonStream> inputs = getInputs();
		for (int index = 0; index < inputs.size(); index++)
			allKeys.add(getKeyExpressions(index));
		return allKeys;
	}

	@SuppressWarnings("unchecked")
	private Class<? extends Key>[] getKeyClasses(final Schema globalSchema, int[] keyIndices) {
		Class<? extends Value>[] pactSchema = globalSchema.getPactSchema();
		Class<? extends Key>[] keyClasses = new Class[keyIndices.length];
		for (int index = 0; index < keyIndices.length; index++) {
			Class<? extends Value> schemaClass = pactSchema[keyIndices[index]];
			if (!(Key.class.isAssignableFrom(schemaClass)))
				throw new IllegalStateException("Schema wrapped a key value with a class that does not implement Key");
			keyClasses[index] = (Class<? extends Key>) schemaClass;
		}
		return keyClasses;
	}

	private int[] getKeyIndices(final Schema globalSchema, Iterable<? extends EvaluationExpression> keyExpressions) {
		if (keyExpressions == ALL_KEYS) {
			final int[] allSchema = new int[globalSchema.getPactSchema().length];
			for (int index = 0; index < allSchema.length; index++)
				allSchema[index] = index;
			return allSchema;
		}
		IntList keyIndices = new IntArrayList();
		for (EvaluationExpression expression : keyExpressions)
			for (int index : globalSchema.indicesOf(expression))
				keyIndices.add(index);
		if (keyIndices.isEmpty())
			throw new IllegalStateException("The given operator needs to specify key expressions");
		return keyIndices.toIntArray();
	}

	// protected abstract Schema getKeyFields();

	/**
	 * Returns the stub class that represents the functionality of this operator.<br>
	 * This method returns the first static inner class found with {@link Class#getDeclaredClasses()} that is
	 * extended from {@link Stub} by default.
	 * 
	 * @return the stub class
	 */
	@SuppressWarnings("unchecked")
	protected Class<? extends Stub> getStubClass() {
		for (final Class<?> stubClass : this.getClass().getDeclaredClasses())
			if ((stubClass.getModifiers() & Modifier.STATIC) != 0 && Stub.class.isAssignableFrom(stubClass))
				return (Class<? extends Stub>) stubClass;
		return null;
	}
}
