package eu.stratosphere.sopremo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.plan.ContractUtil;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class ElementaryOperator extends Operator {
	private final static Map<Class<?>, Class<?>> STUB_CONTRACTS = new HashMap<Class<?>, Class<?>>();

	public ElementaryOperator(JsonStream... inputs) {
		super(inputs);
	}

	public ElementaryOperator(List<? extends JsonStream> inputs) {
		super(inputs);
	}

	static {
		STUB_CONTRACTS.put(MapStub.class, MapContract.class);
		STUB_CONTRACTS.put(ReduceStub.class, ReduceContract.class);
		STUB_CONTRACTS.put(CoGroupStub.class, CoGroupContract.class);
		STUB_CONTRACTS.put(CrossStub.class, CrossContract.class);
		STUB_CONTRACTS.put(MatchStub.class, MatchContract.class);
		STUB_CONTRACTS.put(InputFormat.class, DataSourceContract.class);
		STUB_CONTRACTS.put(OutputFormat.class, DataSinkContract.class);
	}

	public PactModule asPactModule(EvaluationContext context) {
		Contract contract = getContract();
		configureContract(contract.getStubParameters(), context);
		Contract[] inputs = ContractUtil.getInputs(contract);
		PactModule module = new PactModule(toString(), inputs.length, 1);
		ContractUtil.setInputs(contract, module.getInputs());
		module.getOutput(0).setInput(contract);
		return module;
	}

	protected void configureContract(Configuration configuration, EvaluationContext context) {
		SopremoUtil.setContext(configuration, context);
	}

	protected Contract getContract() {
		Class<? extends Stub<?, ?>> stubClass = getStubClass();
		if (stubClass == null)
			throw new IllegalStateException("no implementing stub found");
		Class<?> contractClass = getContractClass(stubClass);
		if (contractClass == null)
			throw new IllegalStateException("no associated contract found");
		try {
			return (Contract) contractClass.getDeclaredConstructor(Class.class, String.class)
				.newInstance(stubClass, toString());
		} catch (Exception e) {
			throw new IllegalStateException("Cannot create contract from stub " + stubClass, e);
		}
	}

	protected Class<?> getContractClass(Class<?> stubClass) {
		Class<?> contract = STUB_CONTRACTS.get(stubClass);
		if (contract == null && stubClass.getSuperclass() != null)
			return getContractClass(stubClass.getSuperclass());
		return contract;
	}

	@SuppressWarnings("unchecked")
	protected Class<? extends Stub<?, ?>> getStubClass() {
		for (Class<?> contractClass : getClass().getDeclaredClasses()) {
			if ((contractClass.getModifiers() & Modifier.STATIC) != 0 && Stub.class.isAssignableFrom(contractClass))
				return (Class<? extends Stub<?, ?>>) contractClass;
		}
		return null;
	}
}
