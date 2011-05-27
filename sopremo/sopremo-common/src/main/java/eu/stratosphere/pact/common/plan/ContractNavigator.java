package eu.stratosphere.pact.common.plan;

import java.util.Arrays;

import eu.stratosphere.dag.Navigator;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;

/**
 * {@link Navigator} for traversing a graph of {@link Contract}s.
 * 
 * @author Arvid Heise
 * @see Navigator
 */
public class ContractNavigator implements Navigator<Contract> {
	/**
	 * The default stateless instance that should be used in most cases.
	 */
	public static final ContractNavigator INSTANCE = new ContractNavigator();

	@SuppressWarnings("rawtypes")
	@Override
	public Iterable<Contract> getConnectedNodes(Contract node) {
		if (node instanceof DualInputContract)
			return Arrays.asList(((DualInputContract) node).getFirstInput(),
				((DualInputContract) node).getSecondInput());
		if (node instanceof SingleInputContract)
			return Arrays.asList(((SingleInputContract) node).getInput());
		if (node instanceof DataSinkContract)
			return Arrays.asList(((DataSinkContract) node).getInput());
		return Arrays.asList();
	}

}
