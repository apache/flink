package eu.stratosphere.pact.common.plan;

import java.util.Arrays;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.util.dag.Navigator;

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

	@Override
	public Iterable<Contract> getConnectedNodes(final Contract node) {
		return Arrays.asList(ContractUtil.getInputs(node));
	}

}
