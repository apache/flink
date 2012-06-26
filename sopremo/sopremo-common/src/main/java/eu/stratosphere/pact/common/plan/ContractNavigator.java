package eu.stratosphere.pact.common.plan;

import java.util.List;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.util.dag.ConnectionNavigator;

/**
 * {@link Navigator} for traversing a graph of {@link Contract}s.
 * 
 * @author Arvid Heise
 * @see Navigator
 */
public class ContractNavigator implements ConnectionNavigator<Contract> {
	/**
	 * The default stateless instance that should be used in most cases.
	 */
	public static final ContractNavigator INSTANCE = new ContractNavigator();

	@Override
	public List<Contract> getConnectedNodes(final Contract node) {
		return ContractUtil.getFlatInputs(node);
	}

}
