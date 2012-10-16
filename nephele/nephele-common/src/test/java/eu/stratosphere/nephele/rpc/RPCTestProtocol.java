package eu.stratosphere.nephele.rpc;

import java.util.List;

public interface RPCTestProtocol extends RPCProtocol {

	int testMethod(boolean par1, int par2, List<String> par3);
}
