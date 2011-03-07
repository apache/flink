package eu.stratosphere.pact.common.contract;

import eu.stratosphere.pact.common.stub.SingleInputStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

public class RangePartitionContract<IK extends Key, IV extends Value, OK extends Key, OV extends Value>
	extends SingleInputContract<IK, IV, OK, OV> {

	public RangePartitionContract(Class<? extends SingleInputStub<IK, IV, OK, OV>> c, String n) {
		super(c, n);
	}

}
