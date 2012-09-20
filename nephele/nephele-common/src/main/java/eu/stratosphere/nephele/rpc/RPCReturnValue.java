package eu.stratosphere.nephele.rpc;

final class RPCReturnValue extends RPCResponse {

	private final Object retVal;

	RPCReturnValue(final int requestID, final Object retVal) {
		super(requestID);

		this.retVal = retVal;
	}

	RPCReturnValue() {
		super(0);

		this.retVal = null;
	}

	Object getRetVal() {
		return this.retVal;
	}
}
