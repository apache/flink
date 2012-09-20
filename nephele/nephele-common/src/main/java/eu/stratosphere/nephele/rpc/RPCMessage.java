package eu.stratosphere.nephele.rpc;

abstract class RPCMessage {

	public static final int MAXIMUM_MSG_SIZE = 2048;

	private final int requestID;

	protected RPCMessage(final int requestID) {
		this.requestID = requestID;
	}

	int getRequestID() {
		return this.requestID;
	}
}
