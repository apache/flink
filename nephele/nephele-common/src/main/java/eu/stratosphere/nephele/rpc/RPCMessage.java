package eu.stratosphere.nephele.rpc;

abstract class RPCMessage {

	public static final int MAXIMUM_MSG_SIZE = 1020;

	public static final int METADATA_SIZE = 4;
	
	private final int requestID;

	protected RPCMessage(final int requestID) {
		this.requestID = requestID;
	}

	int getRequestID() {
		return this.requestID;
	}
}
