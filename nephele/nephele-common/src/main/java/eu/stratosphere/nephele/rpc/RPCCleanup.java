package eu.stratosphere.nephele.rpc;

final class RPCCleanup extends RPCMessage {

	RPCCleanup(final int requestID) {
		super(requestID);
	}

	RPCCleanup() {
		super(0);
	}
}
