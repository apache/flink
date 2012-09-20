package eu.stratosphere.nephele.rpc;

abstract class RPCResponse extends RPCMessage {

	protected RPCResponse(final int requestID) {
		super(requestID);
	}
}
