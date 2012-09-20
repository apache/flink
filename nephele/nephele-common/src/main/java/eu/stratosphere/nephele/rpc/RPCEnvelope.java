package eu.stratosphere.nephele.rpc;

class RPCEnvelope {

	private final RPCMessage rpcMessage;

	RPCEnvelope(final RPCMessage rpcMessage) {
		this.rpcMessage = rpcMessage;
	}

	RPCEnvelope() {
		this.rpcMessage = null;
	}

	RPCMessage getRPCMessage() {
		return this.rpcMessage;
	}
}
