package eu.stratosphere.nephele.rpc;

final class RPCThrowable extends RPCResponse {

	private final Throwable throwable;

	RPCThrowable(final int requestID, final Throwable throwable) {
		super(requestID);

		this.throwable = throwable;
	}

	RPCThrowable() {
		super(0);

		this.throwable = null;
	}

	Throwable getThrowable() {
		return this.throwable;
	}
}
