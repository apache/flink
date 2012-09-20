package eu.stratosphere.nephele.rpc;

import java.lang.reflect.Method;

final class RPCRequest extends RPCMessage {

	private final String interfaceName;

	private final String methodName;

	private final Class<?>[] parameterTypes;

	private final Object[] args;

	RPCRequest(final int requestID, final String interfaceName, final Method method, final Object[] args) {
		super(requestID);

		this.interfaceName = interfaceName;
		this.methodName = method.getName();
		this.parameterTypes = method.getParameterTypes();
		this.args = args;
	}

	RPCRequest() {
		super(0);

		this.interfaceName = null;
		this.methodName = null;
		this.parameterTypes = null;
		this.args = null;
	}

	String getInterfaceName() {

		return this.interfaceName;
	}

	String getMethodName() {

		return this.methodName;
	}

	Class<?>[] getParameterTypes() {

		return this.parameterTypes;
	}

	Object[] getArgs() {

		return this.args;
	}
}
