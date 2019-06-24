package org.apache.flink.state.api.runtime.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.types.Either;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Savepoint metadata that can be modified.
 */
@Internal
public class ModifiableSavepointMetadata extends SavepointMetadata {

	private transient Map<OperatorID, Either<OperatorState, BootstrapTransformation<?>>> operatorStateIndex;

	public ModifiableSavepointMetadata(int maxParallelism, Collection<MasterState> masterStates, Collection<OperatorState> initialStates) {
		super(maxParallelism, masterStates);

		this.operatorStateIndex = new HashMap<>(initialStates.size());

		for (OperatorState operatorState : initialStates) {
			operatorStateIndex.put(operatorState.getOperatorID(), Either.Left(operatorState));
		}
	}

	/**
	 * @return Operator state for the given UID.
	 *
	 * @throws IOException If the savepoint does not contain operator state with the given uid.
	 */
	public OperatorState getOperatorState(String uid) throws IOException {
		OperatorID operatorID = OperatorIDGenerator.fromUid(uid);

		Either<OperatorState, BootstrapTransformation<?>> operatorState = operatorStateIndex.get(operatorID);
		if (operatorState == null || operatorState.isRight()) {
			throw new IOException("Savepoint does not contain state with operator uid " + uid);
		}

		return operatorState.left();
	}

	public void removeOperator(String uid) {
		operatorStateIndex.remove(OperatorIDGenerator.fromUid(uid));
	}

	public void addOperator(String uid, BootstrapTransformation<?> transformation) {
		OperatorID id = OperatorIDGenerator.fromUid(uid);

		if (operatorStateIndex.containsKey(id)) {
			throw new IllegalArgumentException("The savepoint already contains uid " + uid + ". All uid's must be unique");
		}

		operatorStateIndex.put(id, Either.Right(transformation));
	}

	public Map<OperatorID, Either<OperatorState, BootstrapTransformation<?>>> getOperatorStates() {
		return operatorStateIndex;
	}
}
