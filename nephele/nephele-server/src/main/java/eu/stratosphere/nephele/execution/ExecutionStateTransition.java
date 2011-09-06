package eu.stratosphere.nephele.execution;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is a utility class to check the consistency of Nephele's execution state model.
 * 
 * @author warneke
 */
public final class ExecutionStateTransition {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ExecutionStateTransition.class);

	/**
	 * Private constructor to prevent instantiation of object.
	 */
	private ExecutionStateTransition() {
	}

	/**
	 * Checks the transition of the execution state and outputs an error in case of an unexpected state transition.
	 * 
	 * @param taskName
	 *        the name of the task whose execution has changed
	 * @param oldState
	 *        the old execution state
	 * @param newState
	 *        the new execution state
	 */
	public static void checkTransition(final String taskName, final ExecutionState oldState,
			final ExecutionState newState) {

		// Ignore state changes in final states
		if (oldState == ExecutionState.CANCELED || oldState == ExecutionState.FINISHED
				|| oldState == ExecutionState.FAILED) {
			return;
		}

		LOG.info("ExecutionState set from " + oldState + " to " + newState + " for task " + taskName);

		boolean unexpectedStateChange = true;

		// This is the regular life cycle of a task
		if (oldState == ExecutionState.CREATED && newState == ExecutionState.SCHEDULED) {
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.SCHEDULED && newState == ExecutionState.ASSIGNED) {
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.ASSIGNED && newState == ExecutionState.READY) {
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.READY && newState == ExecutionState.STARTING) {
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.STARTING && newState == ExecutionState.RUNNING) {
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.RUNNING && newState == ExecutionState.FINISHING) {
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.FINISHING && newState == ExecutionState.FINISHED) {
			unexpectedStateChange = false;
		}

		if (oldState == ExecutionState.SCHEDULED && newState == ExecutionState.CANCELED) {
			/**
			 * This transition can appear if a task in a stage which is not yet executed gets canceled.
			 */
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.ASSIGNED && newState == ExecutionState.CANCELED) {
			/**
			 * This transition can appear if a task in a stage which is not yet executed gets canceled.
			 */
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.READY && newState == ExecutionState.CANCELED) {
			/**
			 * This transition can appear if a task is canceled that is not yet running on the task manager.
			 */
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.STARTING && newState == ExecutionState.FAILED) {
			/**
			 * This transition can appear if a task cannot be deployed at the assigned task manager.
			 */
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.RUNNING && newState == ExecutionState.FAILED) {
			/**
			 * This is a regular transition in case of a task error.
			 */
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.FINISHING && newState == ExecutionState.FAILED) {
			/**
			 * This is a regular transition in case of a task error.
			 */
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.RUNNING && newState == ExecutionState.CANCELING) {
			/**
			 * This is a regular transition as a result of a cancel operation.
			 */
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.FINISHING && newState == ExecutionState.CANCELING) {
			/**
			 * This is a regular transition as a result of a cancel operation.
			 */
			unexpectedStateChange = false;
		}
		if (oldState == ExecutionState.CANCELING && newState == ExecutionState.CANCELED) {
			/**
			 * This is a regular transition as a result of a cancel operation.
			 */
			unexpectedStateChange = false;
		}

		if (unexpectedStateChange) {
			LOG.error("Unexpected state change: " + oldState + " -> " + newState);
		}
	}
}
