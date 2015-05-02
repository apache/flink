/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskmanager;

import java.util.Arrays;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

/**
 * This class represents an update about a task's execution state.
 *
 * <b>NOTE:</b> The exception that may be attached to the state update is
 * not necessarily a Flink or core Java exception, but may be an exception
 * from the user code. As such, it cannot be deserialized without a
 * special class loader. For that reason, the class keeps the actual
 * exception field transient and deserialized it lazily, with the
 * appropriate class loader.
 */
public class TaskExecutionState implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private final JobID jobID;

	private final ExecutionAttemptID executionId;

	private final ExecutionState executionState;

	private final byte[] serializedError;

	// The exception must not be (de)serialized with the class, as its
	// class may not be part of the system class loader.
	private transient Throwable cachedError;

	/**
	 * Creates a new task execution state update, with no attached exception.
	 *
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param executionId
	 *        the ID of the task execution whose state is to be reported
	 * @param executionState
	 *        the execution state to be reported
	 */
	public TaskExecutionState(JobID jobID, ExecutionAttemptID executionId, ExecutionState executionState) {
		this(jobID, executionId, executionState, null);
	}
	
	/**
	 * Creates a new task execution state update, with an attached exception.
	 * This constructor may never throw an exception.
	 * 
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param executionId
	 *        the ID of the task execution whose state is to be reported
	 * @param executionState
	 *        the execution state to be reported
	 * @param error
	 *        an optional error
	 */
	public TaskExecutionState(JobID jobID, ExecutionAttemptID executionId,
								ExecutionState executionState, Throwable error) {

		if (jobID == null || executionId == null || executionState == null) {
			throw new NullPointerException();
		}

		this.jobID = jobID;
		this.executionId = executionId;
		this.executionState = executionState;
		this.cachedError = error;

		if (error != null) {
			byte[] serializedError;
			try {
				serializedError = InstantiationUtil.serializeObject(error);
			}
			catch (Throwable t) {
				// could not serialize exception. send the stringified version instead
				try {
					this.cachedError = new Exception(ExceptionUtils.stringifyException(error));
					serializedError = InstantiationUtil.serializeObject(this.cachedError);
				}
				catch (Throwable tt) {
					// seems like we cannot do much to report the actual exception
					// report a placeholder instead
					try {
						this.cachedError = new Exception("Cause is a '" + error.getClass().getName()
								+ "' (failed to serialize or stringify)");
						serializedError = InstantiationUtil.serializeObject(this.cachedError);
					}
					catch (Throwable ttt) {
						// this should never happen unless the JVM is fubar.
						// we just report the state without the error
						this.cachedError = null;
						serializedError = null;
					}
				}
			}
			this.serializedError = serializedError;
		}
		else {
			this.serializedError = null;
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the attached exception. Requires to pass a classloader, because the
	 * class of the exception may be user-defined and hence only accessible through
	 * the user code classloader, not the default classloader.
	 *
	 * @param usercodeClassloader The class loader for the user code of the
	 *                            job this update refers to.
	 */
	public Throwable getError(ClassLoader usercodeClassloader) {
		if (this.serializedError == null) {
			return null;
		}

		if (this.cachedError == null) {
			try {
				cachedError = (Throwable) InstantiationUtil.deserializeObject(this.serializedError, usercodeClassloader);
			}
			catch (Exception e) {
				throw new RuntimeException("Error while deserializing the attached exception", e);
			}
		}
		return this.cachedError;
	}

	/**
	 * Returns the ID of the task this result belongs to
	 * 
	 * @return the ID of the task this result belongs to
	 */
	public ExecutionAttemptID getID() {
		return this.executionId;
	}

	/**
	 * Returns the new execution state of the task.
	 * 
	 * @return the new execution state of the task
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
	}

	/**
	 * The ID of the job the task belongs to
	 * 
	 * @return the ID of the job the task belongs to
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TaskExecutionState) {
			TaskExecutionState other = (TaskExecutionState) obj;
			return other.jobID.equals(this.jobID) &&
					other.executionId.equals(this.executionId) &&
					other.executionState == this.executionState &&
					(other.serializedError == null ? this.serializedError == null :
						(this.serializedError != null && Arrays.equals(this.serializedError, other.serializedError)));
		}
		else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return jobID.hashCode() + executionId.hashCode() + executionState.ordinal();
	}
	
	@Override
	public String toString() {
		return String.format("TaskState jobId=%s, executionId=%s, state=%s, error=%s", 
				jobID, executionId, executionState,
				cachedError == null ? (serializedError == null ? "(null)" : "(serialized)")
									: (cachedError.getClass().getName() + ": " + cachedError.getMessage()));
	}
}
