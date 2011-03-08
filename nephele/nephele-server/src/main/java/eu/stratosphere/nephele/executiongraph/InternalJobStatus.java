package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.jobgraph.JobStatus;

/**
 * This enumeration contains all states a job represented by an {@link ExecutionGraph} can have during its lifetime. It
 * contains all states from {@link JobStatus} but also internal states to keep track of shutdown processes.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public enum InternalJobStatus {

	/**
	 * All tasks of the job are in the execution state CREATED.
	 */
	CREATED,

	/**
	 * All tasks of the job are in the execution state
	 */
	SCHEDULED,

	/**
	 * At least one task of the job is running, none has definitely failed.
	 */
	RUNNING,

	/**
	 * At least one task of the job has definitely failed and cannot be recovered. The job is in the process of being
	 * terminated.
	 */
	FAILING,

	/**
	 * At least one task of the job has definitively failed and cannot
	 * be recovered anymore. As a result, the job has been terminated.
	 */
	FAILED,

	/**
	 * At least one task has been canceled as a result of a user request. The job is in the process of being canceled
	 * completely.
	 */
	CANCELING,

	/**
	 * All tasks of the job are canceled as a result of a user request. The job has been terminated.
	 */
	CANCELED,

	/**
	 * All of the job's tasks have successfully finished.
	 */
	FINISHED;

	/**
	 * Converts an internal job status in a {@link JobStatus} state.
	 * 
	 * @param status
	 *        the internal job status to converted.
	 * @return the corresponding job status or <code>null</code> if no corresponding job status exists
	 */
	public static JobStatus toJobStatus(InternalJobStatus status) {

		switch (status) {

		case CREATED:
			return JobStatus.CREATED;
		case SCHEDULED:
			return JobStatus.SCHEDULED;
		case RUNNING:
			return JobStatus.RUNNING;
		case FAILED:
			return JobStatus.FAILED;
		case CANCELED:
			return JobStatus.CANCELED;
		case FINISHED:
			return JobStatus.FINISHED;
		}

		return null;
	}
}
