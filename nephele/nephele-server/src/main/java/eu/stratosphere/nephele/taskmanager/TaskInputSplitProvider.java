package eu.stratosphere.nephele.taskmanager;

import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.protocols.InputSplitProviderProtocol;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The task input split provider is a component of the task manager which implements the {@link InputSplitProvider}
 * interface. It is called by a task in order to acquire a new input split to consume. The task input split provider in
 * return will call the global input split provider to retrieve a new input split.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class TaskInputSplitProvider implements InputSplitProvider {

	private final JobID jobID;

	private final ExecutionVertexID executionVertexID;

	private final InputSplitProviderProtocol globalInputSplitProvider;

	TaskInputSplitProvider(final JobID jobID, final ExecutionVertexID executionVertexID,
			final InputSplitProviderProtocol globalInputSplitProvider) {

		this.jobID = jobID;
		this.executionVertexID = executionVertexID;
		this.globalInputSplitProvider = globalInputSplitProvider;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplit getNextInputSplit() {

		try {

			synchronized (this.globalInputSplitProvider) {
				return this.globalInputSplitProvider.requestNextInputSplit(this.jobID, this.executionVertexID);
			}

		} catch (IOException ioe) {
			// Convert IOException into a RuntimException and let the regular fault tolerance routines take care of the
			// rest
			throw new RuntimeException(StringUtils.stringifyException(ioe));
		}

	}
}
