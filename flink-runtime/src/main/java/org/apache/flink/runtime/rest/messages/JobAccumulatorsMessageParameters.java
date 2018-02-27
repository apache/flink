package org.apache.flink.runtime.rest.messages;

import java.util.Collection;
import java.util.Collections;

/**
 * request parameter for job accumulator's handler {@link org.apache.flink.runtime.rest.handler.job.JobAccumulatorsHandler}.
 */
public class JobAccumulatorsMessageParameters extends JobMessageParameters {

	public final JobAccumulatorsQueryParameter queryParameter = new JobAccumulatorsQueryParameter();

	@Override
	public Collection<MessageQueryParameter<?>> getQueryParameters() {
		return Collections.singleton(queryParameter);
	}
}
