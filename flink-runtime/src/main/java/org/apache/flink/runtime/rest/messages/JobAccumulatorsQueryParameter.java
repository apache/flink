package org.apache.flink.runtime.rest.messages;

/**
 * query parameter for job's accumulator handler {@link org.apache.flink.runtime.rest.handler.job.JobAccumulatorsHandler}.
 */
public class JobAccumulatorsQueryParameter extends MessageQueryParameter<String> {

	private static final String key = "includeSerializedValue";

	public JobAccumulatorsQueryParameter() {
		super(key, MessageParameterRequisiteness.OPTIONAL);
	}

	@Override
	public String convertValueFromString(String value) {
		return value;
	}

	@Override
	public String convertStringToValue(String value) {
		return value;
	}
}
