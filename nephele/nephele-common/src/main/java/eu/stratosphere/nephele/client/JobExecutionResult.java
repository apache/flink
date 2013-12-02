package eu.stratosphere.nephele.client;

import java.util.Map;

public class JobExecutionResult {
	
	private long netRuntime;
	private Map<String, Object> accumulatorResults;
	
	public JobExecutionResult(long netRuntime, Map<String, Object> accumulators) {
		this.netRuntime = netRuntime;
		this.accumulatorResults = accumulators;
	}
	
	public long getNetRuntime() {
		return this.netRuntime;
	}

	public Object getAccumulatorResult(String accumulatorName) {
		return this.accumulatorResults.get(accumulatorName);
	}

	public Map<String, Object> getAllAccumulatorResults() {
		return this.accumulatorResults;
	}
	
	/**
	 * @param accumulatorName
	 *            Name of the counter
	 * @return Result of the counter, or null if the counter does not exist
	 */
	public Integer getIntCounterResult(String accumulatorName) {
		Object result = this.accumulatorResults.get(accumulatorName);
		if (result == null) {
			return null;
		}
		if (!(result instanceof Integer)) {
			throw new ClassCastException(
					"Requested result of the accumulator '" + accumulatorName
							+ "' should be Integer but has type "
							+ result.getClass());
		}
		return (Integer) result;
	}

	// TODO Create convenience methods for the other shipped accumulator types

}
