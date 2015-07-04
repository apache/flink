package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.util.SerializedValue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * LICENSE SHOULD GO HERE.
 * Created by kkloudas on 6/24/15.
 */
public class LargeAccumulatorEvent extends SerializedValue<Map<String, List<BlobKey>>> {

	/** JobID for the target job */
	private final JobID jobID;
	private final Map<String, List<BlobKey>> value;

	public LargeAccumulatorEvent(JobID jobID, Map<String, List<BlobKey>> value) throws IOException {
		super(value);
		this.jobID = jobID;
		this.value = value;
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public Map<String, List<BlobKey>> getValue() { return  this.value; }
}
