package eu.stratosphere.hadoopcompatibility;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;

/**
 * This is a dummy progress monitor / reporter
 *
 */
public class DummyHadoopReporter implements Reporter {

	@Override
	public void progress() {
	}

	@Override
	public void setStatus(String status) {

	}

	@Override
	public Counter getCounter(Enum<?> name) {
		return null;
	}

	@Override
	public Counter getCounter(String group, String name) {
		return null;
	}

	@Override
	public void incrCounter(Enum<?> key, long amount) {

	}

	@Override
	public void incrCounter(String group, String counter, long amount) {

	}

	@Override
	public InputSplit getInputSplit() throws UnsupportedOperationException {
		return null;
	}

	@Override
	public float getProgress() {
		return 0;
	}

}
