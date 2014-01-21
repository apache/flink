package eu.stratosphere.hadoopcompat;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;

public class HadoopReporter implements Reporter {

	@Override
	public void progress() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setStatus(String status) {
		// TODO Auto-generated method stub

	}

	@Override
	public Counter getCounter(Enum<?> name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Counter getCounter(String group, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void incrCounter(Enum<?> key, long amount) {
		// TODO Auto-generated method stub

	}

	@Override
	public void incrCounter(String group, String counter, long amount) {
		// TODO Auto-generated method stub

	}

	@Override
	public InputSplit getInputSplit() throws UnsupportedOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getProgress() {
		// TODO Auto-generated method stub
		return 0;
	}

}
