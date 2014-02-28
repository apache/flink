package eu.stratosphere.hadoopcompatibility;

import org.apache.hadoop.util.Progressable;

/**
 * This is a dummy progress
 *
 */
public class DummyHadoopProgressable implements Progressable {
	@Override
	public void progress() {

	}
}
