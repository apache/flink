package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.io.HdfsCheckpointWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DanglingPageRankHdfsCheckpointWriter extends HdfsCheckpointWriter {

	private LongWritable key;

	private DanglingRankWritable value;

	public DanglingPageRankHdfsCheckpointWriter()
		throws IOException {
		key = new LongWritable();
		value = new DanglingRankWritable();
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends WritableComparable> keyClass() {
		return key.getClass();
	}

	@Override
	protected Class<? extends Writable> valueClass() {
		return value.getClass();
	}

	@Override
	protected void add(SequenceFile.Writer writer, PactRecord record) throws IOException {
		key.set(record.getField(0, PactLong.class).getValue());
		value.set(record.getField(1, PactDouble.class).getValue(), record.getField(2, BooleanValue.class).get());
		writer.append(key, value);
	}

	public static class DanglingRankWritable implements Writable {

		private double rank;

		private boolean dangling;

		public DanglingRankWritable() {
		}

		public void set(double rank, boolean dangling) {
			this.rank = rank;
			this.dangling = dangling;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeDouble(rank);
			out.writeBoolean(dangling);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			rank = in.readDouble();
			dangling = in.readBoolean();
		}
	}
}
