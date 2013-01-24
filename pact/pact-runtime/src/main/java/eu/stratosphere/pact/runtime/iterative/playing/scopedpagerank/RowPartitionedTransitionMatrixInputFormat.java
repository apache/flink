package eu.stratosphere.pact.runtime.iterative.playing.scopedpagerank;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.regex.Pattern;

public class RowPartitionedTransitionMatrixInputFormat extends TextInputFormat {

  private static final Pattern SEPARATOR = Pattern.compile("[, \t]");

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
    String str = new String(bytes, offset, numBytes);
    String[] parts = SEPARATOR.split(str);

    target.clear();
    target.addField(new PactLong(Long.parseLong(parts[0])));

    int numEntries = (parts.length - 1) / 2;
    long[] indexes = new long[numEntries];
    double[] values = new double[numEntries];

    for (int n = 0; n < numEntries; n++) {
      indexes[n] = Long.parseLong(parts[n * 2 + 1]);
      values[n] = Double.parseDouble(parts[n * 2 + 2]);
    }

    target.addField(new SequentialAccessSparseRowVector(indexes, values));

    return true;
  }
}
