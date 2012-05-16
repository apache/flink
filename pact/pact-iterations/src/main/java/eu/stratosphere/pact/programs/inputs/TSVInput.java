package eu.stratosphere.pact.programs.inputs;

import com.google.common.base.Charsets;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.programs.preparation.tasks.LongList;

import java.util.regex.Pattern;

public class TSVInput extends DelimitedInputFormat {

  PactLong nodeId = new PactLong();
  LongList adjList = new LongList();
  long[] neighbours = new long[10];

  static final Pattern SEPARATOR = Pattern.compile("\t");

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
    String[] ids = SEPARATOR.split(new String(bytes, Charsets.UTF_8));

    if (ids.length < 2) {
      return false;
    }

    nodeId.setValue(Long.parseLong(ids[0]));

    int numNeighbours = ids.length - 1;
    if (numNeighbours > neighbours.length) {
      neighbours = new long[numNeighbours];
    }
    for (int i = 0; i < numNeighbours; i++) {
      neighbours[i] = Long.valueOf(ids[1+i]);
    }

    adjList.setList(neighbours, numNeighbours);

    target.setField(0, nodeId);
    target.setField(1, adjList);

    return true;
  }

}
