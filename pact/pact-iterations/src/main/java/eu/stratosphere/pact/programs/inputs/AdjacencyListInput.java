package eu.stratosphere.pact.programs.inputs;

import com.google.common.base.Charsets;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.programs.preparation.tasks.LongList;

public class AdjacencyListInput extends DelimitedInputFormat {

  PactLong nodeId = new PactLong();
  LongList adjList = new LongList();
  long[] neighbours = new long[10];

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
    String[] pair = new String(bytes, Charsets.UTF_8).split("\\|");
    String[] list = pair[1].split(",");

    nodeId.setValue(Long.parseLong(pair[0]));

    if (list.length > neighbours.length) {
      neighbours = new long[list.length];
    }
    for (int i = 0; i < list.length; i++) {
      neighbours[i] = Long.valueOf(list[i]);
    }
    adjList.setList(neighbours, list.length);

    target.setField(0, nodeId);
    target.setField(1, adjList);

    return true;
  }

}
