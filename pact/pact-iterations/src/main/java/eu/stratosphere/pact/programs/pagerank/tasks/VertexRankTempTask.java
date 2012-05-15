package eu.stratosphere.pact.programs.pagerank.tasks;

import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.iterative.nephele.util.SerializedUpdateBuffer;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRank;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRankAccessor;

public class VertexRankTempTask extends AbstractMinimalTask {

  private static final int SEGMENT_SIZE = 64*1024;

  @Override
  protected void initTask() {
    accessors[0] = new VertexPageRankAccessor();
    outputAccessors[0] = new VertexPageRankAccessor();
  }

  @Override
  public void run() throws Exception {
    VertexPageRank vRank = new VertexPageRank();

    List<MemorySegment> memSegments =
        memoryManager.allocateStrict(this, (int) (memorySize / SEGMENT_SIZE), SEGMENT_SIZE);
    SerializedUpdateBuffer buffer = new SerializedUpdateBuffer(memSegments, SEGMENT_SIZE, ioManager);

    while (inputs[0].next(vRank)) {
      vRank.write(buffer);
    }

    buffer.flush();

    DataInputViewV2 readView = buffer.switchBuffers();;
    DeserializingIterator readIter = new DeserializingIterator(readView);
    while (readIter.next(vRank)) {
      output.collect(vRank);
    }

    buffer.close();
  }

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

}
