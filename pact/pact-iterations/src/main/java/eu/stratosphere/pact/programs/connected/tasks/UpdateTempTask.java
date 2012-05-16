package eu.stratosphere.pact.programs.connected.tasks;

import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.iterative.nephele.util.SerializedUpdateBuffer;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdate;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdateAccessor;

public class UpdateTempTask extends AbstractMinimalTask {

  private static final int SEGMENT_SIZE = 512*1024;

  @Override
  protected void initTask() {
    accessors[0] = new ComponentUpdateAccessor();
    outputAccessors[0] = new ComponentUpdateAccessor();
  }

  @Override
  public void run() throws Exception {
    ComponentUpdate update = new ComponentUpdate();

    List<MemorySegment> memSegments =
        memoryManager.allocateStrict(this, (int) (memorySize / SEGMENT_SIZE), SEGMENT_SIZE);
    SerializedUpdateBuffer buffer = new SerializedUpdateBuffer(memSegments, SEGMENT_SIZE, ioManager);

    while (inputs[0].next(update)) {
      update.write(buffer);
    }

    buffer.flush();
    //buffer.close();

    DataInputViewV2 readView = buffer.switchBuffers();
    DeserializingIterator readIter = new DeserializingIterator(readView);
    while (readIter.next(update)) {
      output.collect(update);
    }

    buffer.close();
  }

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

}
