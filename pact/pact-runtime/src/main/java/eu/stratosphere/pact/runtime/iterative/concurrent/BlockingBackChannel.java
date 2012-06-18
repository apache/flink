package eu.stratosphere.pact.runtime.iterative.concurrent;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.pact.runtime.iterative.SerializedUpdateBuffer;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/** A concurrent datastructure that establishes a backchannel buffer between an iteration head and an iteration tail */
public class BlockingBackChannel {

  /** buffer to send back the superstep results */
  private final SerializedUpdateBuffer buffer;
  /** a one element queue used for blocking hand over of the buffer */
  private final BlockingQueue<SerializedUpdateBuffer> queue;

  public BlockingBackChannel(SerializedUpdateBuffer buffer) {
    this.buffer = buffer;
    queue = new ArrayBlockingQueue<SerializedUpdateBuffer>(1);
  }

  /** Called by iteration head after it has sent all input for the current superstep through the data channel (blocks iteration head) */
  public DataInputView getReadEndAfterSuperstepEnded() {
    try {
      return queue.take().switchBuffers();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Called by iteration tail to save the output of the current superstep */
  public DataOutputView getWriteEnd() {
    return buffer;
  }

  /** Called by iteration tail to signal that all input of a superstep has been processed (unblocks iteration head) */
  public void notifyOfEndOfSuperstep() {
    queue.offer(buffer);
  }

}
