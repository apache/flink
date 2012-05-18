package eu.stratosphere.pact.iterative.nephele.util;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.AbstractPagedInputView;
import eu.stratosphere.nephele.services.memorymanager.AbstractPagedOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class SerializedUpdateBuffer extends AbstractPagedOutputView {

  private static final int HEADER_LENGTH = 4;

  private static final float SPILL_THRESHOLD = 0.9f;

  private final LinkedBlockingQueue<MemorySegment> emptyBuffers;

  private ArrayDeque<MemorySegment> fullBuffers;

  private BlockChannelWriter currentWriter;

  private final IOManager ioManager;

  private final Channel.Enumerator channelEnumerator;

  private final int numSegmentsSpillingThreshold;

  private int numBuffersSpilled;

  private final int minBuffersForWriteEnd;

  private final int minBuffersForSpilledReadEnd;

  private final List<ReadEnd> readEnds;

  private final int totalNumBuffers;



  public SerializedUpdateBuffer() {
    super(-1, HEADER_LENGTH);

    emptyBuffers = null;
    fullBuffers = null;

    ioManager = null;
    channelEnumerator = null;

    numSegmentsSpillingThreshold = -1;
    minBuffersForWriteEnd = -1;
    minBuffersForSpilledReadEnd = -1;
    totalNumBuffers = -1;

    readEnds = Collections.emptyList();
  }

  public SerializedUpdateBuffer(List<MemorySegment> memSegments, int segmentSize, IOManager ioManager) {
    super(memSegments.remove(memSegments.size()-1), segmentSize, HEADER_LENGTH);

    totalNumBuffers = memSegments.size() + 1;
    Preconditions.checkArgument(totalNumBuffers >= 3, "SerializedUpdateBuffer needs at least 3 memory segments.");

    emptyBuffers = new LinkedBlockingQueue<MemorySegment>(totalNumBuffers);
    fullBuffers = new ArrayDeque<MemorySegment>(64);

    emptyBuffers.addAll(memSegments);

    int threshold = (int) ((1 - SPILL_THRESHOLD) * totalNumBuffers);
    numSegmentsSpillingThreshold = threshold > 0 ? threshold : 0;
    minBuffersForWriteEnd = Math.max(2, Math.min(16, totalNumBuffers / 2));
    minBuffersForSpilledReadEnd = Math.max(1, Math.min(16,totalNumBuffers / 4));

    Preconditions.checkState(minBuffersForSpilledReadEnd + minBuffersForWriteEnd <= totalNumBuffers,
        "BUG: Unfulfillable memory assignment.");

    this.ioManager = ioManager;
    channelEnumerator = ioManager.createChannelEnumerator();
    readEnds = Lists.newArrayList();
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.io.AbstractPagedOutputViewV2#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment, int)
   */
  @Override
  protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException {
    current.putInt(0, positionInCurrent);

    // check if we keep the segment in memory, or if we spill it
    if (emptyBuffers.size() > numSegmentsSpillingThreshold) {
      // keep buffer in memory
      fullBuffers.addLast(current);
    } else {
      // spill all buffers up to now
      // check, whether we have a channel already
      if (currentWriter == null) {
        currentWriter = ioManager.createBlockChannelWriter(channelEnumerator.next(), emptyBuffers);
      }

      // spill all elements gathered up to now
      numBuffersSpilled += fullBuffers.size();
      while (fullBuffers.size() > 0) {
        currentWriter.writeBlock(fullBuffers.removeFirst());
      }
      currentWriter.writeBlock(current);
      numBuffersSpilled++;
    }

    try {
      return emptyBuffers.take();
    } catch (InterruptedException iex) {
      throw new RuntimeException("Spilling Fifo Queue was interrupted while waiting for next buffer.");
    }
  }

  public void flush() throws IOException {
    advance();
  }


  public ReadEnd switchBuffers() throws IOException {
    // remove exhausted read ends
    for (int i = readEnds.size() - 1; i >= 0; --i) {
      final ReadEnd re = readEnds.get(i);
      if (re.disposeIfDone()) {
        readEnds.remove(i);
      }
    }

    // add the current memorySegment and reset this writer
    final MemorySegment current = getCurrentSegment();
    current.putInt(0, getCurrentPositionInSegment());
    fullBuffers.addLast(current);

    // create the reader
    final ReadEnd readEnd;
    if (numBuffersSpilled == 0 && emptyBuffers.size() >= minBuffersForWriteEnd) {
      // read completely from in-memory segments
      readEnd = new ReadEnd(fullBuffers.removeFirst(), emptyBuffers, fullBuffers, null, null, segmentSize, 0);
    } else {
      int toSpill = Math.min(minBuffersForSpilledReadEnd + minBuffersForWriteEnd - emptyBuffers.size(),
          fullBuffers.size());

      // reader reads also segments on disk
      // grab some empty buffers to re-read the first segment
      if (toSpill > 0) {
        // need to spill to make a buffers available
        if (currentWriter == null) {
          currentWriter = ioManager.createBlockChannelWriter(channelEnumerator.next(), emptyBuffers);
        }

        for (int i = 0; i < toSpill; i++) {
          currentWriter.writeBlock(fullBuffers.removeFirst());
        }
        numBuffersSpilled += toSpill;
      }

      // now close the writer and create the reader
      currentWriter.close();
      final BlockChannelReader reader = ioManager.createBlockChannelReader(currentWriter.getChannelID());

      // gather some memory segments to circulate while reading back the data
      final ArrayList<MemorySegment> readSegments = Lists.newArrayList();
      try {
        while (readSegments.size() < minBuffersForSpilledReadEnd) {
          readSegments.add(emptyBuffers.take());
        }

        // read the first segment
        MemorySegment firstSeg = readSegments.remove(readSegments.size() - 1);
        reader.readBlock(firstSeg);
        firstSeg = reader.getReturnQueue().take();

        // create the read end reading one less buffer, because the first buffer is already read back
        readEnd = new ReadEnd(firstSeg, emptyBuffers, fullBuffers, reader, readSegments, segmentSize,
            numBuffersSpilled - 1);
      } catch (InterruptedException iex) {
        throw new RuntimeException("SerializedUpdateBuffer was interrupted while reclaiming memory by spilling.");
      }
    }

    // reset the writer
    fullBuffers = new ArrayDeque<MemorySegment>(64);
    currentWriter = null;
    numBuffersSpilled = 0;
    try {
      seekOutput(emptyBuffers.take(), HEADER_LENGTH);
    } catch (InterruptedException iex) {
      throw new RuntimeException("SerializedUpdateBuffer was interrupted while reclaiming memory by spilling.");
    }

    // register this read end
    readEnds.add(readEnd);
    return readEnd;
  }

  public List<MemorySegment> close() {
    if (currentWriter != null) {
      try {
        currentWriter.closeAndDelete();
      } catch (Throwable t) {}
    }

    ArrayList<MemorySegment> freeMem = Lists.newArrayListWithCapacity(64);

    // add all memory allocated to the write end
    freeMem.add(getCurrentSegment());
    clear();
    freeMem.addAll(fullBuffers);
    fullBuffers = null;

    // add memory from non-exhausted read ends
    try {
      for (int i = readEnds.size() - 1; i >= 0; --i) {
        final ReadEnd re = readEnds.remove(i);
        re.forceDispose(freeMem);
      }

      // release all empty segments
      while (freeMem.size() < totalNumBuffers)
        freeMem.add(emptyBuffers.take());
    }
    catch (InterruptedException iex) {
      throw new RuntimeException("Retrieving memory back from asynchronous I/O was interrupted.");
    }

    return freeMem;
  }

  // ============================================================================================


  private static final class ReadEnd extends AbstractPagedInputView {

    private final LinkedBlockingQueue<MemorySegment> emptyBufferTarget;

    private final ArrayDeque<MemorySegment> fullBufferSource;

    private final BlockChannelReader spilledBufferSource;

    private int spilledBuffersRemaining;

    private int requestsRemaining;

    private ReadEnd(MemorySegment firstMemSegment, LinkedBlockingQueue<MemorySegment> emptyBufferTarget,
        ArrayDeque<MemorySegment> fullBufferSource, BlockChannelReader spilledBufferSource,
        ArrayList<MemorySegment> emptyBuffers, int segmentSize, int numBuffersSpilled)
    throws IOException {
      super(firstMemSegment, firstMemSegment.getInt(0), HEADER_LENGTH);

      this.emptyBufferTarget = emptyBufferTarget;
      this.fullBufferSource = fullBufferSource;

      this.spilledBufferSource = spilledBufferSource;

      requestsRemaining = numBuffersSpilled;
      this.spilledBuffersRemaining = numBuffersSpilled;

      // send the first requests
      while (requestsRemaining > 0 && emptyBuffers.size() > 0) {
        this.spilledBufferSource.readBlock(emptyBuffers.remove(emptyBuffers.size() - 1));
        requestsRemaining--;
      }
    }

    /* (non-Javadoc)
     * @see eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
     */
    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws IOException {
      // use the buffer to send the next request
      if (requestsRemaining > 0) {
        requestsRemaining--;
        spilledBufferSource.readBlock(current);
      } else {
        emptyBufferTarget.add(current);
      }

      // get the next buffer either from the return queue, or the full buffer source
      if (spilledBuffersRemaining > 0) {
        spilledBuffersRemaining--;
        try {
          return spilledBufferSource.getReturnQueue().take();
        }
        catch (InterruptedException iex) {
          throw new RuntimeException("Read End was interrupted while waiting for spilled buffer.");
        }
      } else if (fullBufferSource.size() > 0) {
        return fullBufferSource.removeFirst();
      } else {
        clear();

        // delete the channel, if we had one
        if (spilledBufferSource != null) {
          spilledBufferSource.closeAndDelete();
        }

        throw new EOFException();
      }
    }

    /* (non-Javadoc)
     * @see eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2#getLimitForSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
     */
    @Override
    protected int getLimitForSegment(MemorySegment segment) {
      return segment.getInt(0);
    }

    private boolean disposeIfDone() {
      if (fullBufferSource.isEmpty() && spilledBuffersRemaining == 0) {
        if (getCurrentSegment() == null || getCurrentPositionInSegment() >= getCurrentSegmentLimit()) {
          if (getCurrentSegment() != null) {
            emptyBufferTarget.add(getCurrentSegment());
            clear();
          }

          if (spilledBufferSource != null) {
            try {
              spilledBufferSource.closeAndDelete();
            } catch (Throwable t) {}
          }
          return true;
        }
      }
      return false;
    }

    private void forceDispose(List<MemorySegment> freeMemTarget) throws InterruptedException {
      // add the current segment
      final MemorySegment current = getCurrentSegment();
      clear();
      if (current != null) {
        freeMemTarget.add(current);
      }

      // add all remaining memory
      freeMemTarget.addAll(fullBufferSource);

      // add the segments with the requests issued but not returned
      for (int i = spilledBuffersRemaining - requestsRemaining; i > 0; --i) {
        freeMemTarget.add(emptyBufferTarget.take());
      }

      if (spilledBufferSource != null) {
        try {
          spilledBufferSource.closeAndDelete();
        } catch (Throwable t) {}
      }
    }
  }
}