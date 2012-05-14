package eu.stratosphere.pact.iterative.nephele.util;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2;
import eu.stratosphere.pact.runtime.io.AbstractPagedOutputViewV2;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class SerializedUpdateBuffer extends AbstractPagedOutputViewV2 {
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



  public SerializedUpdateBuffer()
  {
    super(-1, HEADER_LENGTH);

    this.emptyBuffers = null;
    this.fullBuffers = null;

    this.ioManager = null;
    this.channelEnumerator = null;

    this.numSegmentsSpillingThreshold = -1;
    this.minBuffersForWriteEnd = -1;
    this.minBuffersForSpilledReadEnd = -1;
    this.totalNumBuffers = -1;

    this.readEnds = Collections.emptyList();
  }

  public SerializedUpdateBuffer(List<MemorySegment> memSegments, int segmentSize, IOManager ioManager)
  {
    super(memSegments.remove(memSegments.size()-1), segmentSize, HEADER_LENGTH);

    this.totalNumBuffers = memSegments.size() + 1;
    if (this.totalNumBuffers < 3) {
      throw new IllegalArgumentException("SerializedUpdateBuffer needs at least 3 memory segments.");
    }

    this.emptyBuffers = new LinkedBlockingQueue<MemorySegment>(this.totalNumBuffers);
    this.fullBuffers = new ArrayDeque<MemorySegment>(64);

    this.emptyBuffers.addAll(memSegments);

    int threshold = (int) ((1 - SPILL_THRESHOLD) * this.totalNumBuffers);
    this.numSegmentsSpillingThreshold = threshold > 0 ? threshold : 0;
    this.minBuffersForWriteEnd = Math.max(2, Math.min(16, this.totalNumBuffers / 2));
    this.minBuffersForSpilledReadEnd = Math.max(1, Math.min(16, this.totalNumBuffers / 4));

    if (this.minBuffersForSpilledReadEnd + this.minBuffersForWriteEnd > this.totalNumBuffers) {
      throw new RuntimeException("BUG: Unfulfillable memory assignment.");
    }

    this.ioManager = ioManager;
    this.channelEnumerator = ioManager.createChannelEnumerator();
    this.readEnds = new ArrayList<ReadEnd>();
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.io.AbstractPagedOutputViewV2#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment, int)
   */
  @Override
  protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException
  {
    current.putInt(0, positionInCurrent);

    // check if we keep the segment in memory, or if we spill it
    if (this.emptyBuffers.size() > this.numSegmentsSpillingThreshold) {
      // keep buffer in memory
      this.fullBuffers.addLast(current);
    } else {
      // spill all buffers up to now
      // check, whether we have a channel already
      if (this.currentWriter == null) {
        this.currentWriter = this.ioManager.createBlockChannelWriter(this.channelEnumerator.next(), this.emptyBuffers);
      }

      // spill all elements gathered up to now
      this.numBuffersSpilled += this.fullBuffers.size();
      while (this.fullBuffers.size() > 0) {
        this.currentWriter.writeBlock(this.fullBuffers.removeFirst());
      }
      this.currentWriter.writeBlock(current);
      this.numBuffersSpilled++;
    }

    try {
      return this.emptyBuffers.take();
    } catch (InterruptedException iex) {
      throw new RuntimeException("Spilling Fifo Queue was interrupted while waiting for next buffer.");
    }
  }

  public void flush() throws IOException
  {
    advance();
  }


  public ReadEnd switchBuffers() throws IOException
  {
    // remove exhausted read ends
    for (int i = this.readEnds.size() - 1; i >= 0; --i) {
      final ReadEnd re = this.readEnds.get(i);
      if (re.disposeIfDone()) {
        this.readEnds.remove(i);
      }
    }

    // add the current memorySegment and reset this writer
    final MemorySegment current = getCurrentSegment();
    current.putInt(0, getCurrentPositionInSegment());
    this.fullBuffers.addLast(current);

    // create the reader
    final ReadEnd readEnd;
    if (this.numBuffersSpilled == 0 && this.emptyBuffers.size() >= this.minBuffersForWriteEnd) {
      // read completely from in-memory segments
      readEnd = new ReadEnd(this.fullBuffers.removeFirst(), this.emptyBuffers,
                          this.fullBuffers, null, null, this.segmentSize, 0);
    } else {
      int toSpill = Math.min(this.minBuffersForSpilledReadEnd + this.minBuffersForWriteEnd - this.emptyBuffers.size(), this.fullBuffers.size());

      // reader reads also segments on disk
      // grab some empty buffers to re-read the first segment
      if (toSpill > 0) {
        // need to spill to make a buffers available
        if (this.currentWriter == null) {
          this.currentWriter = this.ioManager.createBlockChannelWriter(this.channelEnumerator.next(), this.emptyBuffers);
        }

        for (int i = 0; i < toSpill; i++) {
          this.currentWriter.writeBlock(this.fullBuffers.removeFirst());
        }
        this.numBuffersSpilled += toSpill;
      }

      // now close the writer and create the reader
      this.currentWriter.close();
      final BlockChannelReader reader = this.ioManager.createBlockChannelReader(this.currentWriter.getChannelID());

      // gather some memory segments to circulate while reading back the data
      final ArrayList<MemorySegment> readSegments = new ArrayList<MemorySegment>();
      try {
        while (readSegments.size() < this.minBuffersForSpilledReadEnd) {
          readSegments.add(this.emptyBuffers.take());
        }

        // read the first segment
        MemorySegment firstSeg = readSegments.remove(readSegments.size() - 1);
        reader.readBlock(firstSeg);
        firstSeg = reader.getReturnQueue().take();

        // create the read end reading one less buffer, because the first buffer is already read back
        readEnd = new ReadEnd(firstSeg, this.emptyBuffers, this.fullBuffers, reader,
          readSegments, this.segmentSize, this.numBuffersSpilled - 1);
      } catch (InterruptedException iex) {
        throw new RuntimeException("SerializedUpdateBuffer was interrupted while reclaiming memory by spilling.");
      }
    }

    // reset the writer
    this.fullBuffers = new ArrayDeque<MemorySegment>(64);
    this.currentWriter = null;
    this.numBuffersSpilled = 0;
    try {
      seekOutput(emptyBuffers.take(), HEADER_LENGTH);
    } catch (InterruptedException iex) {
      throw new RuntimeException("SerializedUpdateBuffer was interrupted while reclaiming memory by spilling.");
    }

    // register this read end
    this.readEnds.add(readEnd);
    return readEnd;
  }

  public List<MemorySegment> close()
  {
    if (this.currentWriter != null) {
      try {
        this.currentWriter.closeAndDelete();
      } catch (Throwable t) {}
    }

    ArrayList<MemorySegment> freeMem = new ArrayList<MemorySegment>(64);

    // add all memory allocated to the write end
    freeMem.add(getCurrentSegment());
    clear();
    freeMem.addAll(this.fullBuffers);
    this.fullBuffers = null;

    // add memory from non-exhausted read ends
    try {
      for (int i = this.readEnds.size() - 1; i >= 0; --i) {
        final ReadEnd re = this.readEnds.remove(i);
        re.forceDispose(freeMem);
      }

      // release all empty segments
      while (freeMem.size() < this.totalNumBuffers)
        freeMem.add(this.emptyBuffers.take());
    }
    catch (InterruptedException iex) {
      throw new RuntimeException("Retrieving memory back from asynchronous I/O was interrupted.");
    }

    return freeMem;
  }

  // ============================================================================================


  private static final class ReadEnd extends AbstractPagedInputViewV2
  {
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

      this.requestsRemaining = numBuffersSpilled;
      this.spilledBuffersRemaining = numBuffersSpilled;

      // send the first requests
      while (this.requestsRemaining > 0 && emptyBuffers.size() > 0) {
        this.spilledBufferSource.readBlock(emptyBuffers.remove(emptyBuffers.size() - 1));
        this.requestsRemaining--;
      }
    }

    /* (non-Javadoc)
     * @see eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
     */
    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws IOException {
      // use the buffer to send the next request
      if (this.requestsRemaining > 0) {
        this.requestsRemaining--;
        this.spilledBufferSource.readBlock(current);
      } else {
        this.emptyBufferTarget.add(current);
      }

      // get the next buffer either from the return queue, or the full buffer source
      if (this.spilledBuffersRemaining > 0) {
        this.spilledBuffersRemaining--;
        try {
          return this.spilledBufferSource.getReturnQueue().take();
        }
        catch (InterruptedException iex) {
          throw new RuntimeException("Read End was interrupted while waiting for spilled buffer.");
        }
      } else if (this.fullBufferSource.size() > 0) {
        return this.fullBufferSource.removeFirst();
      } else {
        clear();

        // delete the channel, if we had one
        if (this.spilledBufferSource != null) {
          this.spilledBufferSource.closeAndDelete();
        }

        throw new EOFException();
      }
    }

    /* (non-Javadoc)
     * @see eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2#getLimitForSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
     */
    @Override
    protected int getLimitForSegment(MemorySegment segment) throws IOException {
      return segment.getInt(0);
    }

    private boolean disposeIfDone() {
      if (this.fullBufferSource.isEmpty() && this.spilledBuffersRemaining == 0)
      {
        if (getCurrentSegment() == null || getCurrentPositionInSegment() >= getCurrentSegmentLimit()) {
          if (getCurrentSegment() != null) {
            this.emptyBufferTarget.add(getCurrentSegment());
            clear();
          }

          if (this.spilledBufferSource != null) {
            try {
              this.spilledBufferSource.closeAndDelete();
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
      freeMemTarget.addAll(this.fullBufferSource);

      // add the segments with the requests issued but not returned
      for (int i = this.spilledBuffersRemaining - this.requestsRemaining; i > 0; --i) {
        freeMemTarget.add(this.emptyBufferTarget.take());
      }

      if (this.spilledBufferSource != null) {
        try {
          this.spilledBufferSource.closeAndDelete();
        } catch (Throwable t) {}
      }
    }
  }
}