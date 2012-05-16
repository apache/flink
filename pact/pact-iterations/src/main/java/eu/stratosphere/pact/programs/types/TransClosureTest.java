package eu.stratosphere.pact.programs.types;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.DataInputViewV2;
import eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.io.SerializedFifoBuffer;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable.LazyHashBucketIterator;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TransClosureTest {
  private final MemoryManager mMan = new DefaultMemoryManager(1024 * 1024 * 1024);
  private final IOManager ioMan = new IOManager("/data/tmp");

  private final AbstractInvokable memOwner = new DummyInvokable();


  public void go(FileChannel input) throws Exception
  {
    // accessor types
    final TypeAccessorsV2<TransitiveClosureEntry> tceAccessors = new TransitiveClosureEntryAccessors();
    final TypeAccessorsV2<LongPair> ipAccessors = new LongPairAccessors();
    final TypeComparator<LongPair, TransitiveClosureEntry> comparator = new LongPairTansClosureComparator();

    // hash table initialization
    final List<MemorySegment> joinMem = this.mMan.allocateStrict(this.memOwner, 400 * 1024 / 32, 32 * 1024);
    final List<MemorySegment> fifoMem = this.mMan.allocateStrict(this.memOwner, 600 * 1024 / 32, 32 * 1024);

    final MutableHashTable<TransitiveClosureEntry, LongPair> table = new MutableHashTable<TransitiveClosureEntry, LongPair>(tceAccessors, ipAccessors, comparator, joinMem, this.ioMan, 40);
    final SerializedFifoBuffer fifo = new SerializedFifoBuffer(fifoMem, 32 * 1024, false);

    // input data stream
    final TCReaderItrator inStream = new TCReaderItrator(input, fifo.getWriteEnd(), ipAccessors);

    // create the initial table and the initial updates
    final long initialBuildStart = System.nanoTime();
    table.open(inStream, EmptyMutableObjectIterator.<LongPair>get());
    final long initialBuildElapsed = System.nanoTime() - initialBuildStart;
    System.out.println("Initial building took " + (initialBuildElapsed / 1000000) + "msecs");


    // iterate
    final DataInputViewV2 inView = fifo.getReadEnd();
    final DataOutputViewV2 outView = fifo.getWriteEnd();
    final LazyTransitiveClosureEntry tcEntry = new LazyTransitiveClosureEntry();
    final LongPair pair = new LongPair();
    long numModifications = 0;

    final long iterationStart = System.nanoTime();
    boolean flushed = false;

    // read until the fifo buffer is exhausted
    while (true) {
      try {
        ipAccessors.deserialize(pair, inView);
        LazyHashBucketIterator<TransitiveClosureEntry, LongPair> matches = table.getLazyMatchesFor(pair);

        while (matches.next(tcEntry)) {
          final long newCid = pair.getValue();
          if (tcEntry.getCid() > newCid) {
            numModifications++;
            flushed = false;

            // need to update entry
            tcEntry.setCid(newCid);

            // send out updates
            long[] neighbors = tcEntry.getNeighbors();
            int numNeighbors = tcEntry.getNumNeighbors();
            for (int i = 0; i < numNeighbors; i++) {
              pair.setKey(neighbors[i]);
              ipAccessors.serialize(pair, outView);
            }
          }
        }
      }
      catch (EOFException eofex) {
        if (!flushed) {
          fifo.flush();
          flushed = true;
        } else {
          break;
        }
      }
    }

    final long iterationsElapsed = System.nanoTime() - iterationStart;
    System.out.println("Iterations took " + (iterationsElapsed / 1000000) + "msecs");
    System.out.println("Modifications: " + numModifications);
  }


  public static void main(String[] args) throws Exception
  {
    // input
    FileChannel c = new RandomAccessFile(args[0], "r").getChannel();

    TransClosureTest tct = new TransClosureTest();
    tct.go(c);
  }

  private static final class TCReaderItrator implements MutableObjectIterator<TransitiveClosureEntry>
  {
    private final FileChannel channel;

    private final ByteBuffer currentBuffer;

    private final DataOutputViewV2 targetView;

    private final TypeAccessorsV2<LongPair> serializer;

    private final LongPair pair = new LongPair();

    TCReaderItrator(FileChannel channel, DataOutputViewV2 targetView, TypeAccessorsV2<LongPair> serializer)
    throws IOException {
      this.channel = channel;
      this.currentBuffer = ByteBuffer.allocateDirect(256 * 1024);
      this.targetView = targetView;
      this.serializer = serializer;

      // initially exhausted
      this.currentBuffer.limit(0);
    }
    /* (non-Javadoc)
     * @see eu.stratosphere.pact.common.util.MutableObjectIterator#next(java.lang.Object)
     */
    @Override
    public boolean next(TransitiveClosureEntry target) throws IOException {
      final ByteBuffer buffer = this.currentBuffer;
      long vid = 0;
      long[] neighbors = target.getNeighbors();
      int numNeighbors = 0;

      long current = 0;

      while (true)
      {
        while (buffer.hasRemaining())
        {
          int next = buffer.get();
          if (next == '\n') {
            // add last neighbor
            if (neighbors.length < numNeighbors + 1) {
              long[] a = new long[Math.max(numNeighbors * 2, 2)];
              System.arraycopy(neighbors, 0, a, 0, numNeighbors);
              neighbors = a;
            }
            neighbors[numNeighbors++] = current;
            current = 0;

            // find min cid
            long min = vid;
            for (int i = 0; i < numNeighbors; i++) {
              min = Math.min(min, neighbors[i]);
            }

            // set target fields
            target.setVid(vid);
            target.setCid(min);
            target.setNeighbors(neighbors, numNeighbors);

            // create initial updates
            final LongPair pair = this.pair;
            final TypeAccessorsV2<LongPair> serializer = this.serializer;
            final DataOutputViewV2 outView = this.targetView;
            pair.setValue(min);

            for (int i = 0; i < numNeighbors; i++) {
              pair.setKey(neighbors[i]);
              serializer.serialize(pair, outView);
            }
            return true;
          }
          else if (next == ',') {
            if (neighbors.length < numNeighbors + 1) {
              long[] a = new long[Math.max(numNeighbors * 2, 2)];
              System.arraycopy(neighbors, 0, a, 0, numNeighbors);
              neighbors = a;
            }

            neighbors[numNeighbors++] =current;
            current = 0;
          }
          else if (next == '|') {
            vid = current;
            current = 0;
          }
          else {
            current *= 10;
            current += (next - '0');
          }
        }

        // refill buffer
        buffer.clear();
        int numRead = this.channel.read(buffer);
        if (numRead == -1) {
          this.channel.close();
          return false;
        }
        buffer.flip();
      }
    }
  }
}
