package eu.stratosphere.pact.standalone;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;


public class ParallelDriver {
  public static final int READ_BUFFER_CAPACITY = 64*1024;

  public static void main(String[] args)
  {
    final int numThreads = Integer.parseInt(args[0]);
    final String[] filePaths = args[1].split(";");

    for (String path : filePaths) {
      System.out.println("Using file '" + path + "'");
    }

    final long totalStart = System.nanoTime();

    final AccessibleConcurrentHashMap<Integer, ParallelTriangleEntry> map = new AccessibleConcurrentHashMap<Integer, ParallelTriangleEntry>(900000, 0.85f, numThreads * 4);
    final Task[] tasks = new Task[numThreads];

    //1: Read elements into Hash-Table {
      long readingStart = System.nanoTime();
      if (filePaths.length == numThreads) {
        for (int i = 0; i < tasks.length; i++) {
          tasks[i] = new ReadAndBuildTask(map, filePaths[i], i);
        }
        runTasks(tasks);
      }
      else {
        Task[] t = new Task[filePaths.length];
        for (int i = 0; i < t.length; i++) {
          t[i] = new ReadAndBuildTask(map, filePaths[i], i);
        }
        runTasks(t);
      }

      long readingElapsed = System.nanoTime() - readingStart;
      System.out.println("Reading took: " + (readingElapsed / 1000000) + "msecs");
    }

    // 2: Sort and uniquify the IDs {
      long sortingStart = System.nanoTime();
      final Iterator<Map.Entry<Integer, ParallelTriangleEntry>>[] iters = map.getIterators(numThreads);
      for (int i = 0; i < tasks.length; i++) {
        tasks[i] = new FinalizingTask(iters[i], i);
      }
      runTasks(tasks);

      long sortingElapsed = System.nanoTime() - sortingStart;
      System.out.println("Finalizing took: " + (sortingElapsed / 1000000) + "msecs");
    }

    // 3: Notify of vertex degrees {
      long degreeComputationStart = System.nanoTime();

      final Iterator<Map.Entry<Integer, ParallelTriangleEntry>>[] iters = map.getIterators(numThreads);

      for (int i = 0; i < tasks.length; i++) {
        tasks[i] = new DegreeComputationTask(map, iters[i], i);
      }
      runTasks(tasks);

      long degreeComputationElapsed = System.nanoTime() - degreeComputationStart;
      System.out.println("Degree computation took: " + (degreeComputationElapsed / 1000000) + "msecs");
    }

    // 4: Build Triangles {
      long enumerationStart = System.nanoTime();

      final Iterator<Map.Entry<Integer, ParallelTriangleEntry>>[] iters = map.getIterators(numThreads);
      for (int i = 0; i < tasks.length; i++) {
        tasks[i] = new BuildTrianglesTask(map, iters[i], i);
      }
      runTasks(tasks);

      long enumerationElapsed = System.nanoTime() - enumerationStart;
      System.out.println("Enumeration took: " + (enumerationElapsed / 1000000) + "msecs");
    }

    // ========================================================================================
    //                           Phase 5: Count the Triangles
    // ========================================================================================
 {
      long countingStart = System.nanoTime();

      long numTriangles = 0;

      Iterator<Map.Entry<Integer, ParallelTriangleEntry>> entryIter = map.entrySet().iterator();
      while (entryIter.hasNext())
      {
        final Map.Entry<Integer, ParallelTriangleEntry> kv = entryIter.next();
        numTriangles += kv.getValue().getNumTriangles();
      }

      long countingElapsed = System.nanoTime() - countingStart;
      System.out.println("Triangle Counting took: " + (countingElapsed / 1000000) + "msecs");

      System.out.println("FOUND " + numTriangles + " TRIANGLES.");
    }

    long totalElapsed = System.nanoTime() - totalStart;
    System.out.println("TOTAL TIME: " + (totalElapsed / 1000000) + "msecs");
  }

  private static final void runTasks(final Task[] tasks)
  {
    for (int i = 0; i < tasks.length; i++) {
      tasks[i].start();
    }

    try {
      for (int i = 0; i < tasks.length; i++) {
        tasks[i].join();
      }
    }
    catch (InterruptedException iex) {
      throw new RuntimeException("Interrupted!");
    }
  }

  // ========================================================================================
  //                             Abstract Task Base
  // ========================================================================================

  protected static abstract class Task extends Thread
  {
    protected final AccessibleConcurrentHashMap<Integer, ParallelTriangleEntry> map;

    protected Task(AccessibleConcurrentHashMap<Integer, ParallelTriangleEntry> map, String taskName, int taskNumber) {
      super(taskName + " - #" + taskNumber);
      setDaemon(true);

      this.map = map;
    }

    public abstract void run();
  }

  // ========================================================================================
  //                           Phase 1: Read elements into Hash-Table
  // ========================================================================================

  protected static final class ReadAndBuildTask extends Task
  {
    private final String fileName;

    protected ReadAndBuildTask(AccessibleConcurrentHashMap<Integer, ParallelTriangleEntry> map, String fileName, int instanceNum) {
      super(map, "File-Reader and HashTable Builder Task", instanceNum);
      this.fileName = fileName;
    }

    public void run() {
      final AccessibleConcurrentHashMap<Integer, ParallelTriangleEntry> map = this.map;
      RandomAccessFile file = null;
      FileChannel channel = null;

      try {
        file = new RandomAccessFile(fileName, "r");
        channel = file.getChannel();

        final ByteBuffer buffer = ByteBuffer.allocateDirect(READ_BUFFER_CAPACITY);
        int current = 0;
        int first = 0;

        while (channel.read(buffer) != -1) {
          buffer.flip();
          while (buffer.hasRemaining()) {
            int next = buffer.get();
            if (next == '\n') {
              Integer firstI = Integer.valueOf(first);
              ParallelTriangleEntry entryFirst = map.get(firstI);
              if (entryFirst == null) {
                entryFirst = new ParallelTriangleEntry();
                ParallelTriangleEntry old = map.putIfAbsent(firstI, entryFirst);
                entryFirst = old == null ? entryFirst : old;
              }
              entryFirst.add(current);

              Integer secondI = Integer.valueOf(current);
              ParallelTriangleEntry entrySecond = map.get(secondI);
              if (entrySecond == null) {
                entrySecond = new ParallelTriangleEntry();
                ParallelTriangleEntry old = map.putIfAbsent(secondI, entrySecond);
                entrySecond = old == null ? entrySecond : old;
              }
              entrySecond.add(first);

              current = 0;
            }
            else if (next == ',') {
              first = current;
              current = 0;
            }
            else {
              current *= 10;
              current += (next - '0');
            }
          }
          buffer.clear();
        }
      }
      catch (IOException ioex) {
        System.err.println("Error reading the input into the hashtable: " + ioex.getMessage());
        ioex.printStackTrace(System.err);
        return;
      }
      finally {
        try {
          if (channel != null) {channel.close(); channel = null;}
          if (file != null) {file.close(); file = null;}
        }
        catch (IOException ioex) {
          System.err.println("Error closing the input file: " + ioex.getMessage());
          ioex.printStackTrace(System.err);
          return;
        }
      }
    }
  };

  // ========================================================================================
  //                           Phase 2: Sort and uniquify the IDs
  // ========================================================================================

  protected static final class FinalizingTask extends Task
  {
    private final Iterator<Map.Entry<Integer, ParallelTriangleEntry>> iterator;

    protected FinalizingTask(Iterator<Map.Entry<Integer, ParallelTriangleEntry>> iterator, int instanceNumber) {
      super(null, "Map-Entry-Finalizer", instanceNumber);
      this.iterator = iterator;
    }

    public void run() {
      while (this.iterator.hasNext()) {
        final Map.Entry<Integer, ParallelTriangleEntry> entry = this.iterator.next();
        entry.getValue().finalizeListBuilding();
      }
    }
  }


  // ========================================================================================
  //                           Phase 3: Notify of vertex degrees
  // ========================================================================================

  protected static final class DegreeComputationTask extends Task
  {
    protected final Iterator<Map.Entry<Integer, ParallelTriangleEntry>> entryIter;

    protected DegreeComputationTask(AccessibleConcurrentHashMap<Integer, ParallelTriangleEntry> map,
        Iterator<Map.Entry<Integer, ParallelTriangleEntry>> entryIter, int instanceNum) {
      super(map, "Degree-Computer", instanceNum);
      this.entryIter = entryIter;
    }

    public void run() {
      Iterator<Map.Entry<Integer, ParallelTriangleEntry>> entryIter = this.entryIter;
      while (entryIter.hasNext())
      {
        final Map.Entry<Integer, ParallelTriangleEntry> entry = entryIter.next();
        final int key = entry.getKey().intValue();
        final ParallelTriangleEntry tEntry = entry.getValue();
        final int degree = tEntry.size();

        for (int i = 0; i < degree; i++) {
          Integer id = Integer.valueOf(tEntry.getId(i));

          // tell that id about this key's degree
          ParallelTriangleEntry other = this.map.get(id);
          other.setDegree(key, degree);
        }
      }
    }
  }

  // ========================================================================================
  //                           Phase 4: Build Triangles
  // ========================================================================================

  protected static final class BuildTrianglesTask extends Task
  {
    protected final Iterator<Map.Entry<Integer, ParallelTriangleEntry>> entryIter;

    protected final PrintWriter out;

    protected BuildTrianglesTask(AccessibleConcurrentHashMap<Integer, ParallelTriangleEntry> map,
        Iterator<Map.Entry<Integer, ParallelTriangleEntry>> entryIter, int instanceNum) {
      super(map, "TriangleBuilder", instanceNum);
      this.entryIter = entryIter;

      try {
        out = new PrintWriter(new BufferedOutputStream(new FileOutputStream("result_" + instanceNum)));
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    public void run() {
      final AccessibleConcurrentHashMap<Integer, ParallelTriangleEntry> map = this.map;
      final Iterator<Map.Entry<Integer, ParallelTriangleEntry>> entryIter = this.entryIter;

      while (entryIter.hasNext())
      {
        final Map.Entry<Integer, ParallelTriangleEntry> kv = entryIter.next();
        final ParallelTriangleEntry entry = kv.getValue();
        final int key = kv.getKey().intValue();
        final int degree = entry.size();

        // notify all that have a larger degree of our neighbors with a lower degree than them
        for (int i = 0; i < degree; i++) {
          final int toNotifyId = entry.getId(i);
          final int toNotifyDegree = entry.getDegree(i);

          // rule out which ones not to notify
          if (toNotifyDegree < degree || (toNotifyDegree == degree && toNotifyId > key))
            continue;

          // tell it all our neighbors with a smaller id than that one itself has
          final ParallelTriangleEntry toNotify = map.get(Integer.valueOf(toNotifyId));
          toNotify.addTriangles(entry.getAllIds(), i, key, toNotifyId, out);
        }
      }

      out.close();
    }
  }

}
