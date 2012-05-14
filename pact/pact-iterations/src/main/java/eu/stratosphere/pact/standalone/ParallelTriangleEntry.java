package eu.stratosphere.pact.standalone;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParallelTriangleEntry
{
	private final AtomicBoolean spinLock = new AtomicBoolean();
	
	private volatile int[] ids;
	private volatile int[] degrees;
	private volatile int[] numTrianglesForEdge;
	private volatile int len;
	
	private volatile int numTriangles;
	
	public ParallelTriangleEntry()
	{
		this.ids = new int[10];
		this.len = 0;
	}
	
	public void add(int id)
	{
		lock();
		try {
			ensureIdArraySize(this.len + 1);
			this.ids[len++] = id;
		} finally {
			unlock();
		}
	}
	
	public int getId(int pos)
	{
		rangeCheck(pos);
		return this.ids[pos];
	}
	
	public int[] getAllIds()
	{
		return this.ids;
	}
	
	public int[] getAllTriangleCounts()
	{
		return this.numTrianglesForEdge;
	}
	
	public int getDegree(int pos)
	{
		rangeCheck(pos);
		return this.degrees[pos];
	}
	
	public void setDegree(int id, int degree)
	{
		int pos = Arrays.binarySearch(this.ids, 0, this.len, id);
		if (pos >= 0) {
			this.degrees[pos] = degree;
		}
		else {
			System.out.println(id);
			System.out.println("Array star");
			Arrays.toString(this.ids);
			System.out.println("arrad end");
			throw new IllegalStateException("ID not found!");
		}
	}
	
	public void setNumTrianglesForEdge(int pos, int numTriangles)
	{
		this.numTrianglesForEdge[pos] = numTriangles;
	}
	
	public int size()
	{
		return this.len;
	}
	
	public void finalizeListBuilding()
	{
		int[] ids = this.ids;
		
		// order the elements
		Arrays.sort(ids, 0, this.len);
		
		// throw out duplicates
		int k = 0;
		for (int curr = -1, i = 0; i < this.len; i++) {
			int val = ids[i];
			if (val != curr) {
				curr = val;
				ids[k] = ids[i];
				k++;
			}
			else {
				ids[k] = ids[i];
			}
		}
		
		if (ids.length >= (k << 1)) {
			int[] na = new int[k];
			System.arraycopy(ids, 0, na, 0, k);
			this.ids = na;
		}
		
		this.len = k;
		this.degrees = new int[this.len + 10];
	}
	
	public void addTriangleCandidate(int id)
	{
		if (Arrays.binarySearch(this.ids, 0, this.len, id) >= 0) {
			lock();
			this.numTriangles++;
			unlock();
		}
	}
	
	public void addTriangles(final int[] otherIds, final int num, final int firstId, final int secondId, final PrintWriter out)
	{
		final int[] ids = this.ids;
		final int len = this.len;
		
		lock();
		
		// go through our neighbors and see which of those neighbors we have
		int thisIndex = 0, otherIndex = 0;
		while (thisIndex < len && otherIndex < num)
		{
			int thisId = ids[thisIndex];
			int otherId = otherIds[otherIndex];
			
			while (thisId < otherId && thisIndex < len-1) {
				thisId = ids[++thisIndex];
			}
			
			while (thisId > otherId && otherIndex < num - 1) {
				otherId = otherIds[++otherIndex];
			}
			
			if (thisId == otherId) {
				this.numTriangles++;
//				out.print(firstId);
//				out.print(',');
//				out.print(secondId);
//				out.print(',');
//				out.print(thisId);
//				out.print('\n');
				otherIndex++;
			}
			thisIndex++;
		}
		
		unlock();
	}
	
	public int countTriangles(final int[] otherIds, final int[] otherTriangleCounts, final int num)
	{
		final int[] ids = this.ids;
		final int[] triangleCounts = this.numTrianglesForEdge;
		final int len = this.len;
		
		int triangles = 0;
		
		lock();
		
		// go through our neighbors and see which of those neighbors we have
		int thisIndex = 0, otherIndex = 0;
		while (thisIndex < len && otherIndex < num)
		{
			int thisId = ids[thisIndex];
			int otherId = otherIds[otherIndex];
			
			while (thisId < otherId && thisIndex < len-1) {
				thisId = ids[++thisIndex];
			}
			
			while (thisId > otherId && otherIndex < num - 1) {
				otherId = otherIds[++otherIndex];
			}
			
			if (thisId == otherId) {
				triangles++;
				triangleCounts[thisIndex]++;
				otherTriangleCounts[otherIndex]++;
				otherIndex++;
			}
			thisIndex++;
		}
		
		this.numTriangles += triangles;
		
		unlock();
		
		return triangles;
	}
	
	public int getNumTriangles()
	{
		return this.numTriangles;
	}
	
	// --------------------------------------------------------------------------------------------
	
	private final void ensureIdArraySize(int size) {
		if (this.ids.length < size) {
			int[] a = new int[this.ids.length * 2];
			System.arraycopy(this.ids, 0, a, 0, this.ids.length);
			this.ids = a;
		}
	}
	
	private final void rangeCheck(int pos) {
		if (pos < 0 || pos >= this.len)
			throw new IndexOutOfBoundsException();
	}
	
	public final void lock() {
		for (; !this.spinLock.compareAndSet(false, true); );
	}
	
	public final void unlock() {
		this.spinLock.set(false);
	}
}
