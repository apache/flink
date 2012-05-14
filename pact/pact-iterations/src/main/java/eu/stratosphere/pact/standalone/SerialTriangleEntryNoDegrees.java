package eu.stratosphere.pact.standalone;

import java.util.Arrays;

public class SerialTriangleEntryNoDegrees
{
	private int[] ids;
	private int[] numTrianglesForEdge;
	private int len;
	
	private int numTriangles;
	
	public SerialTriangleEntryNoDegrees()
	{
		this.ids = new int[10];
		this.len = 0;
	}
	
	public void add(int id)
	{
		ensureIdArraySize(this.len + 1);
		this.ids[len++] = id;
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
		this.numTrianglesForEdge = new int[k];
	}
	
	public int countTriangles(final int[] otherIds, final int[] otherTriangleCounts, final int num, final int souceId)
	{
		final int[] ids = this.ids;
		final int[] triangleCounts = this.numTrianglesForEdge;
		final int len = this.len;
		
		int triangles = 0;
		
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
				if (otherId > souceId) {
					triangles++;
					triangleCounts[thisIndex]++;
					otherTriangleCounts[otherIndex]++;
				}
				otherIndex++;
			}
			thisIndex++;
		}
		
		this.numTriangles += triangles;
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
}
