package eu.stratosphere.pact.compiler.util;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import eu.stratosphere.pact.compiler.util.FieldSetOperations;

public class FieldSetOperationTest {

	@Test
	public void unionSetTest() {
		
		int[] a,b,u;
		
		a = new int[]{0,1,2,3,4};
		b = new int[]{5,6,7,8,9};
		u = FieldSetOperations.unionSets(a, b);
		assertTrue(Arrays.toString(u), Arrays.equals(u, new int[]{0,1,2,3,4,5,6,7,8,9}));
		
		a = new int[]{0,1,2,5,6};
		b = new int[]{5,6,7,8,9};
		u = FieldSetOperations.unionSets(a, b);
		assertTrue(Arrays.toString(u), Arrays.equals(u, new int[]{0,1,2,5,6,7,8,9}));
		
		a = new int[]{1,2,6,7};
		b = new int[]{1,3,6,7,8,9};
		u = FieldSetOperations.unionSets(a, b);
		assertTrue(Arrays.toString(u), Arrays.equals(u, new int[]{1,2,3,6,7,8,9}));
		
		a = new int[]{0,4,5,6,8};
		b = new int[]{1,2,3,4,5,6,7,8};
		u = FieldSetOperations.unionSets(a, b);
		assertTrue(Arrays.toString(u), Arrays.equals(u, new int[]{0,1,2,3,4,5,6,7,8}));
	}
	
	@Test
	public void emptyIntersectTest() {
		
		int[] a,b;
		
		a = new int[]{0,1,2,3,4};
		b = new int[]{5,6,7,8,9};
		assertTrue(FieldSetOperations.emptyIntersect(a, b));
		
		a = new int[]{0,1,2,5,6};
		b = new int[]{5,6,7,8,9};
		assertFalse(FieldSetOperations.emptyIntersect(a, b));
		
		a = new int[]{1,2,4,5,6};
		b = new int[]{3,7,8,9};
		assertTrue(FieldSetOperations.emptyIntersect(a, b));
		
		a = new int[]{1,2,4,5,6,9};
		b = new int[]{3,7,8,9};
		assertFalse(FieldSetOperations.emptyIntersect(a, b));
		
		a = new int[]{1,3,5,7,9};
		b = new int[]{0,2,4,6,8};
		assertTrue(FieldSetOperations.emptyIntersect(a, b));
	}
	
	@Test
	public void fullyContainedTest() {
		
		int[] a,b;
		
		a = new int[]{0,1,2,3,4};
		b = new int[]{5,6,7,8,9};
		assertFalse(FieldSetOperations.fullyContained(a, b));

		a = new int[]{0,1,2,3,4};
		b = new int[]{0,2,4};
		assertTrue(FieldSetOperations.fullyContained(a, b));
		
		a = new int[]{0,1,2,3,4};
		b = new int[]{1,2,4,5};
		assertFalse(FieldSetOperations.fullyContained(a, b));

		a = new int[]{5,6,7,8,9};
		b = new int[]{0,1,2,3,4};
		assertFalse(FieldSetOperations.fullyContained(a, b));
		
		a = new int[]{0,2,4};
		b = new int[]{0,1,2,3,4};
		assertFalse(FieldSetOperations.fullyContained(a, b));

		a = new int[]{1,2,4,5};
		b = new int[]{0,1,2,3,4};
		assertFalse(FieldSetOperations.fullyContained(a, b));
		
		a = new int[]{1,2,4,5};
		b = new int[]{1,2,4,5};
		assertTrue(FieldSetOperations.fullyContained(a, b));
	}
	
}
