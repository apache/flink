package eu.stratosphere.pact.compiler.util;

import java.util.Arrays;

public class FieldSetOperations {

	/**
	 * Computes the union of two int arrays. 
	 * 
	 * @param a sorted int array
	 * @param b sorted int array
	 * @return sorted unioned array
	 */
	public static int[] unionSets(int[] a, int[] b) {
		int i = 0;
		int j = 0;
		int[] u = new int[a.length+b.length];
		Arrays.fill(u,-1);
				
		int k = 0;
		while(i < a.length && j < b.length) {
			if(a[i] == b[j]) {
				if(a[i] != u[k==0 ? 0 : k-1]) {
					u[k++] = a[i];
				}
				i++;
			} else if(a[i] < b[j]) {
				if(a[i] != u[k==0 ? 0 : k-1]) {
					u[k++] = a[i];
				}
				i++;
			} else {
				if(b[j] != u[k==0 ? 0 : k-1]) {
					u[k++] = b[j];
				}
				j++;
			}
		}
		while(i < a.length) {
			if(a[i] != u[k==0 ? 0 : k-1]) {
				u[k++] = a[i];
			}
			i++;
		}
		while(j < b.length) {
			if(b[j] != u[k==0 ? 0 : k-1]) {
				u[k++] = b[j];
			}
			j++;
		}
		if(k == a.length+b.length) {
			return u;
		} else {
			return Arrays.copyOf(u, k);
		}
	}
	
	/**
	 * Determines whether the intersection of two int arrays is emtpy. 
	 * 
	 * @param a sorted int array
	 * @param b sorted int array
	 * @return true if intersection of arrays is empty, false otherwise
	 */
	public static boolean emptyIntersect(int[] a, int[] b) {
		
		int f = 0;
		int s = 0;
		while(f < a.length && s < b.length) {
			if(a[f] < b[s]) {
				f++;
			} else if(a[f] > b[s]) {
				s++;
			} else {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Determines whether an int array is fully contained within another int array.
	 * 
	 * @param a a sorted int array
	 * @param b a sorted int array
	 * @return true if the b array is fully contained within the a.
	 */
	public static boolean fullyContained(int[] a, int[] b) {
		if(b.length > a.length) return false;
		int r = 0;
		int d = 0;
		while(r < a.length && d < b.length) {
			if(a[r] < b[d]) {
				// step to next field
				r++;
			} else if(a[r] > b[d]) {
				// contained field was not found
				return false;
			} else {
				// contained field was found
				r++;
				d++;
			}
		}
		if(d == b.length) {
			// all fields have been found
			return true;
		} else {
			return false;
		}
	}
	
}
