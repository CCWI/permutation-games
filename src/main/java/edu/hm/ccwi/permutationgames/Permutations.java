package edu.hm.ccwi.permutationgames;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * See <a href="http://stackoverflow.com/a/14444037">http://stackoverflow.com/a/14444037</a>
 * @see <a href="http://wordaligned.org/articles/next-permutation">http://wordaligned.org/articles/next-permutation</a>
 * 
 * @author Yevgen Yampolskiy
 *
 * @param <E> Array to be permuted
 */
public class Permutations<E> implements Iterator<E[]> {
	private E[] arr;
	private int[] ind;
	private boolean has_next;

	// next() returns this array, make it public
	public E[] output;

	@SuppressWarnings("unchecked")
	Permutations(E[] arr) {
		this.arr = arr.clone();
		ind = new int[arr.length];

		// convert an array of any elements into array of integers - first
		// occurrence is used to enumerate
		Map<E, Integer> hm = new HashMap<E, Integer>();
		for (int i = 0; i < arr.length; i++) {
			Integer n = hm.get(arr[i]);
			if (n == null) {
				hm.put(arr[i], i);
				n = i;
			}
			ind[i] = n.intValue();
		}

		// start with ascending sequence of integers
		Arrays.sort(ind);

		// output = new E[arr.length]; <-- cannot do in Java with generics, so
		// use reflection
		output = (E[]) Array.newInstance(arr.getClass().getComponentType(), arr.length);
		has_next = true;
	}

	@Override
	public boolean hasNext() {
		return has_next;
	}

	/**
	 * Computes next permutations. Same array instance is returned every time!
	 * 
	 * @return
	 */
	@Override
	public E[] next() {
		if (!has_next)
			throw new NoSuchElementException();

		for (int i = 0; i < ind.length; i++) {
			output[i] = arr[ind[i]];
		}

		// get next permutation
		has_next = false;
		for (int tail = ind.length - 1; tail > 0; tail--) {
			if (ind[tail - 1] < ind[tail]) { // still increasing

				// find last element which does not exceed ind[tail-1]
				int s = ind.length - 1;
				while (ind[tail - 1] >= ind[s])
					s--;

				swap(ind, tail - 1, s);

				// reverse order of elements in the tail
				for (int i = tail, j = ind.length - 1; i < j; i++, j--) {
					swap(ind, i, j);
				}
				has_next = true;
				break;
			}
		}
		return output;
	}

	private void swap(int[] arr, int i, int j) {
		int t = arr[i];
		arr[i] = arr[j];
		arr[j] = t;
	}

	@Override
	public void remove() {
	}

}
