package gr.ds.unipi.spades.util;

import java.util.Comparator;

import gr.ds.unipi.spades.geometry.Point;

public class Points {
	private Points() {}
	
	public static void sort(Point[] a, Comparator<? super Point> c) throws IllegalArgumentException {
		TimSort.sort(a, c);
	}
}
