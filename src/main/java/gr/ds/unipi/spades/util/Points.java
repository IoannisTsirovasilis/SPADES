package gr.ds.unipi.spades.util;

import java.util.ArrayList;
import java.util.Comparator;

import gr.ds.unipi.spades.geometry.Point;

public class Points {
	private Points() {}
	
	public static void sort(Point[] a, Comparator<? super Point> c) throws IllegalArgumentException {
		TimSort.sort(a, c);
	}
	
	public static void sort(ArrayList<Point> a, Comparator<? super Point> c) throws IllegalArgumentException {
		TimSort.sort(a, c);
	}
	
	public static void mergeSort(Point[] a) {
		sort(a, 0, a.length - 1);
	}
	
	private static void merge(Point[] arr, int l, int m, int r) 
    { 
        // Find sizes of two subarrays to be merged 
        int n1 = m - l + 1; 
        int n2 = r - m; 
  
        /* Create temp arrays */
        Point[] L = new Point[n1]; 
        Point[] R = new Point[n2]; 
  
        /*Copy data to temp arrays*/
        for (int i = 0; i < n1; ++i) 
            L[i] = arr[l + i]; 
        for (int j = 0; j < n2; ++j) 
            R[j] = arr[m + 1 + j]; 
  
        /* Merge the temp arrays */
  
        // Initial indexes of first and second subarrays 
        int i = 0, j = 0; 
  
        // Initial index of merged subarry array 
        int k = l; 
        while (i < n1 && j < n2) { 
            if (Point.XComparator.compare(L[i], R[j]) <= 0) { 
                arr[k] = L[i]; 
                i++; 
            } 
            else { 
                arr[k] = R[j]; 
                j++; 
            } 
            k++; 
        } 
  
        /* Copy remaining elements of L[] if any */
        while (i < n1) { 
            arr[k] = L[i]; 
            i++; 
            k++; 
        } 
  
        /* Copy remaining elements of R[] if any */
        while (j < n2) { 
            arr[k] = R[j]; 
            j++; 
            k++; 
        } 
    } 
  
    // Main function that sorts arr[l..r] using 
    // merge() 
    private static void sort(Point[] arr, int l, int r) 
    { 
        if (l < r) { 
            // Find the middle point 
            int m = (l + r) / 2; 
  
            // Sort first and second halves 
            sort(arr, l, m); 
            sort(arr, m + 1, r); 
  
            // Merge the sorted halves 
            merge(arr, l, m, r); 
        } 
    } 
}
