package gr.ds.unipi.spades.util;

public class IntArrays {
	public static int min(int[] arr) {
		int min = Integer.MAX_VALUE;
		for (int i : arr) {
			if (min > i) min = i;
		}
		
		return min;
	}
	
	public static int max(int[] arr) {
		int max = Integer.MIN_VALUE;
		for (int i : arr) {
			if (max < i) max = i;
		}
		
		return max;
	}
	
	public static double mean(int[] arr) {
		int mean = 0;
		for (int i : arr) {
			mean += i;
		}
		
		return mean / arr.length;
	}
	
	public static double std(int[] arr) {
		double mean = mean(arr);
		double std = 0;
		for (int i : arr) {
			std += Math.pow(mean - i, 2);
		}
		
		return Math.sqrt(std / arr.length);
	}
}
