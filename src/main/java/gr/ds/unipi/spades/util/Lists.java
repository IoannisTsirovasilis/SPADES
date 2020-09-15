package gr.ds.unipi.spades.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

public class Lists {
	public static double min(List<Tuple2<Integer, Integer>> list) {
		double min = Integer.MAX_VALUE;
		Iterator<Tuple2<Integer, Integer>> iter = list.iterator(); 
		Integer i;
		while (iter.hasNext()) {
			i = iter.next()._2;
			if (min > i) min = i;
		}
		return min;		
	}
	
	public static double max(List<Tuple2<Integer, Integer>> list) {
		double max = Integer.MIN_VALUE;
		Iterator<Tuple2<Integer, Integer>> iter = list.iterator(); 
		Integer i;
		while (iter.hasNext()) {
			i = iter.next()._2;
			if (max < i) max = i;
		}
		return max;		
	}
	
	public static double mean(List<Tuple2<Integer, Integer>> list) {
		double mean = 0;
		double size = 0;
		Iterator<Tuple2<Integer, Integer>> iter = list.iterator(); 
		Integer i;
		while (iter.hasNext()) {
			mean += iter.next()._2;	
			size++;
		}
		
		return mean / size;		
	}
	
	public static double std(List<Tuple2<Integer, Integer>> list) {
		double mean = mean(list);
		double std = 0;
		double size = 0;
		Iterator<Tuple2<Integer, Integer>> iter = list.iterator(); 
		while (iter.hasNext()) {
			std += Math.pow(iter.next()._2 - mean, 2);
			size++;
		}
		
		return Math.sqrt(std / size);		
	}
	
	public static void main(String[] args) {
		ArrayList<Tuple2<Integer, Integer>> a = new ArrayList<Tuple2<Integer, Integer>>();
		a.add(new Tuple2<Integer, Integer>(1, 10));
		a.add(new Tuple2<Integer, Integer>(10, -1));
		a.add(new Tuple2<Integer, Integer>(4, 3));
		
		System.out.println(min(a));
		System.out.println(max(a));
		System.out.println(mean(a));
		System.out.println(std(a));
	}
}
