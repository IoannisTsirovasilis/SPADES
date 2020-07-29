package gr.ds.unipi.spades.util;

import java.util.HashSet;
import java.util.Set;

import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.geometry.Rectangle;


public class MathUtils {
	private static final double EARTH_RADIUS = 6371;
	
	// https://stackoverflow.com/questions/7222382/get-lat-long-given-current-point-distance-and-bearing
	public static Point getPointInDistanceAndBearing(Point point, double distance, double bearing) {
		double yRad = Math.toRadians(point.getY());
		double xRad = Math.toRadians(point.getX());
		bearing = Math.toRadians(bearing);
		
		double y = Math.asin(Math.sin(yRad) * Math.cos(distance / EARTH_RADIUS) 
				+ Math.cos(yRad) * Math.sin(distance / EARTH_RADIUS) * Math.cos(bearing));
		
		double x = xRad + Math.atan2(Math.sin(bearing) * 
				Math.sin(distance / EARTH_RADIUS) * Math.cos(yRad), 
				Math.cos(distance / EARTH_RADIUS) - Math.sin(yRad) * Math.sin(y));
		
		y = Math.toDegrees(y);
		x = Math.toDegrees(x);
		
		return new Point(x, y);	
    }
	
	public static double getXInDistanceOnEquator(double x, double distance) {
		return getPointInDistanceAndBearing(new Point(x, 0), distance, 90).getX();
	}
	
	
	public static double haversineDistance(Point p1, Point p2) {		 
		return haversineDistance(p1.getX(), p2.getX(), p1.getY(), p2.getY());
	}
	
	// https://gist.github.com/vananth22/888ed9a22105670e7a4092bdcf0d72e4
	public static double haversineDistance(double x1, double x2, double y1, double y2) {	
		double dy = Math.toRadians(y2 - y1);
		double dx = Math.toRadians(x2 - x1);
		double a = Math.sin(dy / 2) * Math.sin(dy / 2) + 
				Math.cos(Math.toRadians(y1)) * Math.cos(Math.toRadians(y2)) *
				Math.sin(dx / 2) * Math.sin(dx / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double distance = EARTH_RADIUS * c;
		
		return distance;
	}
	
	// https://stackoverflow.com/questions/43634867/computing-jaccard-similarity-in-java
	public static double jaccardSimilarity(String[] a, String[] b) {

	    Set<String> s1 = new HashSet<String>();
	    for (int i = 0; i < a.length; i++) {
	        s1.add(a[i]);
	    }
	    Set<String> s2 = new HashSet<String>();
	    for (int i = 0; i < b.length; i++) {
	        s2.add(b[i]);
	    }

	    final int sa = s1.size();
	    final int sb = s2.size();
	    s1.retainAll(s2);
	    final int intersection = s1.size();
	    return 1d / (sa + sb - intersection) * intersection;
	}
	
	// 0            1             2
	//   ------------------------
	//   |						| 
	// 7 |						| 3
	//	 |						|
	//	 ------------------------
	// 6            5             4                
	public static double pointToRectangleDistance(Point point, Rectangle rect) {
		double dx = Math.max(rect.getMinX() - point.getX(), 0);
		double dy = Math.max(rect.getMinY() - point.getY(), 0);
		
		if (dx > 0) {
			// case 6
			if (dy > 0) {
				return haversineDistance(point.getX(), rect.getMinX(), point.getY(), rect.getMinY());
			}
			
			dy = Math.max(dy, point.getY() - rect.getMaxY());
			
			// case 7
			if (dy == 0) {
				return haversineDistance(point.getX(), rect.getMinX(), point.getY(), point.getY());
			}
			
			// case 0
			return haversineDistance(point.getX(), rect.getMinX(), point.getY(), rect.getMaxY());
		} 
		
		dx = Math.max(dx, point.getX() - rect.getMaxX());
		
		if (dx == 0) {
			// case 5
			if (dy > 0) {
				return haversineDistance(point.getX(), point.getX(), point.getY(), rect.getMinY());
			}
						
			// case 1
			return haversineDistance(point.getX(), point.getX(), point.getY(), rect.getMaxY());
		}
		
		// case 4
		if (dy > 0) {
			return haversineDistance(point.getX(), rect.getMaxX(), point.getY(), rect.getMinY());
		}
		
		dy = Math.max(dy, point.getY() - rect.getMaxY());
		
		// case 3
		if (dy == 0) {
			return haversineDistance(point.getX(), rect.getMaxX(), point.getY(), point.getY());
		}
		
		
		// case 2
		return haversineDistance(point.getX(), rect.getMaxX(), point.getY(), rect.getMaxY());
	}
	
	public static void main(String[] args) {
		System.out.println(haversineDistance(0, 0.009, 90, 90));
	}
}
