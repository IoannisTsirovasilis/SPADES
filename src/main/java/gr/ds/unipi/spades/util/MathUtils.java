package gr.ds.unipi.spades.util;

import java.util.HashSet;
import java.util.Set;

import gr.ds.unipi.spades.geometry.Point;


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
	
	// https://gist.github.com/vananth22/888ed9a22105670e7a4092bdcf0d72e4
	public static double haversineDistance(Point p1, Point p2) {		 
		double latDistance = Math.toRadians(p2.getY()- p1.getY());
		double lonDistance = Math.toRadians(p2.getX() - p1.getX());
		double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + 
				Math.cos(Math.toRadians(p1.getY())) * Math.cos(Math.toRadians(p2.getY())) *
				Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
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
}
