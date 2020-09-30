package gr.ds.unipi.spades.geometry;

import java.util.Comparator;

public class Point {
	private double  x, y;
	private int tag;
    
	public Point() {
		x = 0;
		y = 0;
	}
	
    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }  
    
    public Point(double x, double y, int tag) {
        this.x = x;
        this.y = y;
        this.tag = tag;
    }  
    
    public int getTag() {
    	return tag;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }
    
    public void setTag(int tag) {
    	this.tag = tag;
    }
    
    public void setX(double x) {
    	this.x = x;
    }
    
    public void setY(double y) {
    	this.x = y;
    }
    
    @Override
    public String toString() {    	
		return "(" + x + ", " + y + ")"; 
    }    
    
    public boolean equals(Point point) {
    	return x == point.x && y == point.y;
    }
    
    public static Comparator<Point> XComparator = new Comparator<Point>() {		
		@Override
		public int compare(Point p1, Point p2) {
			if (p1.x - p2.x < 0) return -1;
			else if (p1.x - p2.x > 0) return 1;
			return 0;
		}
	};
	
	public static Comparator<Point> YComparator = new Comparator<Point>() {		
		@Override
		public int compare(Point p1, Point p2) {
			if (p1.y - p2.y < 0) return -1;
			else if (p1.y - p2.y > 0) return 1;
			return 0;
		}
	};
}
