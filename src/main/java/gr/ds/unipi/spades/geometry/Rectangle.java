package gr.ds.unipi.spades.geometry;

import gr.ds.unipi.spades.util.MathUtils;

public class Rectangle {
	protected double minX, minY, maxX, maxY;
	
	public Rectangle() {}
	
	public Rectangle(double minX, double minY, double maxX, double maxY) {
		this.minX = minX;
		this.minY = minY;
		this.maxX = maxX;
		this.maxY = maxY;
	}
	
	public double getMinX() {
		return minX;
	}
	
	public double getMinY() {
		return minY;
	}
	
	public double getMaxX() {
		return maxX;
	}
	
	public double getMaxY() {
		return maxY;
	}
	
	// https://stackoverflow.com/questions/5254838/calculating-distance-between-a-point-and-a-rectangular-box-nearest-point
	public boolean isInDistance(Point point, double radius) {
		return MathUtils.pointToRectangleDistance(point, this) <= radius;
	}
	
	public boolean contains(double x, double y) {
		return minX <= x && minY <= y 
				&& maxX  >= x && maxY >= y;
	}
	
	public boolean contains(Point p) {
		return minX <= p.getX() && minY <= p.getY()
				&& maxX  >= p.getX() && maxY >= p.getY();
	}
	
	public boolean equals(Rectangle rect) {		
		if (minX != rect.minX || minY != rect.minY) {
			return false;
		}
		
		if (maxX != rect.maxX || maxY != rect.maxY) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		return String.format("%f, %f, %f, %f", minX, minY, maxX, maxY);
	}
}
