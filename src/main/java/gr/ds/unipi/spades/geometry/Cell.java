package gr.ds.unipi.spades.geometry;

import gr.ds.unipi.spades.util.MathUtils;

public class Cell {
	private double minX, minY, maxX, maxY;
	private int id;
	
	public Cell(double minX, double minY, double maxX, double maxY, int id) {
		this.minX = minX;
		this.minY = minY;
		this.maxX = maxX;
		this.maxY = maxY;
		this.id = id;
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
	
	public int getId() {
		return id;
	}
	
	// https://stackoverflow.com/questions/5254838/calculating-distance-between-a-point-and-a-rectangular-box-nearest-point
		public boolean isInDistance(Point point, double radius) {
			return MathUtils.pointToCellDistance(point, this) <= radius;
		}
	
	public boolean contains(double x, double y) {
		return minX <= x && minY <= y 
				&& maxX  >= x && maxY >= y;
	}
	
	public boolean contains(Point p) {
		return minX <= p.getX() && minY <= p.getY()
				&& maxX  >= p.getX() && maxY >= p.getY();
	}
	
	public boolean equals(Cell cell) {		
		if (minX != cell.minX || minY != cell.minY) {
			return false;
		}
		
		if (maxX != cell.maxX || maxY != cell.maxY) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		return String.format("%f, %f, %f, %f", minX, minY, maxX, maxY);
	}
}
