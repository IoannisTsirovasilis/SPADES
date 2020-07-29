package gr.ds.unipi.spades.regularGrid;

import gr.ds.unipi.spades.geometry.Rectangle;

public class Cell extends Rectangle {
	private int id;
	
	public Cell(int id) {
		this.id = id;
	}
	
	public Cell(double minX, double minY, double maxX, double maxY, int id) {
		super(minX, minY, maxX, maxY);
		this.id = id;
	}
	
	public int getId() {
		return id;
	}
	
	@Override
    public boolean equals(Object obj) {
    	if (obj.getClass() == this.getClass()) {
    		return ((Cell) obj).hashCode() == this.hashCode();
    	}
    	
    	return false;
    }
    
    @Override
    public int hashCode() {
        return id;
    }
}
