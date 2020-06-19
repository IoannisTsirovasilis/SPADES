package gr.ds.unipi.spades.geometry;

import java.util.Comparator;

public class DataObject extends Point implements Comparator<Point> {   
	private int tag;
	
	public DataObject() {}
	
    public DataObject(double x, double y, int tag) {
        super(x, y);
        this.tag = tag;
    }  
    
    public int getTag() {
    	return tag;
    }

	@Override
	public int compare(Point point1, Point point2) {
		if (point1 instanceof DataObject && point2 instanceof DataObject) {
			return ((DataObject) point1).getTag() - ((DataObject) point2).getTag();
		}
		
		throw new IllegalArgumentException("Wrong object types provided.");
	}
}
