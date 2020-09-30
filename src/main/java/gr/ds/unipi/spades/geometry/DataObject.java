package gr.ds.unipi.spades.geometry;

import java.util.Comparator;

public class DataObject extends Point {   
	
	public DataObject() {}
	
    public DataObject(double x, double y, int tag) {
        super(x, y, tag);
    }
    
    // A custom comparator that sorts objects such that data objects come before feature objects
    public static Comparator<Point> Comparator = new Comparator<Point>() {		
		@Override
		public int compare(Point point1, Point point2) {
			if (point1 == null) return -1;
			if (point2 == null) return 1;
			if (point1 instanceof DataObject && point2 instanceof DataObject) {
				return ((DataObject) point1).getTag() - ((DataObject) point2).getTag();
			}
			
			throw new IllegalArgumentException("Wrong object types provided.");
		}
	};
}
