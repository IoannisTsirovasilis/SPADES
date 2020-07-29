package gr.ds.unipi.spades;

import gr.ds.unipi.spades.regularGrid.RegularGrid;

import java.util.ArrayList;
import java.util.Comparator;

import gr.ds.unipi.spades.regularGrid.Cell;
import gr.ds.unipi.spades.geometry.Point;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import scala.Tuple2;

public class RegularGridTest extends TestCase implements Comparator<Tuple2<Integer, Point>>
{
	public RegularGridTest() {}
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public RegularGridTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( RegularGridTest.class );
    }
    
    double radius, minX, minY, maxX, maxY;
    int hSectors, vSectors;
    RegularGrid grid;
    
    public void setUp() {
    	radius = 15.72;
		minX = 0;
		minY = 0;
		maxX = 1;
		maxY = 1;
		hSectors = 3;
		vSectors = 3;
		
		grid = new RegularGrid(minX, minY, maxX, maxY, hSectors, vSectors);
    }
    
    public void testBuildGridCreatesProperGrid() {
    	int hSectors = 3;
    	int vSectors = 2;
    	RegularGrid gb = new RegularGrid(0, 0, 10, 6, hSectors, vSectors);
    	double hStep = 10.0 / hSectors;
    	double vStep = 6.0 / vSectors;
    	Point point00 = new Point(0, 0);
    	Point point01 = new Point(0 + hStep, 0);
    	Point point02 = new Point(0 + 2 * hStep, 0);
    	Point point10 = new Point(0, 0 + vStep);
    	Point point11 = new Point(0 + hStep, 0 + vStep);
    	Point point12 = new Point(0 + 2 * hStep, 0 + vStep);
    	Point point13 = new Point(0 + 3 * hStep, 0 + vStep);
    	Point point21 = new Point(0 + hStep, 0 + 2 * vStep);
    	Point point22 = new Point(0 + 2 * hStep, 0 + 2 * vStep);
    	Point point23 = new Point(0 + 3 * hStep, 0 + 2 * vStep);
    	
    	
    	
    	assertEquals(2, gb.getCells().length);
    	assertEquals(3, gb.getCells()[0].length);
    	
    	Cell[][] actualGridCells = new Cell[2][3];
    	actualGridCells[0][0] = new Cell(point00.getX(), point00.getY(), point11.getX(), point11.getY(), 0);
    	actualGridCells[0][1] = new Cell(point01.getX(), point01.getY(), point12.getX(), point12.getY(), 1);
    	actualGridCells[0][2] = new Cell(point02.getX(), point02.getY(), point13.getX(), point13.getY(), 2);
    	actualGridCells[1][0] = new Cell(point10.getX(), point10.getY(), point21.getX(), point21.getY(), 3);
    	actualGridCells[1][1] = new Cell(point11.getX(), point11.getY(), point22.getX(), point22.getY(), 4);
    	actualGridCells[1][2] = new Cell(point12.getX(), point12.getY(), point23.getX(), point23.getY(), 5);
    	
    	for (int i = 0; i < gb.getCells().length; i++) {
    		for (int j = 0; j < gb.getCells()[0].length; j ++) {
    			assertEquals(true, actualGridCells[i][j].equals(gb.getCells()[i][j]));
    		}
    	}
    }
    
    // Tests on 3x3 Grid    
    //  -------------------------------------------------------------
    //  |					|					|					|   
    //  |    	6			|		7			|		8			|					
    //  |					|					|					|
    //  -------------------------------------------------------------
    //  |					|					|					|   
    //  |    	3			|		4			|		5			|					
    //  |					|					|					|
    //  -------------------------------------------------------------
    //  |					|					|					|   
    //  |    	0			|		1			|		2			|					
    //  |					|					|					|
    //  -------------------------------------------------------------
    // The numbers represent the cell id. 
    // Cell[3][3] array is translated as follows:
    // [0][0] --> 0
    // [0][1] --> 1
    // [0][2] --> 2
    // [1][0] --> 3
    // ............
    // [2][1] --> 7
    // [2][2] --> 8
    
    
    public void testEdgeCaseRightBound() {    		
    	Point point = new Point(1, 0.15);
		
		ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();
		actualResults.add(new Tuple2<Integer, Point>(2, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    // Naming convention for the following methods:
    // testEdgeCase + <cell_id> + <position of point in cell with cell id "cell_id">
    // e.g testEdgeCase0UpperRight ---> test for a point that lies in cell with id 0 and 
    // is positioned near to the upper right corner of the cell.
    
    public void testEdgeCase0UpperRight() {    	
    	Point point = new Point(0.31, 0.31);		
    	
    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();
		actualResults.add(new Tuple2<Integer, Point>(0, point));
		actualResults.add(new Tuple2<Integer, Point>(1, point));
		actualResults.add(new Tuple2<Integer, Point>(3, point));
		actualResults.add(new Tuple2<Integer, Point>(4, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    
    
    public void testEdgeCase0Top() {
    	Point point = new Point(0.15, 0.31);		
    	
    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();
		actualResults.add(new Tuple2<Integer, Point>(0, point));
		actualResults.add(new Tuple2<Integer, Point>(3, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    public void testEdgeCase0Right() {
    	Point point = new Point(0.31, 0.15);		
    	
    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();
		actualResults.add(new Tuple2<Integer, Point>(0, point));
		actualResults.add(new Tuple2<Integer, Point>(1, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    public void testEdgeCase2UpperLeft() {
    	Point point = new Point(0.68, 0.31);		
    	
    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();
		actualResults.add(new Tuple2<Integer, Point>(1, point));
		actualResults.add(new Tuple2<Integer, Point>(2, point));
		actualResults.add(new Tuple2<Integer, Point>(4, point));
		actualResults.add(new Tuple2<Integer, Point>(5, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    public void testEdgeCase2Top() {    	
    	Point point = new Point(0.9, 0.31);		
    	
    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();
		actualResults.add(new Tuple2<Integer, Point>(2, point));
		actualResults.add(new Tuple2<Integer, Point>(5, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    public void testEdgeCase2Left() {  
    	Point point = new Point(0.7, 0.15);		

    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();
		actualResults.add(new Tuple2<Integer, Point>(1, point));
		actualResults.add(new Tuple2<Integer, Point>(2, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    public void testEdgeCase6Right() {    	
    	Point point = new Point(0.31, 0.9);		
    	
    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();
		actualResults.add(new Tuple2<Integer, Point>(6, point));
		actualResults.add(new Tuple2<Integer, Point>(7, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    public void testEdgeCase6Bottom() {    	
    	Point point = new Point(0.15, 0.68);		
    	
    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();
		actualResults.add(new Tuple2<Integer, Point>(3, point));
		actualResults.add(new Tuple2<Integer, Point>(6, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    public void testEdgeCase6BottomRight() {
    	Point point = new Point(0.31, 0.68);		
    	
    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();
		actualResults.add(new Tuple2<Integer, Point>(3, point));
		actualResults.add(new Tuple2<Integer, Point>(4, point));
		actualResults.add(new Tuple2<Integer, Point>(6, point));
		actualResults.add(new Tuple2<Integer, Point>(7, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    public void testEdgeCase8Bottom() {    	
    	Point point = new Point(0.9, 0.68);		
    	
    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();
		actualResults.add(new Tuple2<Integer, Point>(5, point));
		actualResults.add(new Tuple2<Integer, Point>(8, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    public void testEdgeCase8Left() {
    	Point point = new Point(0.68, 0.9);		
    	
    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();		
		actualResults.add(new Tuple2<Integer, Point>(7, point));
		actualResults.add(new Tuple2<Integer, Point>(8, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }
    
    public void testEdgeCase8BottomLeft() {    	
    	Point point = new Point(0.68, 0.68);		
    	
    	ArrayList<Tuple2<Integer, Point>> result = grid.assignToCellAndDuplicate(point, radius);
		result.sort(new RegularGridTest());
		ArrayList<Tuple2<Integer, Point>> actualResults = new ArrayList<Tuple2<Integer, Point>>();	
		actualResults.add(new Tuple2<Integer, Point>(4, point));
		actualResults.add(new Tuple2<Integer, Point>(5, point));
		actualResults.add(new Tuple2<Integer, Point>(7, point));
		actualResults.add(new Tuple2<Integer, Point>(8, point));
		
		assertEquals(actualResults.size(), result.size());
		for (int i = 0; i < actualResults.size(); i++) {
			assertEquals(actualResults.get(i)._1.intValue(), result.get(i)._1.intValue());
		}
    }

	@Override
	public int compare(Tuple2<Integer, Point> pair1, Tuple2<Integer, Point> pair2) {
		return pair1._1.intValue() - pair2._1.intValue(); 
	}
}
