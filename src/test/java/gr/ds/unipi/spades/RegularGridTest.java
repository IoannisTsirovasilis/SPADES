package gr.ds.unipi.spades;

import gr.ds.unipi.spades.regularGrid.RegularGrid;
import gr.ds.unipi.spades.geometry.Cell;
import gr.ds.unipi.spades.geometry.Point;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class RegularGridTest extends TestCase
{
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
}
