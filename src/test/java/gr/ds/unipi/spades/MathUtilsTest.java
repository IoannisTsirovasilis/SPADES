package gr.ds.unipi.spades;

import gr.ds.unipi.spades.geometry.Cell;
import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.util.MathUtils;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MathUtilsTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public MathUtilsTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( MathUtilsTest.class );
    }
    
    public void testHaversineDistance() {
    	double lat1 = 0;
    	double lat2 = 2;
    	double lon1 = 1;
    	double lon2 = -1;
    	double d = MathUtils.haversineDistance(lat1, lat2, lon1, lon2);
    	assertEquals(314.5, d, 0.1);
    }
    
    public void testPointToCellDistanceCase0() {
    	Point point = new Point(-1, 2);
    	Cell cell = new Cell(0, 0, 1, 1, -100);
    	double d = MathUtils.pointToCellDistance(point, cell);
    	assertEquals(157.2, d, 0.1);
    }
    
    public void testPointToCellDistanceCase1() {
    	Point point = new Point(0.5, 3);
    	Cell cell = new Cell(0, 0, 1, 1, -100);
    	double d = MathUtils.pointToCellDistance(point, cell);
    	assertEquals(222.4, d, 0.1);
    }
    
    public void testPointToCellDistanceCase2() {
    	Point point = new Point(3, 3);
    	Cell cell = new Cell(0, 0, 1, 1, -100);
    	double d = MathUtils.pointToCellDistance(point, cell);
    	assertEquals(314.4, d, 0.1);
    }
    
    public void testPointToCellDistanceCase3() {
    	Point point = new Point(5, 0.7);
    	Cell cell = new Cell(0, 0, 1, 1, -100);
    	double d = MathUtils.pointToCellDistance(point, cell);
    	assertEquals(444.7, d, 0.1);
    }
    
    public void testPointToCellDistanceCase4() {
    	Point point = new Point(5, -5);
    	Cell cell = new Cell(0, 0, 1, 1, -100);
    	double d = MathUtils.pointToCellDistance(point, cell);
    	assertEquals(711.3, d, 0.1);
    }
    
    public void testPointToCellDistanceCase5() {
    	Point point = new Point(0.2, -3);
    	Cell cell = new Cell(0, 0, 1, 1, -100);
    	double d = MathUtils.pointToCellDistance(point, cell);
    	assertEquals(333.6, d, 0.1);
    }
    
    public void testPointToCellDistanceCase6() {
    	Point point = new Point(-3, -3);
    	Cell cell = new Cell(0, 0, 1, 1, -100);
    	double d = MathUtils.pointToCellDistance(point, cell);
    	assertEquals(471.7, d, 0.1);
    }
    
    public void testPointToCellDistanceCase7() {
    	Point point = new Point(-3, 1);
    	Cell cell = new Cell(0, 0, 1, 1, -100);
    	double d = MathUtils.pointToCellDistance(point, cell);
    	assertEquals(333.5, d, 0.1);
    }
}