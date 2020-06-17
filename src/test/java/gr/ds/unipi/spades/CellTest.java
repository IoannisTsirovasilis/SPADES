package gr.ds.unipi.spades;

import gr.ds.unipi.spades.geometry.Cell;
import gr.ds.unipi.spades.geometry.Point;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class CellTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public CellTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( CellTest.class );
    }
    
	public void testIsInDistanceTrue() {
    	Cell cell = new Cell(0, 0, 1, 1, -100);
    	Point sample = new Point(1, 1.5);
    	assertEquals(true, cell.isInDistance(sample, 55.6));
    }
    
    public void testIsInDistanceFalse() {
    	Cell cell = new Cell(0, 0, 1, 1, -100);
    	Point sample = new Point(2.1, 1.5);
    	assertEquals(false, cell.isInDistance(sample, 1));
    	
    	
    }
}
