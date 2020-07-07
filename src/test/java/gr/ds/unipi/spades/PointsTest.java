package gr.ds.unipi.spades;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.ArrayList;

import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.util.Points;

public class PointsTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public PointsTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( PointsTest.class );
    }
    
    public void testSortX() {
    	Point p1 = new Point(1, 2);
    	Point p2 = new Point(-11, 2);
    	Point p3 = new Point(5, 2);
    	Point p4 = new Point(-2, 2);
    	
    	Point sp1 = new Point(-11, 2);
    	Point sp2 = new Point(-2, 2);
    	Point sp3 = new Point(1, 2);
    	Point sp4 = new Point(5, 2);
    	
    	Point[] a = {p1, p2, p3, p4};
    	
    	Point[] sorted = {sp1, sp2, sp3, sp4};
    	
    	Points.sort(a, Point.XComparator);
    	
    	for (int i = 0; i < a.length; i++) {
    		assertEquals(true, a[i].equals(sorted[i]));
    	}
    	
    }
    
    public void testSortArrayListX() {
    	Point p1 = new Point(1, 2);
    	Point p2 = new Point(-11, 2);
    	Point p3 = new Point(5, 2);
    	Point p4 = new Point(-2, 2);
    	
    	Point sp1 = new Point(-11, 2);
    	Point sp2 = new Point(-2, 2);
    	Point sp3 = new Point(1, 2);
    	Point sp4 = new Point(5, 2);
    	
    	ArrayList<Point> a = new ArrayList<Point>();
    	a.add(p1);
    	a.add(p2);
    	a.add(p3);
    	a.add(p4);
    	
    	Point[] sorted = {sp1, sp2, sp3, sp4};
    	
    	Points.sort(a, Point.XComparator);
    	
    	for (int i = 0; i < a.size(); i++) {
    		assertEquals(true, a.get(i).equals(sorted[i]));
    	}
    	
    }
    
    public void testSortY() {
    	Point p1 = new Point(1, 10);
    	Point p2 = new Point(-11, 5);
    	Point p3 = new Point(5, 2);
    	Point p4 = new Point(-2, 22);
    	
    	Point sp1 = new Point(5, 2);
    	Point sp2 = new Point(-11, 5);
    	Point sp3 = new Point(1, 10);
    	Point sp4 = new Point(-2, 22);
    	
    	Point[] a = {p1, p2, p3, p4};
    	
    	Point[] sorted = {sp1, sp2, sp3, sp4};
    	Points.sort(a, Point.YComparator);
    	
    	for (int i = 0; i < a.length; i++) {
    		assertEquals(true, a[i].equals(sorted[i]));
    	}    	
    }
}
