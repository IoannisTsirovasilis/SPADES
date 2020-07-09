package gr.ds.unipi.spades;

import java.util.ArrayList;

import gr.ds.unipi.spades.geometry.Cell;
import gr.ds.unipi.spades.geometry.DataObject;
import gr.ds.unipi.spades.geometry.FeatureObject;
import gr.ds.unipi.spades.geometry.Point;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import scala.Tuple2;

public class FeatureObjectTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public FeatureObjectTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( FeatureObjectTest.class );
    }
    
	public void testComparatorResultBetweenFeatureObjects() {
		String[] k = {"italian"};
    	FeatureObject f1 = new FeatureObject(0, 0, 2, k, 0.8);
    	FeatureObject f2 = new FeatureObject(0, 0, 2, k, 0.9);
    	FeatureObject f3 = new FeatureObject(0, 0, 2, k, 0.3);
    	FeatureObject f4 = new FeatureObject(0, 0, 2, k, 0.5);
    	
    	ArrayList<Point> p = new ArrayList<Point>();
    	p.add(f1);
    	p.add(f2);
    	p.add(f3);
    	p.add(f4);
    	
    	ArrayList<Point> pSorted = new ArrayList<Point>();
    	
    	pSorted.add(f2);
    	pSorted.add(f1);
    	pSorted.add(f4);
    	pSorted.add(f3);
    	
    	p.sort(FeatureObject.Comparator);
    	
    	for (int i = 0; i < p.size(); i++) {
    		assertEquals(((FeatureObject) pSorted.get(i)).getTextualRelevanceScore(), ((FeatureObject) p.get(i)).getTextualRelevanceScore()); 
    	}   	
    }
	
	public void testPairComparator() {
		String[] k = new String[]{"italian"};
		Tuple2<Point, Point> p1 = new Tuple2<Point, Point>(new DataObject(0, 0, 1), new FeatureObject(0, 0, 2, k, 0.8));
		Tuple2<Point, Point> p2 = new Tuple2<Point, Point>(new DataObject(0, 0, 1), new FeatureObject(0, 0, 2, k, 0.6));
		Tuple2<Point, Point> p3 = new Tuple2<Point, Point>(new DataObject(0, 0, 1), new FeatureObject(0, 0, 2, k, 0.9));
		Tuple2<Point, Point> p4 = new Tuple2<Point, Point>(new DataObject(0, 0, 1), new FeatureObject(0, 0, 2, k, 0.7));
		
		ArrayList<Tuple2<Point, Point>> pairs = new ArrayList<Tuple2<Point, Point>>();
		pairs.add(p1);
		pairs.add(p2);
		pairs.add(p3);
		pairs.add(p4);
		
		ArrayList<Tuple2<Point, Point>> pairsSorted = new ArrayList<Tuple2<Point, Point>>();
		pairsSorted.add(p3);
		pairsSorted.add(p1);
		pairsSorted.add(p4);
		pairsSorted.add(p2);
		
		pairs.sort(FeatureObject.PairComparator);
		
		for (int i = 0; i < pairs.size(); i++) {
			assertEquals(((FeatureObject) pairs.get(i)._2).getTextualRelevanceScore(), ((FeatureObject) pairsSorted.get(i)._2).getTextualRelevanceScore());
		}
	}
	
	public void testComparatorResultBetweenDataAndFeatureObjects() {
		String[] k = {"italian"};
		DataObject o1 = new DataObject(-1, 0, 1);
		DataObject o2 = new DataObject(0, 1, 1);
		DataObject o3 = new DataObject(2, 0, 1);
		DataObject o4 = new DataObject(0, -2, 1);
    	FeatureObject f1 = new FeatureObject(3, 0, 2, k, 0.8);
    	FeatureObject f2 = new FeatureObject(0, -3, 2, k, 0.9);
    	FeatureObject f3 = new FeatureObject(4, 0, 2, k, 0.3);
    	FeatureObject f4 = new FeatureObject(0, -4, 2, k, 0.5);
    	
    	ArrayList<Point> p = new ArrayList<Point>();
    	p.add(f1);
    	p.add(o1);
    	p.add(f2);
    	p.add(o2);
    	p.add(o3);
    	p.add(f3);
    	p.add(f4);
    	p.add(o4);
    	
    	ArrayList<Point> pSorted = new ArrayList<Point>();
    	pSorted.add(o1);
    	pSorted.add(o2);
    	pSorted.add(o3);
    	pSorted.add(o4);
    	pSorted.add(f2);
    	pSorted.add(f1);
    	pSorted.add(f4);
    	pSorted.add(f3);
    	
    	p.sort(FeatureObject.Comparator);
    	
    	for (int i = 0; i < p.size(); i++) {
    		assertEquals(true, p.get(i).equals(pSorted.get(i))); 
    	}   	
    }
}
