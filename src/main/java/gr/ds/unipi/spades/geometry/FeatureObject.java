package gr.ds.unipi.spades.geometry;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import scala.Tuple2;

public class FeatureObject extends DataObject {
	private double squareLowerX, squareLowerY, squareUpperX, squareUpperY;
	private String[] keywords;
	private double textualRelevanceScore;
    
    public FeatureObject(double x, double y, int tag, String[] keywords, double textualRelevanceScore) {
        super(x, y, tag);
        this.keywords = keywords;
        this.textualRelevanceScore = textualRelevanceScore;
    }  
    
    public String[] getKeywords() {
    	return keywords;
    }
    
    public void setTextualRelevanceScore(double textualRelevanceScore) {
    	this.textualRelevanceScore = textualRelevanceScore;
    }
    
    public void setSquare(double squareLowerX, double squareLowerY, double squareUpperX, double squareUpperY) {
    	this.squareLowerX = squareLowerX;
    	this.squareLowerY = squareLowerY;
    	this.squareUpperX = squareUpperX;
    	this.squareUpperY = squareUpperY;
    }
    
    public double getTextualRelevanceScore() {
    	return textualRelevanceScore;
    }
    
    public double getSquareLowerX() {
    	return squareLowerX;
    }
    
    public double getSquareLowerY() {
    	return squareLowerY;
    }
    
    public double getSquareUpperX() {
    	return squareUpperX;
    }
    
    public double getSquareUpperY() {
    	return squareUpperY;
    }
    
    public boolean hasCommonKeywords(String[] keywords) {
    	return !Collections.disjoint(Arrays.asList(this.keywords), Arrays.asList(keywords));
    }
    
    @Override
    public String toString() {
    	if (keywords == null) {
    		return super.toString(); 
    	}
    	
    	return super.toString() + " Keywords = " + keywords[0]; 
    }    
    
    // Custom comparator for sorting feature object in a descending manner based on textual relevance score
    public static Comparator<Point> Comparator = new Comparator<Point>() {		
		@Override
		public int compare(Point point1, Point point2) {
			if (point1.getClass() == DataObject.class && point2.getClass() == DataObject.class) {
				return 0;
			}
			
			if (point1.getClass() == DataObject.class) return -1;
			if (point2.getClass() == DataObject.class) return 1;
			
			FeatureObject f1 = (FeatureObject) point1;
			FeatureObject f2 = (FeatureObject) point2;
			
			double diff = f1.getTextualRelevanceScore() - f2.getTextualRelevanceScore();
			
			if (diff > 0) return -1;
			else if (diff < 0) return 1;
			else if (diff == 0) return 0;
			
			throw new IllegalArgumentException("Wrong object types provided.");
		}
	};
	
	// Custom comparator for sorting feature object in a descending manner based on textual relevance score
    public static Comparator<Tuple2<Point, Point>> PairComparator = new Comparator<Tuple2<Point, Point>>() {		
		@Override
		public int compare(Tuple2<Point, Point> pair1, Tuple2<Point, Point> pair2) {
			double diff = ((FeatureObject) pair1._2).getTextualRelevanceScore() - ((FeatureObject) pair2._2).getTextualRelevanceScore();
			if (diff > 0) return -1;
			else if (diff < 0) return 1;
			return 0;
		}
	};
}
