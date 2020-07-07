package gr.ds.unipi.spades.geometry;

import java.util.Arrays;
import java.util.Collections;

public class FeatureObject extends DataObject {
	private double squareLowerX, squareLowerY, squareUpperX, squareUpperY;
	private String[] keywords;
    
    public FeatureObject(double x, double y, int tag, String[] keywords) {
        super(x, y, tag);
        this.keywords = keywords;
    }  
    
    public String[] getKeywords() {
    	return keywords;
    }
    
    public void setSquare(double squareLowerX, double squareLowerY, double squareUpperX, double squareUpperY) {
    	this.squareLowerX = squareLowerX;
    	this.squareLowerY = squareLowerY;
    	this.squareUpperX = squareUpperX;
    	this.squareUpperY = squareUpperY;
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
}
