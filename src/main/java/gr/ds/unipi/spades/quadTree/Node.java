package gr.ds.unipi.spades.quadTree;

import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.geometry.Rectangle;

public class Node extends Rectangle {	 
    private int id;
    private static int idCounter = 0;
    private int numberOfContainedPoints;
    private boolean hasChildrenQuadrants = false;
    private Point[] points;
    private int numberOfAssignedPoints = 0;
	
    public Node() {
    	id = newId();
    }
	
    public Node(Node parent, double minX, double minY, double maxX, double maxY) {
    	super(minX, minY, maxX, maxY);
        this.parent = parent;
        id = newId();
    }

    public Node getParent() {
        return parent;
    }

    private Node parent;
    private Node[] children;

    public int getId() {
    	return id;
    }
    
    private int newId() {
    	return idCounter++;
    }
    
    public boolean hasChildrenQuadrants() {
        return hasChildrenQuadrants;
    }

    public int getNumberOfContainedPoints() {
        return numberOfContainedPoints;
    }
    
    public int getNumberOfAssignedPoints() {
    	return numberOfAssignedPoints;
    }
    
    public void increaseNumberOfAssignedPoints() {
    	numberOfAssignedPoints++;
    }

    

    public boolean isEmpty() throws IllegalArgumentException {
    	if (hasChildrenQuadrants) throw new IllegalArgumentException("Node is not leaf node.");
    	
    	return numberOfContainedPoints == 0;
    }
    
    public boolean isFull(int maxPoints) throws IllegalArgumentException {
    	if (hasChildrenQuadrants) throw new IllegalArgumentException("Node is not leaf node.");
    	
    	return numberOfContainedPoints == maxPoints;
    }
    
    public Node[] getChildren() {
    	return children;
    }
    
    public Node getTopLeftChildQuadrant() {
        return children[0];
    }

    public void increaseByOneNumberOfContainedPoints(){
        numberOfContainedPoints++;
    }

    public void setChildQuadrants(Node topLeftChildQuadrant, Node topRightChildQuadrant, Node bottomRightChildQuadrant, Node bottomLeftChildQuadrant){
    	children = new Node[4];
    	children[0] = topLeftChildQuadrant;
    	children[1] = topRightChildQuadrant;
    	children[2] = bottomRightChildQuadrant;
    	children[3] = bottomLeftChildQuadrant;
        hasChildrenQuadrants = true;
    }

    public Node getTopRightChildQuadrant() {
        return children[1];
    }

    public Node getBottomRightChildQuadrant() {
        return children[2];
    }

    public Node getBottomLeftChildQuadrant() {
        return children[3];
    }   

    public Point[] getPoints() {
        return points;
    }

    public void setPoints(Point[] points) {
        this.points = points;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Top Left");
        return "Node ["+ minX + ","+ minY +"], ["+ maxX +","+ maxY +"]" +" - has "+ getNumberOfContainedPoints() + " array:"+getPoints();

    }
    
    // Node - Rectangle intersection
    // https://stackoverflow.com/questions/306316/determine-if-two-rectangles-overlap-each-other
    public boolean intersects(double minX, double minY, double maxX, double maxY) {
    	return !(this.minX > maxX || this.maxX < minX || this.minX > maxY || this.maxY < minY);
    }    
    
    
    // Maybe this method is of no use anymore
    // Should check and remove in later commits
    @Override
    public boolean equals(Object obj) {
    	if (obj.getClass() == this.getClass()) {
    		return ((Node) obj).hashCode() == this.hashCode();
    	}
    	
    	return false;
    }
    
    @Override
    public int hashCode() {
        return id;
    }
}
