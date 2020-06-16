package gr.ds.unipi.spades.quadTree;

import gr.ds.unipi.spades.geometry.Point;

public class Node {	  
	private double upperBoundx;
    private double upperBoundy;
    private double lowerBoundx;
    private double lowerBoundy;
    private int id;
    private static int idCounter = 0;
    private int numberOfContainedPoints;
    private boolean hasChildrenQuadrants = false;
    private Point[] points;
	
	public Node() {}
	
    public Node(Node parent, double lowerBoundx, double lowerBoundy, double upperBoundx, double upperBoundy) {
        this.parent = parent;
        this.upperBoundx = upperBoundx;
        this.upperBoundy = upperBoundy;
        this.lowerBoundx = lowerBoundx;
        this.lowerBoundy = lowerBoundy;
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

    public double getUpperBoundx() {
        return upperBoundx;
    }

    public double getUpperBoundy() {
        return upperBoundy;
    }

    public double getLowerBoundx() {
        return lowerBoundx;
    }

    public double getLowerBoundy() {
        return lowerBoundy;
    }

    public boolean intersects(Point point){
        if(((Double.compare(lowerBoundx, point.getX()) != 1) && (Double.compare(upperBoundx, point.getX()) == 1)) &&
                ((Double.compare(lowerBoundy, point.getY()) != 1) && (Double.compare(upperBoundy, point.getY()) == 1))){
            return true;
        }
        return false;
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
        return "Node ["+lowerBoundx + ","+lowerBoundy +"], ["+upperBoundx +","+upperBoundy +"]" +" - has "+ getNumberOfContainedPoints() + " array:"+getPoints();

    }
    
    // Node - Rectangle intersection
    // https://stackoverflow.com/questions/306316/determine-if-two-rectangles-overlap-each-other
    public boolean intersects(double minX, double minY, double maxX, double maxY) {
    	return !(lowerBoundx > maxX || upperBoundx < minX || lowerBoundy > maxY || upperBoundy < minY);
    }    
    
    
    // Maybe this method is of no use anymore
    // Should check and remove in later commits
    @Override
    public boolean equals(Object obj) {
    	if (obj.getClass() == this.getClass()) {
    		return ((Node) obj).getId() == this.getId();
    	}
    	
    	return false;
    }
}
