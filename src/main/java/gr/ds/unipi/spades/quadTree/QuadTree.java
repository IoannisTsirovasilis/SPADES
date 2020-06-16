package gr.ds.unipi.spades.quadTree;

import java.util.ArrayList;

import gr.ds.unipi.spades.geometry.FeatureObject;
import gr.ds.unipi.spades.geometry.Point;
import scala.Tuple2;

public class QuadTree {

    private Node root;
    private int MAX_NUMBER_OF_POINTS_IN_LEAVES;
    public static int o = 0;
    private int numberOfLeaves = 1;
    private int numberOfEmptyLeaves = 0;
    private int numberOfFullLeaves = 0;
    private int standardDeviation = 0;

    public QuadTree() {}
    
    public QuadTree(double lowerBoundx, double lowerBoundy, double upperBoundx, double upperBoundy, int maxNumberOfPointsInLeaves) {
    	root = new Node(null, lowerBoundx, lowerBoundy, upperBoundx, upperBoundy);
    	MAX_NUMBER_OF_POINTS_IN_LEAVES = maxNumberOfPointsInLeaves;
    }
    
    public QuadTree(Node root, int maxNumberOfPointsInLeaves) {
        this.root = root;
        this.MAX_NUMBER_OF_POINTS_IN_LEAVES = maxNumberOfPointsInLeaves;
    }
    
    public Node getRoot() {
    	return root;
    }
    
    public double getStandardDeviationNonEmptyLeaves() {
    	return Math.sqrt(standardDeviation / (numberOfLeaves - numberOfEmptyLeaves));
    }
    
    public double getMeanNumberOfPointsPerNonEmptyLeaf( ) {
    	return (double) getNumberOfInsertedPoints() / (numberOfLeaves - numberOfEmptyLeaves);
    }
    
    public int getNumberOfLeaves() {
    	return numberOfLeaves;
    }
    
    public int getNumberOfEmptyLeaves() {
    	return numberOfEmptyLeaves;
    }
    
    public int getNumberOfFullLeaves() {
    	return numberOfFullLeaves;
    }

    public void insertPoint(Point point) {
        insertPoint(root, point);
    }  
    
    private void insertPoint(Node node, Point point) {

        Node leafNode = determineLeafNodeForInsertion(node, point);

        if (leafNode.getNumberOfContainedPoints() == MAX_NUMBER_OF_POINTS_IN_LEAVES) {
            createQuadrants(leafNode);
            o++;
            numberOfLeaves += 3;
            disseminatePointsToQuadrants(leafNode);
            insertPoint(leafNode, point);
        } else {
            addPointToNode(leafNode, point);
        }
    }
    
    // Recursive method for assigning a point to a leaf and 
    // to all the leaves that intersect with the square around the point
    public ArrayList<Tuple2<Integer, Point>> assignToLeafNodeAndDuplicate(Node node, FeatureObject featureObject) {
    	
    	// Array list containing the pairs (leaf id, point)
    	ArrayList<Tuple2<Integer, Point>> pairs = new ArrayList<Tuple2<Integer, Point>>();
    	
    	// If it's NOT a leaf node, then explore children
    	if (node.hasChildrenQuadrants()) {
    		
    		// for each child node
    		for (Node child : node.getChildren()) {    			
    			// if child intersects with square
    			if (child.intersects(featureObject.getSquareLowerX(), featureObject.getSquareLowerY(), featureObject.getSquareUpperX(), featureObject.getSquareUpperY())) {
    				
    				// Recursively explore its children
                    pairs.addAll(assignToLeafNodeAndDuplicate(child, featureObject));
                }
    		}
    	// else if it is a leaf node
        } else {
        	// assign point to this leaf
        	pairs.add(new Tuple2<Integer, Point>(node.getId(), featureObject));
        }
    	
    	return pairs;
    }
    
    // Recursive method for assigning a point to a leaf
    // This method could simply return a Tuple2<Integer, Point>, 
    // but Sparks' flatMapToPair demands an iterator
    public ArrayList<Tuple2<Integer, Point>> assignToLeafNodeIterator(Node node, Point point) {
    	if (node.hasChildrenQuadrants()) {
    		for (Node child : node.getChildren()) {
    			if (child.intersects(point)) {
                    return assignToLeafNodeIterator(child, point);
                }
    		}
        } 
    	
    	ArrayList<Tuple2<Integer, Point>> pair = new ArrayList<Tuple2<Integer, Point>>();
    	pair.add(new Tuple2<Integer, Point>(node.getId(), point));
    	return pair;
    }
    
    public Tuple2<Integer, Point> assignToLeafNode(Node node, Point point) {
    	if (node.hasChildrenQuadrants()) {
    		for (Node child : node.getChildren()) {
    			if (child.intersects(point)) {
                    node.increaseByOneNumberOfContainedPoints();
                    return assignToLeafNode(child, point);
                }
    		}
        } 
    	
    	return new Tuple2<Integer, Point>(node.getId(), point);
    }

    public Node determineLeafNodeForInsertion(Node node, Point point) {

        if (node.hasChildrenQuadrants()) {
            if (node.getTopLeftChildQuadrant().intersects(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return determineLeafNodeForInsertion(node.getTopLeftChildQuadrant(), point);
            } else if (node.getTopRightChildQuadrant().intersects(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return determineLeafNodeForInsertion(node.getTopRightChildQuadrant(), point);
            } else if (node.getBottomRightChildQuadrant().intersects(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return determineLeafNodeForInsertion(node.getBottomRightChildQuadrant(), point);
            } else if (node.getBottomLeftChildQuadrant().intersects(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return determineLeafNodeForInsertion(node.getBottomLeftChildQuadrant(), point);
            } else {
                try {
                    throw new Exception("Error1");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return node;
    }
    
    public int getLeafNodeIdForInsertion(Node node, Point point) {

        if (node.hasChildrenQuadrants()) {
            if (node.getTopLeftChildQuadrant().intersects(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return getLeafNodeIdForInsertion(node.getTopLeftChildQuadrant(), point);
            } else if (node.getTopRightChildQuadrant().intersects(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return getLeafNodeIdForInsertion(node.getTopRightChildQuadrant(), point);
            } else if (node.getBottomRightChildQuadrant().intersects(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return getLeafNodeIdForInsertion(node.getBottomRightChildQuadrant(), point);
            } else if (node.getBottomLeftChildQuadrant().intersects(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return getLeafNodeIdForInsertion(node.getBottomLeftChildQuadrant(), point);
            } else {
                try {
                    throw new Exception("Error1");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return node.getId();
    }


    private void createQuadrants(Node node) {

        node.setChildQuadrants(new Node(node, node.getLowerBoundx(), (node.getUpperBoundy() + node.getLowerBoundy()) / 2, (node.getUpperBoundx() + node.getLowerBoundx()) / 2, node.getUpperBoundy()),

        		new Node(node, (node.getUpperBoundx() + node.getLowerBoundx()) / 2, (node.getUpperBoundy() + node.getLowerBoundy()) / 2, node.getUpperBoundx(), node.getUpperBoundy()),

        		new Node(node, (node.getUpperBoundx() + node.getLowerBoundx()) / 2, node.getLowerBoundy(), node.getUpperBoundx(), (node.getUpperBoundy() + node.getLowerBoundy()) / 2),

        		new Node(node, node.getLowerBoundx(), node.getLowerBoundy(), (node.getUpperBoundx() + node.getLowerBoundx()) / 2, (node.getUpperBoundy() + node.getLowerBoundy()) / 2));
    }

    private void disseminatePointsToQuadrants(Node node) {

        Point[] points = node.getPoints();

        for (int i = 0; i < points.length; i++) {

            if (node.getTopLeftChildQuadrant().intersects(points[i])) {
                addPointToNode(node.getTopLeftChildQuadrant(), points[i]);
                continue;
            }

            if (node.getTopRightChildQuadrant().intersects(points[i])) {
                addPointToNode(node.getTopRightChildQuadrant(), points[i]);
                continue;
            }

            if (node.getBottomRightChildQuadrant().intersects(points[i])) {
                addPointToNode(node.getBottomRightChildQuadrant(), points[i]);
                continue;
            }

            if (node.getBottomLeftChildQuadrant().intersects(points[i])) {
                addPointToNode(node.getBottomLeftChildQuadrant(), points[i]);
                continue;
            }

        }

        node.setPoints(null);

    }

    private void addPointToNode(Node node, Point point) {

        if (node.getPoints() == null) {
            node.setPoints(new Point[MAX_NUMBER_OF_POINTS_IN_LEAVES]);
        }

        node.getPoints()[node.getNumberOfContainedPoints()] = point;
        node.increaseByOneNumberOfContainedPoints();

    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        return sb.toString();

    }

    public int getNumberOfInsertedPoints() {
        return root.getNumberOfContainedPoints();
    }    
    
    public void traverse() {
    	traverse(root);
    }

    // get metrics by traversing the tree once after it is built
	private void traverse(Node node) {
		if (node == null) return;
		if (node.hasChildrenQuadrants()) {
			Node[] children = node.getChildren();
			for (int i = 0; i < children.length; i++) {
				traverse(children[i]);
			}
		} else {
			if (node.isEmpty()) {
				numberOfEmptyLeaves++;
				return;
			}
			
			if (node.isFull(MAX_NUMBER_OF_POINTS_IN_LEAVES)) {
				numberOfFullLeaves++;
			} 
			
			standardDeviation += Math.pow(node.getNumberOfContainedPoints() 
					- getMeanNumberOfPointsPerNonEmptyLeaf(), 2);
		}
	}

}
