package gr.ds.unipi.spades.quadTree;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;

import gr.ds.unipi.spades.geometry.DataObject;
import gr.ds.unipi.spades.geometry.FeatureObject;
import gr.ds.unipi.spades.geometry.Point;
import gr.ds.unipi.spades.queries.Query;
import gr.ds.unipi.spades.util.MathUtils;
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
    
    public void insertPoint(Point point, double radius) {
        insertPoint(root, point, radius);
    }
    
    // Special case to prevent leaf creation with side length less than a given number (e.g. 2*r)
    private void insertPoint(Node node, Point point, double radius) {
        Node leafNode = determineLeafNodeForInsertion(node, point);
        
        if (leafNode.getNumberOfContainedPoints() > MAX_NUMBER_OF_POINTS_IN_LEAVES) {
        	addPointToNode(leafNode, point);
        	return;
        }
        
        if (leafNode.getNumberOfContainedPoints() == MAX_NUMBER_OF_POINTS_IN_LEAVES) {
        	if (MathUtils.haversineDistance(leafNode.getMinX(), (leafNode.getMaxX() + leafNode.getMinX()) / 2,
        			 leafNode.getMinY(), leafNode.getMinY()) <= 2 * radius) {
        		addPointToNode(leafNode, point);
        		return;
        	}
        	
        	if (MathUtils.haversineDistance(leafNode.getMinX(), leafNode.getMinX(),
       			 leafNode.getMinY(), (leafNode.getMaxY() + leafNode.getMinY()) / 2) <= 2 * radius) {
        		addPointToNode(leafNode, point);
        		return;
	       	}
        	
            createQuadrants(leafNode);
            o++;
            numberOfLeaves += 3;
            disseminatePointsToQuadrants(leafNode);
            insertPoint(leafNode, point, radius);
        } else {
            addPointToNode(leafNode, point);
        }
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
    
    public static ArrayList<Tuple2<Integer, Point>> empty() {
    	ArrayList<Tuple2<Integer, Point>> empty = new ArrayList<Tuple2<Integer, Point>>();
    	empty.add(new Tuple2<Integer, Point>(-1, null));
    	return empty;
    }
    
    // Recursive method for assigning a point to a leaf and 
    // to all the leaves that intersect with the square around the point
    public ArrayList<Tuple2<Integer, FeatureObject>> assignToLeafNodeAndDuplicate(Node node, FeatureObject featureObject) {
    	
    	// Array list containing the pairs (leaf id, point)
    	ArrayList<Tuple2<Integer, FeatureObject>> pairs = new ArrayList<Tuple2<Integer, FeatureObject>>();
    	
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
        	pairs.add(new Tuple2<Integer, FeatureObject>(node.getId(), featureObject));
        }
    	
    	return pairs;
    }
    
    // Used for LR Dataset duplication
    public ArrayList<Tuple2<Integer, FeatureObject>> duplicateToLeafNodes(Node node, int enclosingNodeId, boolean duplicateL,
    		FeatureObject featureObject) {
    	
    	// Array list containing the pairs (leaf id, point)
    	ArrayList<Tuple2<Integer, FeatureObject>> pairs = new ArrayList<Tuple2<Integer, FeatureObject>>();
    	
    	if (duplicateL) {
    		// If it's NOT a leaf node, then explore children
        	if (node.hasChildrenQuadrants()) {
        		
        		// for each child node
        		for (Node child : node.getChildren()) {    			
        			// if child intersects with square
        			if (child.intersects(featureObject.getSquareLowerX(), featureObject.getSquareLowerY(), featureObject.getSquareUpperX(), featureObject.getSquareUpperY())) {
        				
        				// Recursively explore its children
                        pairs.addAll(duplicateToLeafNodes(child, enclosingNodeId, duplicateL, featureObject));
                    }
        		}
        	// else if it is a leaf node
            } else {
            	// assign point to this leaf
            	pairs.add(new Tuple2<Integer, FeatureObject>(node.getId(), featureObject));
            }
    	} else {
    		// If it's NOT a leaf node, then explore children
        	if (node.hasChildrenQuadrants()) {
        		
        		// for each child node
        		for (Node child : node.getChildren()) {    			
        			// if child intersects with square
        			if (child.intersects(featureObject.getSquareLowerX(), featureObject.getSquareLowerY(), 
        					featureObject.getSquareUpperX(), featureObject.getSquareUpperY())) {
        				
        				// Recursively explore its children
                        pairs.addAll(duplicateToLeafNodes(child, enclosingNodeId, duplicateL, featureObject));
                    }
        		}
        	// else if it is a leaf node
            } else  {
            	// assign point to this leaf
            	pairs.add(new Tuple2<Integer, FeatureObject>(node.getId(), featureObject));
            }
    	}
    	
    	return pairs;
    }
    
    // Recursive method for assigning a point to a leaf
    // This method could simply return a Tuple2<Integer, Point>, 
    // but Sparks' flatMapToPair demands an iterator
    public ArrayList<Tuple2<Integer, FeatureObject>> assignToLeafNodeIterator(Node node, FeatureObject point) {
    	if (node.hasChildrenQuadrants()) {
    		for (Node child : node.getChildren()) {
    			if (child.contains(point)) {
                    return assignToLeafNodeIterator(child, point);
                }
    		}
        } 
    	
    	ArrayList<Tuple2<Integer, FeatureObject>> pair = new ArrayList<Tuple2<Integer, FeatureObject>>();
    	pair.add(new Tuple2<Integer, FeatureObject>(node.getId(), point));
    	return pair;
    }
    
    // Used for LR Dataset Duplication
    public Node getEnclosingNode(Node node, FeatureObject point) {
    	if (node.hasChildrenQuadrants()) {
    		for (Node child : node.getChildren()) {
    			if (child.contains(point)) {
                    return getEnclosingNode(child, point);
                }
    		}
        } 
    	
    	return node;
    }
    
    public Tuple2<Integer, Point> assignToLeafNode(Node node, Point point) {
    	if (node.hasChildrenQuadrants()) {
    		for (Node child : node.getChildren()) {
    			if (child.contains(point)) {
                    node.increaseByOneNumberOfContainedPoints();
                    return assignToLeafNode(child, point);
                }
    		}
        } 
    	
    	return new Tuple2<Integer, Point>(node.getId(), point);
    }

    public Node determineLeafNodeForInsertion(Node node, Point point) {

        if (node.hasChildrenQuadrants()) {
            if (node.getTopLeftChildQuadrant().contains(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return determineLeafNodeForInsertion(node.getTopLeftChildQuadrant(), point);
            } else if (node.getTopRightChildQuadrant().contains(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return determineLeafNodeForInsertion(node.getTopRightChildQuadrant(), point);
            } else if (node.getBottomRightChildQuadrant().contains(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return determineLeafNodeForInsertion(node.getBottomRightChildQuadrant(), point);
            } else if (node.getBottomLeftChildQuadrant().contains(point)) {
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
            if (node.getTopLeftChildQuadrant().contains(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return getLeafNodeIdForInsertion(node.getTopLeftChildQuadrant(), point);
            } else if (node.getTopRightChildQuadrant().contains(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return getLeafNodeIdForInsertion(node.getTopRightChildQuadrant(), point);
            } else if (node.getBottomRightChildQuadrant().contains(point)) {
                node.increaseByOneNumberOfContainedPoints();
                return getLeafNodeIdForInsertion(node.getBottomRightChildQuadrant(), point);
            } else if (node.getBottomLeftChildQuadrant().contains(point)) {
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

        node.setChildQuadrants(new Node(node, node.getMinX(), (node.getMaxY() + node.getMinY()) / 2, (node.getMaxX() + node.getMinX()) / 2, node.getMaxY()),

        		new Node(node, (node.getMaxX() + node.getMinX()) / 2, (node.getMaxY() + node.getMinY()) / 2, node.getMaxX(), node.getMaxY()),

        		new Node(node, (node.getMaxX() + node.getMinX()) / 2, node.getMinY(), node.getMaxX(), (node.getMaxY() + node.getMinY()) / 2),

        		new Node(node, node.getMinX(), node.getMinY(), (node.getMaxX() + node.getMinX()) / 2, (node.getMaxY() + node.getMinY()) / 2));
    }

    private void disseminatePointsToQuadrants(Node node) {

        ArrayList<Point> points = node.getPoints();

        for (int i = 0; i < points.size(); i++) {

            if (node.getTopLeftChildQuadrant().contains(points.get(i))) {
                addPointToNode(node.getTopLeftChildQuadrant(), points.get(i));
                continue;
            }

            if (node.getTopRightChildQuadrant().contains(points.get(i))) {
                addPointToNode(node.getTopRightChildQuadrant(), points.get(i));
                continue;
            }

            if (node.getBottomRightChildQuadrant().contains(points.get(i))) {
                addPointToNode(node.getBottomRightChildQuadrant(), points.get(i));
                continue;
            }

            if (node.getBottomLeftChildQuadrant().contains(points.get(i))) {
                addPointToNode(node.getBottomLeftChildQuadrant(), points.get(i));
                continue;
            }

        }

        node.setPoints(null);

    }

    private void addPointToNode(Node node, Point point) {

        if (node.getPoints() == null) {
            node.setPoints(new ArrayList<Point>(MAX_NUMBER_OF_POINTS_IN_LEAVES));
        }

        node.getPoints().add(point);
        node.increaseByOneNumberOfContainedPoints();
        node.increaseLRDatasetPoints(point.getTag() == 1);
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
