package gr.ds.unipi.spades;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import gr.ds.unipi.qtree.MathUtils;
import gr.ds.unipi.qtree.Point;
import gr.ds.unipi.qtree.QuadTree;
import scala.Tuple2;

public class SpatialJoin {
	public JavaRDD<Tuple2<Point, Point>> resultPairs;
	
	public QuadTree createQuadTree(double minX, double minY, double maxX, double maxY, int samplePointsPerLeaf, double samplePercentage, JavaRDD<Point> points) {
		//System.out.println(points.first().toString());
		double epsilon = 0.00001;
		QuadTree quadTree = new QuadTree(minX - epsilon, minY - epsilon, maxX + epsilon, maxY + epsilon, samplePointsPerLeaf);
		 // sampling
        List<Point> sample = points.takeSample(false, (int) (points.count() * samplePercentage));
        
        for (Point p : sample) {
        	//System.out.println(p.toString());
        	quadTree.insertPoint(p);
        }   
        return quadTree;
	}
	
	public JavaRDD<Point> map(JavaRDD<String> lines) {
		// Map
        // Extract point information
        JavaRDD<Point> points = lines.map(line -> {
        	String lon, lat;
        	int lonOrdinalIndex, latOrdinalIndex;
        	int filelonIndex, filelatIndex;
        	int fileTag;
        	String fileseparator;
        	
    		filelonIndex = 0;
    		filelatIndex = 1;
    		fileseparator = "|";
    		
        	
        	if (filelonIndex == 0) {
    			lon = line.substring(0, line.indexOf(fileseparator));
    		} else {
    			lonOrdinalIndex = StringUtils.ordinalIndexOf(line, fileseparator, filelonIndex);
    			
    			if (lonOrdinalIndex == StringUtils.lastOrdinalIndexOf(line, fileseparator, 1))
        		{
        			lon =  line.substring(lonOrdinalIndex + 1);
        		} else {
        			lon = line.substring(lonOrdinalIndex + 1, StringUtils.ordinalIndexOf(line, fileseparator, filelonIndex + 1));
        		}    			
    		}
    		
    		
        	if (filelatIndex == 0) {
    			lat = line.substring(0, line.indexOf(fileseparator));
    		} else {
    			latOrdinalIndex = StringUtils.ordinalIndexOf(line, fileseparator, filelatIndex);
    			
    			if (latOrdinalIndex == StringUtils.lastOrdinalIndexOf(line, fileseparator, 1))
        		{
        			lat =  line.substring(latOrdinalIndex + 1);
        		} else {
        			lat = line.substring(latOrdinalIndex + 1, StringUtils.ordinalIndexOf(line, fileseparator, filelatIndex + 1));
        		}
    		}
        	String[] keywords = null;
        	if (StringUtils.ordinalIndexOf(line, fileseparator, 2) == -1) {
        		fileTag = 1;
        		
        	} else {
        		keywords = line.substring(StringUtils.ordinalIndexOf(line, fileseparator, 2) + 1).split(",");
        		fileTag = 2;
        	}
    		return new Point(Double.parseDouble(lon), Double.parseDouble(lat), fileTag, keywords);         	
        });
        
        return points;
	}
	
	public JavaRDD<Tuple2<Point, Point>> reduce(Broadcast<QuadTree> broadcastQuadTree, JavaRDD<Point> points, double radius, double similarityScore, String[] keywords) {
        JavaPairRDD<Integer, Iterable<Point>> nodePointPairs = points.flatMapToPair(point -> {
        	QuadTree qt = broadcastQuadTree.getValue();
        	if (point.getTag() == 1)
        		return qt.assignToLeafNodeIterator(qt.getRoot(), point).iterator();
        	
        	double mbrUpperY = MathUtils.getPointInDistanceAndBearing(point, radius, 0).getY();
        	double mbrLowerY = MathUtils.getPointInDistanceAndBearing(point, radius, 180).getY();
        	double mbrUpperX = MathUtils.getPointInDistanceAndBearing(point, radius, 90).getX();
        	double mbrLowerX = MathUtils.getPointInDistanceAndBearing(point, radius, 270).getX();
        	point.setMBR(mbrLowerX, mbrLowerY, mbrUpperX, mbrUpperY);
        	return qt.assignToLeafNodeAndDuplicate(qt.getRoot(), point).iterator();
        }).groupByKey();
        
        resultPairs = nodePointPairs.flatMap((FlatMapFunction<Tuple2<Integer, Iterable<Point>>, Tuple2<Point, Point>>) pair -> {
        	ArrayList<Point> local = new ArrayList<Point>();
        	ArrayList<Tuple2<Point, Point>> output = new ArrayList<Tuple2<Point, Point>>(); 
        	for (Point point : pair._2) {
        		if (point.getTag() == 1) { 
        			local.add(point);
        			continue;
        		}
        		
    			for (Point p : local) {
        			if (MathUtils.haversineDistance(p, point) <= radius && MathUtils.jaccardSimilarity(keywords, point.getKeywords()) >= similarityScore) {
        				output.add(new Tuple2<Point, Point>(p, point));
        			}
        		}
        	}
        	
        	return output.iterator();
        });
        
        return resultPairs;
	}
}
