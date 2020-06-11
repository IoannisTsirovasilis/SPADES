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
		QuadTree quadTree = new QuadTree(minX, minY, maxX, maxY, samplePointsPerLeaf);
		 // sampling
        List<Point> sample = points.takeSample(false, (int) (points.count() * samplePercentage));
        
        for (Point p : sample) {
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
    		fileseparator = ",";
    		fileTag = Integer.parseInt(line.substring(StringUtils.lastOrdinalIndexOf(line, fileseparator, 1) + 1));
        	
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
        	
        	String[] keywords = line.substring(StringUtils.ordinalIndexOf(line, fileseparator, 2) + 1, StringUtils.ordinalIndexOf(line, fileseparator, 3)).split("\\|");
        	
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
        				//System.out.println(String.format("(%d, %d)", p.getTag(), point.getTag()));
        			}
        		}
        	}
        	
        	return output.iterator();
        });
        
        return resultPairs;
	}
}
