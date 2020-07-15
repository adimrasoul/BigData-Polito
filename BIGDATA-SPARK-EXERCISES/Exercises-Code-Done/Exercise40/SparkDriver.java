package it.polito.bigdata.spark.exercise;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {
		
		String inputPath;
		String outputPath;

		
		inputPath=args[1];
		outputPath=args[2];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #40")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		//read the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		//filter the lines which have pm over 50
		JavaRDD<String> filteredInputRDD = inputRDD.filter(
				inputLine->
				{
					String[] fields = inputLine.split(",");
					double pmValue = new Double(fields[2]);
					if (pmValue>=50)
						return true;
					else
						return false;
				});
		//map all sID as key and 1 as value
		JavaPairRDD<String,Integer> sIdNumOfOccurPRDD = filteredInputRDD.mapToPair(
				inputLine->
				{
					String sID;
					Tuple2<String,Integer> pair;
					String[] fields = inputLine.split(",");
					sID = fields[0];
					pair = new Tuple2<String,Integer>(sID,1);
					return pair;
				});
		// sum all values of each sID to know number of occurnesses
		JavaPairRDD<String,Integer> sIdTotalNumOfOccurPRDD = sIdNumOfOccurPRDD.reduceByKey(
				(value1,value2)->{
					return value1+value2;
					});
		//swipping key and value 
		JavaPairRDD<Integer,String> totalNumOfOccurSIdPRRD = sIdTotalNumOfOccurPRDD
				.mapToPair(
				(Tuple2<String,Integer> inputPair)->
				{
				return new Tuple2<Integer,String>(inputPair._2().intValue(),inputPair._1())	;
				});
		//sort desc by key
		JavaPairRDD<Integer,String> sortedFinalResult = totalNumOfOccurSIdPRRD.sortByKey(false);
		
		
		//save
		sortedFinalResult.saveAsTextFile(outputPath);
		//close session
		sc.close();
				
	}
}
