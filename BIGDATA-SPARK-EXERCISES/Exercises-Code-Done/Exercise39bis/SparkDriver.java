package it.polito.bigdata.spark.exercise;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #39")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		//read input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		
		//filter the rows that have pm value over 50
		JavaRDD<String> filteredInputRDD = inputRDD.filter(lineInput->
		{
			String[] fields= lineInput.split(",");
			double pmValue = new Double(fields[2]);
			if (pmValue>50)
				return true;
			else
				return false;
		});
		JavaPairRDD<String,String> sidDatePRDD = filteredInputRDD.mapToPair(
				lineInput->
				{
					Tuple2<String,String> pair;
					String sID;
					String date;
					String[] fields= lineInput.split(",");
					sID=new String(fields[0]);
					date=new String(fields[1]);
					pair = new Tuple2<String,String> (sID,date);
					return pair;					
				});
		//join all dates associated with one sID with groupByKey
		JavaPairRDD<String,Iterable<String>> sidAllDatePRDD = sidDatePRDD.groupByKey();
		
		
		//***************************//
		//SecondPart//
		//***************************//
		//create a RDD of all sensorIds in input
		JavaRDD<String> allSensorsRDD = inputRDD.map(inputLine->
				{
					String[] fields = inputLine.split(",");
					return fields[0];
			});
		
		//subtract all sensors from all over50 to find the sensors that have never been above 50
		JavaRDD<String> sensorsNeverOverRDD=allSensorsRDD.subtract(sidDatePRDD.keys());
		
		//convert sensorsNeverOverRDD to JavapairRDD in order to do union
		JavaPairRDD<String,Iterable<String>> convertedSensorsNeverOverPRDD =  sensorsNeverOverRDD
				.mapToPair(sensorID ->
				{
				 Tuple2<String,Iterable <String>> pair;
				 pair = new Tuple2<String,Iterable <String>>(sensorID, new ArrayList<String>());
				 return pair;
				});
		//union
		JavaPairRDD<String,Iterable<String>> unionPRDD = sidAllDatePRDD
				.union(convertedSensorsNeverOverPRDD);
				//save output
		unionPRDD.saveAsTextFile(outputPath);
		
		//close the session
		sc.close();
		
	}
}
