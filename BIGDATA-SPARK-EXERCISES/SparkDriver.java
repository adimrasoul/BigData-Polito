package it.polito.bigdata.spark.exercise;

import org.apache.spark.api.java.*;

import scala.Tuple2;

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
		
		//save output
		sidAllDatePRDD.saveAsTextFile(outputPath);
		
		//close the session
		sc.close();
		
	}
}
