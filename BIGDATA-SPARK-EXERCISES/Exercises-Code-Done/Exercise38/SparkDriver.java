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
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #38")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
			
		//read input
		JavaRDD<String> inputRDD =sc.textFile(inputPath);
		
		//filter the line that has pm over 50
		JavaRDD<String> filteredRDD = inputRDD.filter(inputLine ->
		{
			String[] fields = inputLine.split(",");
			double pmValue = Double.parseDouble(fields[2]);
			if(pmValue>50)
				return true;
			else
				return false;
		});
		//map sIF and pmValue of rows over 50
		JavaPairRDD<String,Integer> sidOverPRDD = filteredRDD.mapToPair(inputLine->
		{
			Tuple2<String,Integer> pair;
			String sID;
			String[] fields = inputLine.split(",");
			sID = new String(fields[0]);
			pair = new Tuple2<String,Integer>(sID,1);
			return pair;
			});
		//sum all values for each sensor
		JavaPairRDD<String,Integer> countSidOverPRDD = sidOverPRDD.reduceByKey(
				(value1,value2)->
		{
			return new Integer(value1+value2);
		});
		
		//filter the ones that have value od count over 2
		JavaPairRDD<String,Integer> filteredCountOverPRDD = countSidOverPRDD.filter(
				(Tuple2<String,Integer> inputCount) ->
				{
					if (inputCount._2().intValue()>=2)
					
						return true;
					else
						return false;
					
					}
				);
		//save output
		filteredCountOverPRDD.saveAsTextFile(outputPath);
		//close session
		sc.close();
		
				}
}
