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
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #37")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		//read input
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		
		//map all SensorID and pmValues
		JavaPairRDD<String,Double> sidPmPairRDD =inputRDD.mapToPair(inputLine ->
				{
					String sID;
					Double pmValue;
					Tuple2<String,Double> pair;
					String[] fields = inputLine.split(",");
					pmValue = new Double(fields[2]);
					sID = new String(fields[0]);
					pair = new Tuple2<String,Double> (sID,pmValue);
					return pair;
				});
		//find the max pm value of each sensor
		JavaPairRDD<String,Double> maxsidPmPairRDD = sidPmPairRDD.reduceByKey((pm1,pm2)->
		{
			if(pm1.compareTo(pm2)>0)
				return pm1;
			else
				return pm2;
		});
		//print sId and its max pmValue
		maxsidPmPairRDD.saveAsTextFile(outputPath);
		
		//close session
		sc.close();
		
	}
}
