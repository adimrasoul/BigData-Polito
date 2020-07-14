package it.polito.bigdata.spark.exercise;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String outputPath;
		String inputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #30")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		//read input
		JavaRDD <String> inputRDD = sc.textFile(inputPath);
		
		//apply filter of www.google.com
		JavaRDD <String> googleRDD = inputRDD.filter(lineInput ->
							lineInput.toLowerCase().contains("www.google.com"));
		//map IP addresses
		JavaRDD <String> ipRDD = googleRDD.map(ipLine -> {
					String[] fields = ipLine.split(" ");
					String ip = fields[0];
					return ip;
		});
		//apply distinct on ips
		JavaRDD <String> ipDistictedRDD = ipRDD.distinct();
		
		//Save the output
		ipDistictedRDD.saveAsTextFile(outputPath);
		
		//close the session
		sc.close();
	}
}
