package it.polito.bigdata.spark.exercise;

import org.apache.spark.api.java.*;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;

		inputPath = args[0];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #36")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		//read input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		
		//map all pm values
		JavaRDD<Double> pmValuesRDD = inputRDD.map(inputLine ->
		{
			String[] fields = inputLine.split(",");
			Double pmValue = new Double(fields[2]);
			return pmValue;
			
		});
		
		//count the number of pm Value
		long countLine = pmValuesRDD.count();
		//sum all pm values
		
		Double pmSum = pmValuesRDD.reduce((pm1,pm2)->
		{
			Double sum;
			sum = pm1+pm2;
			return sum;
		});
		//Print output
		System.out.println("Avg is : "+ pmSum/countLine);
		//close session
		sc.close();
	}
}
