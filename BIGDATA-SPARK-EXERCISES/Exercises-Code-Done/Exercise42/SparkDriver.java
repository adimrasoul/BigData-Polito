package it.polito.bigdata.spark.exercise;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPathQuestions;
		String inputPathAnswers;
		String outputPath;

		inputPathQuestions = args[0];
		inputPathAnswers = args[1];
		outputPath = args[2];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #42")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		//read the questions input
		JavaRDD<String> questionRDD = sc.textFile(inputPathQuestions);
		//read the answers input
		JavaRDD<String> answerRDD = sc.textFile(inputPathAnswers);
		//map qID and Text of question
		JavaPairRDD<String,String> questionPRDD = questionRDD.mapToPair(
				inputLine->
				{
					Tuple2<String,String> pair;
					String[] fields = inputLine.split(",");
					String qID;
					String qText;
					qID = fields[0];
					qText = fields[2];
					pair = new Tuple2<String,String>(qID,qText);
					return pair;
				});
		//map aID and Text of answer
		JavaPairRDD<String,String> answerPRDD = answerRDD.mapToPair(
				inputLine->
				{
					Tuple2<String,String> pair;
					String[] fields = inputLine.split(",");
					String aID;
					String aText;
					aID = fields[1];
					aText = fields[3];
					pair = new Tuple2<String,String>(aID,aText);
					return pair;
				});
		//create for each question an list of related answers 
		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> questionAnswerPRDD =
				questionPRDD.cogroup(answerPRDD);
		//save
		questionAnswerPRDD.saveAsTextFile(outputPath);
	}
}
