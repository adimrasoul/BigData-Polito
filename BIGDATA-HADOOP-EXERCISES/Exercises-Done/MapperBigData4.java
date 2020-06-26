package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 4 - Mapper
 */
class MapperBigData4 extends
		Mapper<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				Text> { // Output value type

	//private static Double PM10Threshold = new Double(50);

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Extract zone and date from the key

		String[] words=key.toString().split(",");
		String zoneID= words[0];
		String SensorDate = words[1];
		Double sensorVal = new Double(value.toString());
		if (sensorVal>=50) {
			context.write(new Text(zoneID), new Text(SensorDate));
		}
	}
}
