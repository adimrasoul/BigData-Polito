package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData12 extends
		Mapper<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				FloatWritable> {// Output value type

	float threshold;

	protected void setup(Context context) {
		// I retrieve the value of the threshold only one time for each mapper
			threshold = Float.parseFloat(
					context.getConfiguration().get("maxThreshold"));
			
	}

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {
			
			float pm10Value;
			pm10Value = Float.parseFloat(value.toString());
			if (pm10Value < threshold) {
				context.write(new Text(key),
						new FloatWritable(pm10Value));
			}
	
		}

	}
