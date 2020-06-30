package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends Mapper<Text, // Input key type
		Text, // Input value type
		NullWritable, // Output key type
		DateIncome> {// Output value type

	private DateIncome chunckTop1;

	protected void setup(Context context) {
		// for each mapper, top1 is used to store the information about the top1
		// date-income of the subset of lines analyzed by the mapper
	
	chunckTop1 = new DateIncome();
	chunckTop1.setIncome(Float.MIN_VALUE);
	chunckTop1.setDate(null);
	}

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {
			String lineDate = new String(key.toString());
			float lineIncome =  Float.parseFloat(value.toString());
			if (lineIncome > chunckTop1.getIncome()||
					lineIncome == chunckTop1.getIncome() && lineDate.compareTo(chunckTop1.getDate()) < 0) {
				chunckTop1.setIncome(lineIncome);
				chunckTop1.setDate(lineDate);
							
			}
	}


	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Emit the top1 date and income related to this mapper
		
		context.write(NullWritable.get(),chunckTop1);
	}

}
