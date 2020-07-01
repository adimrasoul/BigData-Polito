package it.polito.bigdata.hadoop.exercise;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends
		Mapper<Text, // Input key type
				Text, // Input value type
				NullWritable, // Output key type
				Text> {// Output value type

	DateValue Top1;
	DateValue Top2;
	protected void setup(Context context) {
		Top1 = null;
		Top2 = null;
				
	}

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {
		
		float tempValue = Float.parseFloat(value.toString());
		String tempDate = new String(key.toString());
		
		if(Top1==null ||
				tempValue > Top1.value ||
				(tempValue == Top1.value && tempDate.compareTo(Top1.date)<0)){
			Top2 = Top1;
			Top1 = new DateValue();
			Top1.date = tempDate;
			Top1.value = tempValue;	
		}
		else 
		{	if
				(Top2==null ||
				tempValue > Top2.value ||
				(tempValue == Top2.value && tempDate.compareTo(Top2.date)<0)) {
			Top2 = new DateValue();
			Top2.date = tempDate;
			Top2.value = tempValue;	
		}
		}
		
		

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), new Text(Top1.date+"_"+Top1.value));
		context.write(NullWritable.get(),new Text(Top2.date+"_"+Top2.value));
	}
}
