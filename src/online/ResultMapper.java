package online;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class ResultMapper extends Mapper<Text, Text, DoubleWritable, Text>{
	
	//input: <author, cosine_similarity>
	//output: <cosine_similarity, author>
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		double sim = Double.parseDouble(value.toString());
		context.write(new DoubleWritable(sim*-1),key);
	}
}