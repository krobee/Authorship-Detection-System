package online;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class UnknownAAVMapper extends Mapper<Text, Text, Text, Text>{
	//input: <author, unigram|rawTF|TF>
	//output: <author, unigram|TF>
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] unigram_tf = value.toString().split("\\|");
		context.write(key, new Text(unigram_tf[0] + "|" + unigram_tf[2]));
	}
}