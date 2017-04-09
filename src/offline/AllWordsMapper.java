package offline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class AllWordsMapper extends Mapper<Text, Text, Text, IntWritable>{
	//input: <author, unigram|rawTF|TF>
	//output: <unigram, rawTF>
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] unigram_TF = value.toString().split("\\|");
		context.write(new Text(unigram_TF[0]), new IntWritable(Integer.parseInt(unigram_TF[1])));
	}
}