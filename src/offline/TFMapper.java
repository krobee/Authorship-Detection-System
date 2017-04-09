package offline;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TFMapper extends Mapper<Text, Text, Text, Text>{
	//input: <author|unigram, rawTF>
	//output: <author, unigram|rawTF>
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] author_unigram = key.toString().split("\\|");
		context.write(new Text(author_unigram[0]), new Text(author_unigram[1] + "|" + value));
	}
}