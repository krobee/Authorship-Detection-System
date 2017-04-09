package offline;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TFIDFMapper extends Mapper<Text, Text, Text, Text>{
	//input: <author, unigram|rawTF|TF>
	//output: <unigram, author|rawTF|TF>
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] unigram_TF = value.toString().split("\\|");
		context.write(new Text(unigram_TF[0]), new Text(key + "|" + unigram_TF[1] + "|" + unigram_TF[2]));
	}
}