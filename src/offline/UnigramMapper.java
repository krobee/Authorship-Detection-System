package offline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class UnigramMapper extends Mapper<Object, Text, Text, IntWritable>{
	
	private final static IntWritable one = new IntWritable(1);
	
	public String getAuthor(String line){
		int end = line.indexOf("<===>");
		int start = line.substring(0, end).lastIndexOf(' ') + 1;
		return line.substring(start, end).toLowerCase();
	}
	
	//input: <lineoffset#, a line>
	//output: <author|unigram, 1>
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		int start = value.toString().lastIndexOf("<===>") + 5;
		String[] words = value.toString().substring(start).replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().split("\\s+");
		for(String word: words){
			word.trim();
			if(word.length() > 0)
				context.write(new Text(getAuthor(value.toString()) + "|" + word), one);
		}
	}
}