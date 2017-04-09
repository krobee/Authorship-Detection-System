package offline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class AllWordsReducer extends Reducer<Text,IntWritable,Text,Text>{
	//input: <unigram, rawTF>
	//output: <unigram, N|ni>
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;

		//get N = numAuthor
		Configuration conf = context.getConfiguration();
		long N = Long.parseLong(conf.get("numAuthor"));
		
		for(IntWritable val: values) {
//			sum += val.get();
			sum++;
		}
		context.write(key, new Text(N + "|" + sum));
	}
}