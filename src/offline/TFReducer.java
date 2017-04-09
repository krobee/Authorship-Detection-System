package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import offline.MainClass.Author;
public class TFReducer extends Reducer<Text,Text,Text,Text>{
	//input: <author, unigram|rawTF>
	//output: <author, unigram|rawTF|TF>
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<String> cache = new ArrayList<>();
		
		int max = Integer.MIN_VALUE;
		for(Text val: values) {
			String[] unigram_rawTF = val.toString().split("\\|");
			int rawTF = Integer.parseInt(unigram_rawTF[1]);
			if(rawTF > max)
				max = rawTF;
			cache.add(val.toString());
		}
		
		for(String val: cache) {
			String[] unigram_rawTF = val.split("\\|");
			long rawTF = Long.parseLong(unigram_rawTF[1]);
			double TF = 0.5+0.5*((double)rawTF/max);
			context.write(key, new Text(unigram_rawTF[0]+"|"+rawTF+"|"+TF));
		}
		
		//update numAuthor and pass back to main
		context.getCounter(Author.COUNT).increment(1);
	}
}