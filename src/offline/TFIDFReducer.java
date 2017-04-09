package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class TFIDFReducer extends Reducer<Text,Text,Text,Text>{
	//input: <unigram, author|rawTF|TF>
	//output: <unigram, author|IDF|TFIDF>
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<String> cache = new ArrayList<>();
		
		//get N = numAuthor
		Configuration conf = context.getConfiguration();
		long N = Long.parseLong(conf.get("numAuthor"));
		long ni = 0;
		
		for(Text val: values) {
//			String[] author_TF = val.toString().split("\\|");
//			long rawTF = Long.parseLong(author_TF[1]);
			cache.add(val.toString());
			ni++;
		}
		
		double IDF = Math.log10((double)N/ni);
		
		for(String val: cache) {
			String[] author_TF = val.split("\\|");
			double TF = Double.parseDouble(author_TF[2]);
			double TFIDF = TF*IDF;
			context.write(key, new Text(author_TF[0]+"|"+IDF+"|"+TFIDF));
		}
		
	}
}