package offline;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.LineReader;

public class AAVReducer extends Reducer<Text,Text,Text,Text>{
	
	HashMap<String, String[]> w_map = new HashMap<String, String[]>();
	
	//input: <unigram, N|ni>
	//output: map<unigram,[N,ni]>
	@Override
	public void setup(Context context) throws IOException{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		if(!fs.exists(OfflinePathConf.FILE_ALL_WORDS))
			return;
		
		FSDataInputStream in = fs.open(OfflinePathConf.FILE_ALL_WORDS);
		LineReader lineReader = new LineReader(in, context.getConfiguration());
		Text currentLine = new Text("");
		
		while(lineReader.readLine(currentLine) > 0){
			String[] val = currentLine.toString().split("\\s+");
			String[] N_ni = val[1].split("\\|");
			w_map.put(val[0], N_ni);
			context.progress();
		}
		
		lineReader.close();
		in.close();
	}
	
	//input: <author, unigram|IDF|TFIDF>
	//output: <author, <unigram1,TFIDF1>, <unigram2, TFIDF2>, ...>
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String str = "";
		HashMap<String, Double[]> map = new HashMap<String, Double[]>();
		for(Text val: values){
			String[] uni_tfidf = val.toString().split("\\|");
			//map<unigram,[IDF,TFIDF]>
			Double[] tfidf_value = new Double[2];
			tfidf_value[0] = Double.parseDouble(uni_tfidf[1]); 
			tfidf_value[1] = Double.parseDouble(uni_tfidf[2]); 
			map.put(uni_tfidf[0], tfidf_value);
			context.progress();
		}
		
		for(Map.Entry<String, String[]> entry: w_map.entrySet()){
			String word = entry.getKey();
			if(map.containsKey(word)){
				str += entry.getKey() + "|" + map.get(word)[1];
			}
			else{
				String[] N_ni = entry.getValue();
				int N = Integer.parseInt(N_ni[0]);
				int ni = Integer.parseInt(N_ni[1]);
				double idf = Math.log10((double)N/ni);
				double tfidf = 0.5*idf;
				str += word + "|" + tfidf;
			}
			str += ",";
			context.progress();
		}
	
		context.write(key, new Text(str));
	}
}