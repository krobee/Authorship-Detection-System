package online;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.LineReader;

import offline.OfflinePathConf;
public class UnknownAAVReducer extends Reducer<Text,Text,Text,Text>{
//	HashMap<String, String[]> tf_map = new HashMap<String, String[]>();
	HashMap<String, String[]> w_map = new HashMap<String, String[]>();
	
	@Override
	public void setup(Context context) throws IOException{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
		//TFinput: <author, unigram|rawTF|TF>
		//TFoutput: tf_map<author,[unigram,TF]>
//		if(!fs.exists(OnlinePathConf.FILE_UNKNOWN_TF))
//			return;
//
//		FSDataInputStream in = fs.open(OnlinePathConf.FILE_UNKNOWN_TF);
//		LineReader lineReader = new LineReader(in, context.getConfiguration());
//		Text currentLine = new Text("");
//
//		while(lineReader.readLine(currentLine) > 0){
//			String[] val = currentLine.toString().split("\\s+");
//			String[] unigram_tf = val[1].split("\\|");
//			tf_map.put(val[0], unigram_tf);
//		}
//
//		lineReader.close();
//		in.close();
		
		//input: <unigram, N|ni>
		//output: map<unigram,[N,ni]>
		if(!fs.exists(OfflinePathConf.FILE_ALL_WORDS))
			return;
		
		FSDataInputStream in2 = fs.open(OfflinePathConf.FILE_ALL_WORDS);
		LineReader lineReader2 = new LineReader(in2, context.getConfiguration());
		Text currentLine = new Text("");
		
		while(lineReader2.readLine(currentLine) > 0){
			String[] val = currentLine.toString().split("\\s+");
			String[] N_ni = val[1].split("\\|");
			w_map.put(val[0], N_ni);
		}
		
		lineReader2.close();
		in2.close();
		
	}

	//input: <author, unigram|TF>
	//output: <author, <unigram1,TFIDF1>, <unigram2, TFIDF2>, ...>

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashMap<String, String[]> map = new HashMap<String, String[]>();
		for(Text val: values){
			//map<unigram,[author,TF]>
			String[] unigram_tf = val.toString().split("\\|");
			String[] author_tf = new String[2];
			author_tf[0] = key.toString();
			author_tf[1] = unigram_tf[1];
			map.put(unigram_tf[0],author_tf);
		}
		
		String str = "";
		for(Map.Entry<String, String[]> entry: w_map.entrySet()){
			String word = entry.getKey();
			String[] N_ni = entry.getValue();
			int N = Integer.parseInt(N_ni[0]);
			int ni = Integer.parseInt(N_ni[1]);
			double idf = Math.log10((double)N/ni);
			if(map.containsKey(word)){
				double tf = Double.parseDouble(map.get(word)[1]);
				double tfidf = tf*idf;
				str += word + "|" + tfidf;
			}
			else{
				str += word + "|" + 0.5*idf;
			}
			str += ",";
		}
		context.write(key, new Text(str));
	}
}