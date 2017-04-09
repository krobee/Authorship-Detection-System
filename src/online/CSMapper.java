package online;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;
public class CSMapper extends Mapper<Text, Text, Text, Text>{
	HashMap<String,Double> unknown_aav = new HashMap<String,Double>();
	
	@Override
	public void setup(Context context) throws IOException{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		if(!fs.exists(OnlinePathConf.FILE_UNKNOWN_AAV))
			return;
		
		FSDataInputStream in = fs.open(OnlinePathConf.FILE_UNKNOWN_AAV);
		LineReader lineReader = new LineReader(in, context.getConfiguration());
		Text currentLine = new Text("");
		
		//map<unigram, tfidf>
		while(lineReader.readLine(currentLine) > 0){
			String[] val = currentLine.toString().split("\\s+");
			String[] unigram_tfidf_vals = val[1].split("\\,");
			for(String values: unigram_tfidf_vals){
				String[] val2 = values.split("\\|");
				unknown_aav.put(val2[0], Double.parseDouble(val2[1]));
			}
		}
		
		lineReader.close();
		in.close();
	}
	
	//input: <author, <unigram1,TFIDF1>, <unigram2, TFIDF2>, ...>
	//output: <author, cosine_similarity>
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		HashMap<String,Double> offline_aav = new HashMap<String,Double>();
		String[] unigram_tfidf_vals = value.toString().split("\\,");
		for(String values: unigram_tfidf_vals){
			String[] val = values.split("\\|");
			offline_aav.put(val[0], Double.parseDouble(val[1]));
		}
		
		double dotProduct = 0;
		double A_norm = 0;
		double B_norm = 0;
		
		for(Map.Entry<String, Double> entry: offline_aav.entrySet()){
			String unigram = entry.getKey();
			double A_value = unknown_aav.get(unigram);
			double B_value = entry.getValue();
			dotProduct += A_value*B_value;
			
			A_norm += A_value*A_value;
			B_norm += B_value*B_value;
		}
		A_norm = Math.sqrt(A_norm);
		B_norm = Math.sqrt(B_norm);
		double similarity = dotProduct/(A_norm*B_norm);
		String sim = String.valueOf(similarity);
		context.write(key, new Text(sim.toString()));
	}
	
	
	
	
}