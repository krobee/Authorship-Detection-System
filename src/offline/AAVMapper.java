package offline;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class AAVMapper extends Mapper<Text, Text, Text, Text>{
	//input: <unigram, author|IDF|TFIDF>
	//output: <author, unigram|IDF|TFIDF>
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] author_TFIDF = value.toString().split("\\|");
		context.write(new Text(author_TFIDF[0]), new Text(key + "|" + author_TFIDF[1] + "|" + author_TFIDF[2]));
	}
}