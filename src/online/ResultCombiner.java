package online;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class ResultCombiner extends Reducer<DoubleWritable, Text, DoubleWritable, Text>{
	int nCount = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		nCount = 0;
	}
	
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if(nCount < 10){
			for(Text val: values){
				context.write(key, val);
				nCount++;
			}
		}
	}
}