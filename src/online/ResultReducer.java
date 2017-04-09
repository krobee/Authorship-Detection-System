package online;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class ResultReducer extends Reducer<DoubleWritable, Text,Text,DoubleWritable>{
	int nCount = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		nCount = 0;
	}
	
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if(nCount < 10){
			for(Text val: values){
				double sim = key.get()*-1;
				context.write(val, new DoubleWritable(sim));
				nCount++;
			}
		}
	}
}