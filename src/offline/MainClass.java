package offline;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class MainClass {
	
	public static enum Author{
		COUNT
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.out.printf("Usage: <jar file> <input dir> <output dir>\n");
			System.exit(-1);
		}

		Configuration conf =new Configuration();
		FileSystem dfs = FileSystem.get(conf);
		int status = 0;
		
		if(dfs.exists(OfflinePathConf.PATH_UNI))
			dfs.delete(OfflinePathConf.PATH_UNI,true);
		if(dfs.exists(OfflinePathConf.PATH_TF))
			dfs.delete(OfflinePathConf.PATH_TF,true);
		if(dfs.exists(OfflinePathConf.PATH_TF_IDF))
			dfs.delete(OfflinePathConf.PATH_TF_IDF,true);
		if(dfs.exists(OfflinePathConf.PATH_ALL_WORDS))
			dfs.delete(OfflinePathConf.PATH_ALL_WORDS,true);
		if(dfs.exists(new Path(args[1])))
			dfs.delete(new Path(args[1]),true);

		//job 1
		Job job1 = Job.getInstance(conf,"get unigram");
		job1.setJarByClass(MainClass.class);
		job1.setMapperClass(UnigramMapper.class);
		job1.setReducerClass(UnigramReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, OfflinePathConf.PATH_UNI);

		
		status = job1.waitForCompletion(true) ? 0 : 1;

		//job 2
		Job job2 = Job.getInstance(conf, "get TF");
		job2.setJarByClass(MainClass.class);
		job2.setMapperClass(TFMapper.class);
		job2.setReducerClass(TFReducer.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job2, OfflinePathConf.PATH_UNI);
		FileOutputFormat.setOutputPath(job2, OfflinePathConf.PATH_TF);

		
		status = job2.waitForCompletion(true) ? 0 : 1;
		
		//get numAuthor from job2 reducer counter
		long numAuthor = job2.getCounters().findCounter(Author.COUNT).getValue();

		//job 3
		//pass numAuthor to job3 reducer
		conf.set("numAuthor", String.valueOf(numAuthor));
		
		Job job3 = Job.getInstance(conf, "get TF-IDF");
		job3.setJarByClass(MainClass.class);
		job3.setMapperClass(TFIDFMapper.class);
		job3.setReducerClass(TFIDFReducer.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job3, OfflinePathConf.PATH_TF);
		FileOutputFormat.setOutputPath(job3, OfflinePathConf.PATH_TF_IDF);

		
		status = job3.waitForCompletion(true) ? 0 : 1;

		//job4
		Job job4 = Job.getInstance(conf, "get all words");

		job4.setJarByClass(MainClass.class);
		job4.setMapperClass(AllWordsMapper.class);
		job4.setReducerClass(AllWordsReducer.class);
		
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(IntWritable.class);

		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job4, OfflinePathConf.PATH_TF);
		FileOutputFormat.setOutputPath(job4, OfflinePathConf.PATH_ALL_WORDS);

		
		status = job4.waitForCompletion(true) ? 0 : 1;
		
		//job5
		Job job5 = Job.getInstance(conf, "get AAV");
		job5.setJarByClass(MainClass.class);
		job5.setMapperClass(AAVMapper.class);
		job5.setReducerClass(AAVReducer.class);

		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(Text.class);

		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);

		job5.setInputFormatClass(KeyValueTextInputFormat.class);
		job5.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job5, OfflinePathConf.PATH_TF_IDF);
		FileOutputFormat.setOutputPath(job5, new Path(args[1]));

		
		status = job5.waitForCompletion(true) ? 0 : 1;
		
		System.exit(status);
	}
}