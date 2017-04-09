package online;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import offline.OfflinePathConf;
import offline.TFMapper;
import offline.TFReducer;
import offline.UnigramMapper;
import offline.UnigramReducer;

public class MainClass {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.out.printf("Usage: <jar file> <input dir> <output dir>\n");
			System.exit(-1);
		}

		Configuration conf =new Configuration();
		FileSystem dfs = FileSystem.get(conf);
		int status = 0;
		
		if(dfs.exists(OnlinePathConf.PATH_UNKNOWN_UNI))
			dfs.delete(OnlinePathConf.PATH_UNKNOWN_UNI,true);
		if(dfs.exists(OnlinePathConf.PATH_UNKNOWN_TF))
			dfs.delete(OnlinePathConf.PATH_UNKNOWN_TF,true);
		if(dfs.exists(OnlinePathConf.PATH_UNKNOWN_AAV))
			dfs.delete(OnlinePathConf.PATH_UNKNOWN_AAV,true);
		if(dfs.exists(OnlinePathConf.PATH_RAW_RESULT))
			dfs.delete(OnlinePathConf.PATH_RAW_RESULT,true);
		if(dfs.exists(new Path(args[1])))
			dfs.delete(new Path(args[1]),true);
		
		//job 1
		Job job1 = Job.getInstance(conf,"get unknown unigram");
		job1.setJarByClass(MainClass.class);
		job1.setMapperClass(UnigramMapper.class);
		job1.setReducerClass(UnigramReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, OnlinePathConf.PATH_UNKNOWN_UNI);
		
		status = job1.waitForCompletion(true) ? 0 : 1;
		
		//job 2
		Job job2 = Job.getInstance(conf, "get unknown TF");
		job2.setJarByClass(MainClass.class);
		job2.setMapperClass(TFMapper.class);
		job2.setReducerClass(TFReducer.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job2, OnlinePathConf.PATH_UNKNOWN_UNI);
		FileOutputFormat.setOutputPath(job2, OnlinePathConf.PATH_UNKNOWN_TF);

		status = job2.waitForCompletion(true) ? 0 : 1;
		
		//job 3
		Job job3 = Job.getInstance(conf, "get unknown aav");
		job3.setJarByClass(MainClass.class);
		job3.setMapperClass(UnknownAAVMapper.class);
		job3.setReducerClass(UnknownAAVReducer.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job3, OnlinePathConf.PATH_UNKNOWN_TF);
		FileOutputFormat.setOutputPath(job3, OnlinePathConf.PATH_UNKNOWN_AAV);

		status = job3.waitForCompletion(true) ? 0 : 1;
		
		//job 4
		Job job4 = Job.getInstance(conf, "compute cosine similarity");
		job4.setJarByClass(MainClass.class);
		job4.setMapperClass(CSMapper.class);
		job4.setReducerClass(CSReducer.class);


		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);

		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job4, OfflinePathConf.PATH_AAV);
		FileOutputFormat.setOutputPath(job4, OnlinePathConf.PATH_RAW_RESULT);

		status = job4.waitForCompletion(true) ? 0 : 1;

		//job 5
		Job job5 = Job.getInstance(conf, "get top 10 results");
		job5.setJarByClass(MainClass.class);
		
		job5.setNumReduceTasks(1);
		
		job5.setMapperClass(ResultMapper.class);
		job5.setCombinerClass(ResultCombiner.class);
		job5.setReducerClass(ResultReducer.class);

		job5.setInputFormatClass(KeyValueTextInputFormat.class);
		job5.setMapOutputKeyClass(DoubleWritable.class);
		job5.setMapOutputValueClass(Text.class);
		
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.setInputPaths(job5, OnlinePathConf.PATH_RAW_RESULT);
		FileOutputFormat.setOutputPath(job5, new Path(args[1]));

		status = job5.waitForCompletion(true) ? 0 : 1;
		
		System.exit(status);
	}

}
