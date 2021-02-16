/*
Input (IP_Address, login_time, logout_time):
192.168.14.201          		10			20
192.168.14.202                  	25			45				
192.168.14.201           		39			50		
192.168.14.203           		67			89
192.168.14.203          		37			68
192.168.14.204          		78			99	

Output:
192.168.14.201  21
192.168.14.202  20
192.168.14.203  53
192.168.14.204  21
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

// main requirements
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;

import org.apache.hadoop.mapred.OutputCollector;	// intermediate output
import org.apache.hadoop.mapred.Reporter;	// debug
import org.apache.hadoop.mapred.TextInputFormat;	
import org.apache.hadoop.mapred.TextOutputFormat;
// data types
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
// file handling
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class AccessLog {
	
	public static class TokenMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
		private Text textObjectKey = new Text();
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				textObjectKey.set(tokenizer.nextToken());	// since output collector requires text object, not string
				long login_time = Long.parseLong(tokenizer.nextToken());
				output.collect(textObjectKey,  new LongWritable(Long.parseLong(tokenizer.nextToken()) - login_time));
			}	
		}
	}
	
	public static class TokenReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException  {
			long sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new LongWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(AccessLog.class);
		conf.setJobName("AccessLogs");
		
		conf.setMapperClass(TokenMapper.class);
		conf.setReducerClass(TokenReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
