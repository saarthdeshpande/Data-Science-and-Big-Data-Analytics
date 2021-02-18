/*
Input (e.g.):
10.130.2.1,[02/Mar/2018:15:47:23,GET /allsubmission.php HTTP/1.1,200
10.130.2.1,[02/Mar/2018:15:47:32,GET /showcode.php?id=309&nm=ham05 HTTP/1.1,200
10.130.2.1,[02/Mar/2018:15:47:35,GET /allsubmission.php HTTP/1.1,200
10.130.2.1,[02/Mar/2018:15:47:46,GET /home.php HTTP/1.1,200

Output:
10.128.2.1      165010.7
10.129.2.1      36924.152
10.130.2.1      165108.45
10.131.0.1      164913.62
10.131.2.1      35997.35
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;  
import java.util.Date;  
import java.text.ParseException;
import java.util.Locale;
import java.util.ArrayList;
import java.util.Collections;

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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
// file handling
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class AccessLog {
	
	public static class WordMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
		private Text textObjectKey = new Text();
		
		public long processDate(String date) throws ParseException {
			try {
	    	    Date dateObject = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss", Locale.US).parse(date); 
	    	    return dateObject.getTime();
		    } catch(ParseException e) {
		        throw e;
		    }
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			WordMapper self = new WordMapper();
			while (tokenizer.hasMoreTokens()) {
				textObjectKey.set(tokenizer.nextToken());	// since output collector requires text object, not string
				try {
					output.collect(textObjectKey,  new LongWritable(self.processDate(tokenizer.nextToken())));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				break;
			}	
			self.close();
		}
	}
	
	public static class WordReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, FloatWritable> {
		
		public float computeTime(long initial_time, long final_time) {
			return ((float)(final_time - initial_time) / (1000 * 60));
		}
		
		public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException  {
			
			WordReducer self = new WordReducer();
			ArrayList<Long> timestamps = new ArrayList<Long>();
			while (values.hasNext()) {
				timestamps.add(values.next().get());
			}
			float active_login_time = self.computeTime(Collections.min(timestamps), Collections.max(timestamps));
			output.collect(key, new FloatWritable(active_login_time));
			self.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(AccessLog.class);
		conf.setJobName("AccessLogs");
		
		conf.setMapperClass(WordMapper.class);
		conf.setReducerClass(WordReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
