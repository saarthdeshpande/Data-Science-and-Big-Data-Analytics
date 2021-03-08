/*
Input (e.g.):
2006-04-01 00:00:00.000 +0200,Partly Cloudy,rain,9.472222222222221,7.3888888888888875,0.89,14.1197,251.0,15.826300000000002,0.0,1015.13,Partly cloudy throughout the day.
2006-04-01 01:00:00.000 +0200,Partly Cloudy,rain,9.355555555555558,7.227777777777776,0.86,14.2646,259.0,15.826300000000002,0.0,1015.63,Partly cloudy throughout the day.
2006-04-01 02:00:00.000 +0200,Mostly Cloudy,rain,9.377777777777778,9.377777777777778,0.89,3.9284000000000003,204.0,14.9569,0.0,1015.94,Partly cloudy throughout the day.


Output:
Coolest Year 2010 had temperature 11.202061.

Hottest Year 2014 had temperature 12.529737.

2006    11.215364
2007    12.135239
2008    12.161876
2009    12.26791
2010    11.202061
2011    11.524453
2012    11.986727
2013    11.940719
2014    12.529737
2015    12.311371
2016    11.985292
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;  
import java.util.Date;  
import java.text.ParseException;
import java.util.Locale;
import java.util.Map;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
// for global extremums
import java.util.Dictionary;
import java.util.Hashtable;
// file handling
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class AccessLog {
	public static Dictionary<Integer, Float> groupedTemperatures = new Hashtable<Integer, Float>();
	public static class WordMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, FloatWritable> {
		
		public int getYearFromDate(String date) throws ParseException {
			try {
	    	    Date dateObject = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US).parse(date); 
	    	    return (dateObject.getYear() + 1900);
		    } catch(ParseException e) {
		        throw e;
		    }
		}
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			WordMapper self = new WordMapper();
			while (tokenizer.hasMoreTokens()) {
				try {
					int year = self.getYearFromDate(tokenizer.nextToken());
					for (int i = 0;i < 2;i++)
						tokenizer.nextToken();
					output.collect(new IntWritable(year), new FloatWritable(Float.parseFloat(tokenizer.nextToken())));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				break;
			}	
			self.close();
		}
	}
	
	public static class WordReducer extends MapReduceBase implements Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
		
		
		public void reduce(IntWritable key, Iterator<FloatWritable> values, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter) throws IOException  {
			
			WordReducer self = new WordReducer();
			ArrayList<Float> temperatures = new ArrayList<Float>();
			while (values.hasNext()) {
				temperatures.add(values.next().get());
			}
			double averageTemperature = temperatures.stream().mapToDouble(val -> val).average().orElse(0.0);
			groupedTemperatures.put(key.get(), (float)averageTemperature);
			output.collect(key, new FloatWritable((float) averageTemperature));
			self.close();
		}
	}
	
	private static Integer getKeyFromValue(Float value){
        
        Integer key = null;
        
        if(value == null)
            return key;
        
        //get an iterator for the entries
        Iterator<Map.Entry<Integer, Float>> itr = ((Hashtable<Integer, Float>) groupedTemperatures).entrySet().iterator();
        
        Map.Entry<Integer, Float> entry = null;
        
        while( itr.hasNext() ){
            
            entry = itr.next();
            
            //if this entry value is equals to the value
            if(entry.getValue().equals(value)){
                return entry.getKey();
            }
        }
        
        return key;
 
    }
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(AccessLog.class);
		conf.setJobName("AccessLogs");
		
		conf.setMapperClass(WordMapper.class);
		conf.setReducerClass(WordReducer.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(FloatWritable.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		float max = Collections.max(Collections.list(groupedTemperatures.elements()));
		float min = Collections.min(Collections.list(groupedTemperatures.elements()));

		System.out.println(String.format("\n\nCoolest Year %d had temperature %f.", getKeyFromValue(min), min));
		System.out.println(String.format("\n\nHottest Year %d had temperature %f.", getKeyFromValue(max), max));
	}
}
