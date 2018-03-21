import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class CosineMapRed{

	public static class Map1 extends Mapper<LongWritable,Text,Text,Text>{
		Text word = new Text();

		@Override
		public void map(LongWritable key, Text Value, Context context ) throws IOException, InterruptedException{
			Text one = new Text("1");
			StringTokenizer stringToken = new StringTokenizer(Value.toString());

			while(stringToken.hasMoreTokens()){
				word.set(stringToken.nextToken());
				context.write(word, one);
			}
		}

	}

	public static class Map2 extends Mapper<LongWritable,Text,Text,Text>{
		Text word = new Text();

		@Override
		public void map(LongWritable key, Text Value, Context context1 ) throws IOException, InterruptedException{
			Text two = new Text("2");
			StringTokenizer stringToken = new StringTokenizer(Value.toString());

			while(stringToken.hasMoreTokens()){
				word.set(stringToken.nextToken());
				context1.write(word, two);
			}
		}		
	}



	public static class Reduce1 extends Reducer<Text, Text, Text, Text>{

		Text one = new Text("1");
		Text two = new Text("2");

		Text value1 = new Text();
		Text value2 = new Text();

		static int i=0;
		static ArrayList<Integer> al1 = new ArrayList<Integer>(500);
		static ArrayList<Integer> al2 = new ArrayList<Integer>(500);		


		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			int sum=0;
			// al1.add(i,0);
			// al2.add(i,0);
			
			for(Text val: values){
				sum += Integer.parseInt(val.toString());
			}
				if(sum == 1){
					al1.add(i,1);
					al2.add(i,0);
				}
				else if(sum == 2){
					al1.add(i,0);
					al2.add(i,1);
				}
				else if(sum == 3){
					al1.add(i,1);
					al2.add(i,1);
				}
			i++;			

			value1 = new Text(al1.toString());
			value2 = new Text(al2.toString());

			//cleanUp(context);
			context.write(one, value1);
			context.write(two, value2);

		}

	}
	static int maxLength=0;

	public static class Map3 extends Mapper<Text, Text, Text, Text>{

		Text key1 = new Text();
		Text value1 = new Text();
		static int i=0;
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			key1=key;
			value1=value;
			i++;
			if(maxLength < value.getLength()){
				maxLength = value.getLength();
			}
			context.write(key,value);
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text>{
		static Text result = new Text();
		int rem=0;
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
				for(Text val : values){
					if(maxLength == val.getLength())
						context.write(key, val);
					
				}
		}
	}

	public static class Map4 extends Mapper<Text, Text, Text, Text>{

		public void map(Text key,Text values, Context context) throws IOException, InterruptedException{
			String[] vectorElements = (values.toString().substring(1,values.toString().length()-1)).split(",");
			for(int i=0;i<vectorElements.length;i++)
				context.write(new Text(i+""), new Text(vectorElements[i]));
		}
	}


	public static class Reduce3 extends Reducer<Text, Text, Text, Text>{
		int temp = 1;
		Text one = new Text("1");
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text val : values){
				temp = temp * Integer.parseInt(val.toString().trim());
				// result = result + temp;
			}

			context.write(one, new Text(temp+""));
			temp = 1;
		}
	}

	public static class Map5 extends Mapper<Text, Text, Text, Text>{
		Text one = new Text("1");
		Text two = new Text("2");
		int sum = 0;

		public void map(Text key,Text values, Context context) throws IOException, InterruptedException{
			String[] vectorElements = (values.toString().substring(1,values.toString().length()-1)).split(",");
			for(int i=0 ; i<vectorElements.length ; i++)
				sum = sum + (Integer.parseInt(vectorElements[i].trim()) * Integer.parseInt(vectorElements[i].trim()));		

			if(key.toString().equals("1")) {
				context.write(one, new Text(Double.toString(Math.sqrt(sum))));
				sum = 0;
			}	
			if(key.toString().equals("2")) {
				context.write(two, new Text(Double.toString(Math.sqrt(sum))));
				sum = 0;
			}		
		}
	}

	public static class Reducer4 extends Reducer<Text, Text, Text, Text>{
		Text two = new Text("2");
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text val : values){
				context.write(two, val);
			}
		}
	}

	public static class Map6 extends Mapper<Text, Text, Text, Text>{
		public void map(Text key,Text values, Context context) throws IOException, InterruptedException{
			context.write(key, values);
		}
	}

	public static class Map7 extends Mapper<Text, Text, Text, Text>{
		public void map(Text key,Text values, Context context) throws IOException, InterruptedException{
			context.write(key, values);
		}
	}	

	public static class Reducer5 extends Reducer<Text, Text, Text, Text>{
		int sum = 0;
		Double product = 1.0;
		Text one = new Text("1");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			if(key.toString().equals("1")){
				for(Text val : values)
					sum += Integer.parseInt(val.toString());
				context.write(one, new Text(sum+""));
			}

			if(key.toString().equals("2")){
				for(Text val : values)
					product *= Double.parseDouble(val.toString());
				context.write(one, new Text(product+""));
			}
		}
	}

	public static class Map8 extends Mapper<Text, Text, Text, Text>{
		public void map(Text key,Text values, Context context) throws IOException, InterruptedException{
			context.write(key, values);
		}
	}	

	public static class Reducer6 extends Reducer<Text, Text, Text, Text>{
		Text two = new Text("Similarity : ");
		Double result = 0.0;
		Double[] arr = new Double[2];
		int i=0;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text val : values){
				arr[i] = Double.parseDouble(val.toString());
				i++;
			}
			result = arr[1]/arr[0];
			context.write(two, new Text(result+""));

		}
	}

	public static void main(String args[]) throws Exception{
		JobControl jobControl = new JobControl("jobchain");

		Configuration conf = new Configuration();
		Job job = new Job(conf,"Vector generation");

		// String outputFinalDir = "/outputtt/Final";

		job.setJarByClass(CosineMapRed.class);
		// job.setNumReduceTasks(2);

		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Map1.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Map2.class);
		FileOutputFormat.setOutputPath(job,new Path(args[2]));

		// job.setCombinerClass(Reduce.class);

		// Configuration red1Conf = new Configuration(false);
		// ChainReducer.setReducer(job, Reduce1.class, Text.class, Text.class, Text.class, Text.class, red1Conf);

		// Configuration map2Conf = new Configuration(false);
		// ChainReducer.addMapper(job, Map3.class, Text.class, Text.class, Text.class, Text.class, map2Conf);


		job.setReducerClass(Reduce1.class);


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);

		if(success){
			Job newJob = new Job(conf,"Vector Extraction");

			newJob.setMapperClass(Map3.class);
			// newJob.setCombinerClass(Reduce2.class);
			newJob.setReducerClass(Reduce2.class);

			newJob.setInputFormatClass(KeyValueTextInputFormat.class);
			FileInputFormat.addInputPath(newJob, new Path(args[2]));
			FileOutputFormat.setOutputPath(newJob, new Path(args[3]));


			newJob.setOutputKeyClass(Text.class);
			newJob.setOutputValueClass(Text.class);
			boolean success1 = newJob.waitForCompletion(true);

			if(success1){
				Job cosineJob = new Job(conf,"Vector Multiplication");

				cosineJob.setMapperClass(Map4.class);
				cosineJob.setReducerClass(Reduce3.class);

				cosineJob.setInputFormatClass(KeyValueTextInputFormat.class);
				FileInputFormat.addInputPath(cosineJob, new Path(args[3]));
				FileOutputFormat.setOutputPath(cosineJob, new Path(args[4]));

				cosineJob.setOutputKeyClass(Text.class);
				cosineJob.setOutputValueClass(Text.class);
				boolean success2 = cosineJob.waitForCompletion(true);

				if(success2){
					Job cosineJob1 = new Job(conf, "Euclidean distance");

					cosineJob1.setMapperClass(Map5.class);
					cosineJob1.setReducerClass(Reducer4.class);

					cosineJob1.setInputFormatClass(KeyValueTextInputFormat.class);
					FileInputFormat.addInputPath(cosineJob1, new Path(args[3]));
					FileOutputFormat.setOutputPath(cosineJob1, new Path(args[5]));

					cosineJob1.setOutputKeyClass(Text.class);
					cosineJob1.setOutputValueClass(Text.class);
					boolean success3 = cosineJob1.waitForCompletion(true);

					if(success3){
						Job finalJob = new Job(conf,"Division");

						// finalJob.setMapperClass(Map6.class);
						MultipleInputs.addInputPath(finalJob,new Path(args[4]),KeyValueTextInputFormat.class,Map6.class);
						MultipleInputs.addInputPath(finalJob,new Path(args[5]),KeyValueTextInputFormat.class,Map7.class);
						FileOutputFormat.setOutputPath(finalJob,new Path(args[6]));

						finalJob.setReducerClass(Reducer5.class);

						finalJob.setOutputKeyClass(Text.class);
						finalJob.setOutputValueClass(Text.class);
						boolean success4 = finalJob.waitForCompletion(true);

						if(success4){
							Job solJob = new Job(conf, "Euclidean distance");

							solJob.setMapperClass(Map8.class);
							solJob.setReducerClass(Reducer6.class);

							solJob.setInputFormatClass(KeyValueTextInputFormat.class);
							FileInputFormat.addInputPath(solJob, new Path(args[6]));
							FileOutputFormat.setOutputPath(solJob, new Path(args[7]));

							solJob.setOutputKeyClass(Text.class);
							solJob.setOutputValueClass(Text.class);
							success4 = solJob.waitForCompletion(true);
						}
					}

				}
			}
		}

		
		// ControlledJob contJob = new ControlledJob(conf);
		// contJob.setJob(contJob);


		// System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}