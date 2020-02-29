import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.*;

public class Program1JobRunner extends Configured implements Tool{
	
	public static final String DATA_SEPERATOR = "	";
	
	public static class KeyPartitioner extends Partitioner <TextPair, Text>{

	}
	
	public int run(String[] args) throws Exception  {		

		if(args.length !=3) {
			System.err.println("Usage: Program1 Driver <input path> <outputpath>");
            // example: hadoop jar ./maxt.jar MaxTemperatureJobRunner /input /output
			System.exit(-1);
		}		

		Job job = new Job(getConf());
				
		job.setJarByClass(getClass());

		job.setJobName("Program1");		
		
		Path moviesInput = new Path(args[0]);
		Path rolesInput = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		MultipleInputs.addInputPath(job, moviesInput, TextInputFormat.class, MoviesMapper.class);
		MultipleInputs.addInputPath(job, rolesInput, TextInputFormat.class, RolesMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setPartitionerClass(KeyPartitioner.class);
		
		job.setMapOutputKeyClass(IntWritable.class);

		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);

		job.setOutputValueClass(Text.class);

		job.setMapperClass(MoviesMapper.class);
		
		job.setReducerClass(MoviesReducer.class);
		
		System.exit(job.waitForCompletion(true) ? 0:1); 	
		
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		Program1JobRunner driver = new Program1JobRunner();
		driver.run(args);
	}
}
