import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import common.io.TextPair;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  

public class Program1JobRunner extends Configured implements Tool{	
	// public static final String DATA_SEPERATOR = "	";	
//	public static class KeyPartitioner extends Partitioner<TextPair, Text> {
//		@Override
//		public int getPartition(/*[*/TextPair key/*]*/, Text value, int numPartitions) {
//		  return (/*[*/key.getFirst().hashCode()/*]*/ & Integer.MAX_VALUE) % numPartitions;
//		}
//	}
//	
	public int run(String[] args) throws Exception  {		

		if(args.length !=3) {
			System.err.println("Usage: Program1 Driver <input path> <outputpath>");
            // example: hadoop jar ./maxt.jar MaxTemperatureJobRunner /input /output
			System.exit(-1);
		}		
		
		Job job = new Job(getConf());
				
		job.setJarByClass(getClass());

		job.setJobName("Program1");		
		
		Path namesInput = new Path(args[0]);
		Path rolesInput = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		//Load roles and names and movies load in reducer
		
		MultipleInputs.addInputPath(job, namesInput, TextInputFormat.class, NamesMapper.class);
		MultipleInputs.addInputPath(job, rolesInput, TextInputFormat.class, RolesMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
//		job.setPartitionerClass(KeyPartitioner.class);
//		//TODO
//		job.setGroupingComparatorClass(TextPair.FirstComparator.class);/*]*/
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		    
		job.setReducerClass(MoviesReducer.class);

		job.setOutputKeyClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0:1); 	
		
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Program1JobRunner(), args);
	    System.exit(exitCode);
	}
	
}