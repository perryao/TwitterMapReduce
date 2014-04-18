import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OnoAverage {

	public static void main(String[] args)throws Exception {
		if(args.length != 2) {
			System.err.println("Usage: Hourly Tweet Rate For @PrezOno <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(OnoAverage.class);
		job.setJobName("PrezOno Hourly Tweet Average");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(OnoAverageMapper.class);
		job.setReducerClass(OnoAverageReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}

}
