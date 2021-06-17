import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver 
{
	public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException 
	{

		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "PRIME");  
		
		////name of Driver class
		job.setJarByClass(MyDriver.class);
		//name of mapper class
		job.setMapperClass(MyMapper.class);
		// name of reducer class
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//FileInputFormat.addInputPath(job, input_file_loc);
		//FileOutputFormat.setOutputPath(job, output_dir_loc);
		//output_dir_loc.getFileSystem(job.getConfiguration()).delete(output_dir_loc,true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
