package fixValue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FixDriver extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		int result = 0;
		//nycrowPath. 
		Path input = args.length > 0 ? new Path(args[0]) : new Path(
				System.getProperty("user.dir") + "/data/*/*/");
		//nycTrajectoryPath
		Path output = args.length > 1 ? new Path(args[1]) : new Path(
				System.getProperty("user.dir") + "/hdfs/hdfsoutput");
		
		
		// First Map-Reduce Job
		JobConf conf = new JobConf(getConf(), FixDriver.class);
		//DistributedCache.addCacheFile(new Path(mbrFile).toUri(), conf);
		FileSystem outfs = output.getFileSystem(conf);
		outfs.delete(output, true);

		conf.setJobName("FIX traj");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(FixJobMapper.class);
		
		conf.setCombinerClass(FixJobReducer.class);
		conf.setReducerClass(FixJobReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//conf.setNumReduceTasks(1);
		

		FileInputFormat.setInputPaths(conf, input);
		FileOutputFormat.setOutputPath(conf, output);

	    JobClient.runJob(conf).waitForCompletion();
		System.out.println("Job1 finish");
		return 0;


	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new FixDriver(), args);
		System.exit(res);

	}



}
