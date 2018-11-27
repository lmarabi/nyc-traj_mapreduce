package nyc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MainDriver extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		int result = 0;
		//nycrowPath. 
		Path nycrowPath = args.length > 0 ? new Path(args[0]) : new Path(
				System.getProperty("user.dir") + "/data/*/*/");
		//nycTrajectoryPath
		Path nycTrajectoryPath = args.length > 1 ? new Path(args[1]) : new Path(
				System.getProperty("user.dir") + "/hdfs/hdfsoutput");
		//nodePath
		Path nodePath = args.length > 2 ? new Path(args[2]) : new Path(
				System.getProperty("user.dir") + "/hdfs/result");
		//graphPath
		Path graphPath =  args.length > 2 ? new Path(args[3]) : new Path(
				System.getProperty("user.dir") + "/hdfs/result");
		
		// First Map-Reduce Job
		JobConf conf = new JobConf(getConf(), MainDriver.class);
		//DistributedCache.addCacheFile(new Path(mbrFile).toUri(), conf);
		FileSystem outfs = nycTrajectoryPath.getFileSystem(conf);
		outfs.delete(nycTrajectoryPath, true);

		conf.setJobName("Trajectory nyc");
		conf.set("nodePath", nodePath.toString());
		conf.set("graphPath", graphPath.toString());
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setMapperClass(NycJobMapper.class);
		conf.setCombinerClass(NycJobReducer.class);
		conf.setReducerClass(NycJobReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, nycrowPath);
		FileOutputFormat.setOutputPath(conf, nycTrajectoryPath);

	    JobClient.runJob(conf).waitForCompletion();
		System.out.println("Job1 finish");
		return 0;


	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MainDriver(), args);
		System.exit(res);

	}



}
