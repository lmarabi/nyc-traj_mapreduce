package fixValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.DirectedEdge;
import util.EdgeWeightedDigraph;
import util.Node;
import Dijkstra.DijkstraSP;

public class FixJobMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, NullWritable> {

	private static Map<Integer, Node> nodeMap = null;
	private static EdgeWeightedDigraph G = null;
	private Text word = new Text();

	@Override
	public void map(Object key, Text value,
			OutputCollector<Text, NullWritable> output, Reporter reporter)
			throws IOException {
		String[] s;
		// TODO Auto-generated method stub
		try {
			String line = value.toString();
			if (line.contains("\t")) {
				s = line.split("\t");
				if (s[0].contains(",")) {
					word.set(new Text(s[0].toString()));
					output.collect(word, NullWritable.get());
				}

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
