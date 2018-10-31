package nyc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.DirectedEdge;
import util.EdgeWeightedDigraph;
import util.In;
import util.Node;
import Dijkstra.DijkstraSP;

public class NycJobMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, IntWritable> {

	private static Map<Integer, Node> nodeMap = null;
	private static EdgeWeightedDigraph G = null;
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);

	@Override
	public void configure(JobConf job) {
		Path nodePath = new Path(job.get("nodePath"));
		readNodeFromDisk(nodePath, job);
		Path graphPath = new Path(job.get("graphPath"));
		readGraphFromDisk(graphPath, job);
	}

	@Override
	public void map(Object key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String[] s;
		double start_longtitude, start_latitude, end_longtitude, end_latitude;
		String id, time;
		Node startNode;
		Node endNode;
		String outputstring;
		StringBuilder outputline = new StringBuilder();
		// TODO Auto-generated method stub
		try {
			String line = value.toString();
			if (line.contains(",")) {
				s = line.split(",");
				s = line.split(",");
				id = s[0];
				time = s[1];
				start_longtitude = Double.parseDouble(s[2]);
				start_latitude = Double.parseDouble(s[3]);
				end_longtitude = Double.parseDouble(s[4]);
				end_latitude = Double.parseDouble(s[5]);
				if (start_longtitude != 0 || end_longtitude != 0
						|| start_latitude != 0 || end_latitude != 0) {

					startNode = new Node(0, start_longtitude, start_latitude);
					endNode = new Node(0, end_longtitude, end_latitude);
					startNode.setId(mapMatching(startNode));
					endNode.setId(mapMatching(endNode));
					// Get the shortest path between start node and end node
					DijkstraSP sp = new DijkstraSP(G, startNode.getId());
					Iterable<DirectedEdge> path = sp.pathTo(endNode.getId());
					if (path != null) {
						System.out.println("Path " + startNode.getId() + "--->"
								+ endNode.getId());
						outputstring = id + "," + time + "," + start_longtitude
								+ "&" + start_latitude;
						outputline = new StringBuilder(outputstring);
						for (DirectedEdge edge : path) {
							// int start = edge.from();
							int end = edge.to();
							// Node sNode = nodeMap.get(start);
							Node eNode = nodeMap.get(end);
							// System.out.println(sNode.toString() + "\t"
							// + eNode.toString());
							outputstring = "," + eNode.getLongtitude() + "&"
									+ eNode.getLatitude();
							outputline.append(outputstring);
							word.set(new Text(outputline.toString()));
							output.collect(word, one);

						}
					}

				}
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void readNodeFromDisk(Path p, JobConf conf) {
		try {
			Path pt = p;
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line = null;
			String[] s;
			double longtitude, latitude;
			int id;
			while ((line = br.readLine()) != null) {
				if (line.contains(" ")) {
					s = line.split(" ");
					id = Integer.parseInt(s[0]);
					latitude = Double.parseDouble(s[1]);
					longtitude = Double.parseDouble(s[2]);

					Node node = new Node(id, longtitude, latitude);
					nodeMap.put(id, node);
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void readGraphFromDisk(Path p, JobConf conf) {
		try {

			In in = new In(p.toString());
			G = new EdgeWeightedDigraph(in, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static int mapMatching(Node originalNode) {
		int nodeId = 0;
		double Mindistance = Double.MAX_VALUE;
		double dist;
		for (Map.Entry<Integer, Node> entry : nodeMap.entrySet()) {
			Node node = entry.getValue();
			dist = originalNode.getDistance(node);
			if (dist < Mindistance) {
				Mindistance = dist;
				nodeId = node.getId();
			}
		}
		return nodeId;
	}

}