package nyc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
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
		String id, timepickup, timedropoff;
		Node startNode;
		Node endNode;
		String outputstring;
		StringBuilder outputline = new StringBuilder();
		// TODO Auto-generated method stub
		try {
			String line = value.toString();
			if (line.contains(",")) {
				s = line.split(",");
				if (s.length == 7) {
					try {
						id = s[0];
						timepickup = s[1];
						timedropoff = s[2];
						start_longtitude = Double.parseDouble(s[3]);
						start_latitude = Double.parseDouble(s[4]);
						end_longtitude = Double.parseDouble(s[5]);
						end_latitude = Double.parseDouble(s[6]);
						if (start_longtitude != 0 || end_longtitude != 0
								|| start_latitude != 0 || end_latitude != 0) {

							startNode = new Node(0, start_longtitude,
									start_latitude);
							endNode = new Node(0, end_longtitude, end_latitude);
							startNode.setId(mapMatching(startNode));
							endNode.setId(mapMatching(endNode));
							outputstring = startNode.getId() + " - "
									+ endNode.getId();
							word.set(new Text(outputstring));
							// Get the shortest path between start node and end
							// node
							DijkstraSP sp = new DijkstraSP(G, startNode.getId());
							Iterable<DirectedEdge> path = sp.pathTo(endNode
									.getId());

							if (path != null) {
								// System.out.println("Path " +
								// startNode.getId() +
								// "--->"
								// + endNode.getId());
								outputstring = id + "," + timepickup + "," + timedropoff+ ","
										+ start_longtitude + "&"
										+ start_latitude;
								outputline = new StringBuilder(outputstring);
								for (DirectedEdge edge : path) {
									// int start = edge.from();
									int end = edge.to();
									// Node sNode = nodeMap.get(start);
									Node eNode = nodeMap.get(end);
									// System.out.println(sNode.toString() +
									// "\t"
									// + eNode.toString());
									outputstring = "," + eNode.getLongtitude()
											+ "&" + eNode.getLatitude();
									outputline.append(outputstring);
									word.set(new Text(outputline.toString()));
									output.collect(word, one);

								}
							}

						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void readNodeFromDisk(Path p, JobConf conf) {
		nodeMap = new HashMap<Integer, Node>();
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
			Path pt = p;
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line = null;
			String[] s;
			double weight;
			int id = 0, v = 0, w = 0, graphsize = 0, graphedges = 0;

			while ((line = br.readLine()) != null) {
				if (line.contains(" ")) {
					s = line.split(" ");
					id = Integer.parseInt(s[0]);
					v = Integer.parseInt(s[1]);
					w = Integer.parseInt(s[2]);
					weight = Double.parseDouble(s[3]);

					G.addEdge(new DirectedEdge(id, v, w, weight));
				} else {
					if (graphsize == 0) {
						graphsize = Integer.parseInt(line);
						G = new EdgeWeightedDigraph(graphsize);
						continue;
					}

					if (graphedges == 0) {
						graphedges = Integer.parseInt(line);
						continue;
					}
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		/*
		 * try { Path pt = p; In in = new In(p.toString()); G = new
		 * EdgeWeightedDigraph(in, true); } catch (Exception e) {
		 * e.printStackTrace(); }
		 */
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
