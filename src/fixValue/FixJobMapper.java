package fixValue;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FixJobMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, Text> {

	private Text word = new Text();
	private Text traj = new Text();

	@Override
	public void map(Object key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// TODO Auto-generated method stub
		try {
			String line = value.toString();
			if (line.contains("\t")) {
				String[] s = line.split("\t");
				if (s[0].contains(",")) {
					traj.set(new Text(s[0].toString()));
					String[] comma = s[0].split(",");
					word.set(new Text(comma[0]+","+comma[1]));
					output.collect(word,traj);
				
				}

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
