package fixValue;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FixJobReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	
	

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String maxString = "",tempString = "";
		 int max = 0;
		 Text longestTrajectory = new Text();
		 StringBuilder traj = new StringBuilder();
		while (values.hasNext()) {
			tempString = values.next().toString();
			if(tempString.length() > max){
				traj = new StringBuilder("");
				traj.append(tempString.toString());
				max = tempString.length();
			}
		}
		longestTrajectory.set(new Text(traj.toString()));
		output.collect(key, longestTrajectory);
	}

}
