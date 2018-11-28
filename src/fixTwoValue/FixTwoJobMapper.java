package fixTwoValue;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FixTwoJobMapper extends MapReduceBase implements Mapper<Object, Text, Text, NullWritable> {

	private Text word = new Text();

	@Override
	public void map(Object key, Text value, OutputCollector<Text, NullWritable> output, Reporter reporter)
			throws IOException {
		
		// TODO Auto-generated method stub
		try {
			String line = value.toString();
			if (line.contains("\t")) {
				String[] s = line.split("\t");
				if (s.length == 2 && s[1].contains(",")) {
					word.set(new Text(s[1].toString()));
					output.collect(word, NullWritable.get());

				}

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
