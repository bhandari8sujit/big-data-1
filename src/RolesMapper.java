import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import common.io.RolesParser;
import common.io.TextPair;

public class RolesMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	private RolesParser parser = new RolesParser();

	@Override
	protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
		// String onlyDirector = parser.getCategory().equals("director") ? "director": "";
		if (parser.getCategory().equals("director")) {
			context.write(new TextPair(parser.getnConst(), "1"), new Text(parser.gettconst()));
		}
	}
}