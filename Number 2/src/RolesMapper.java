import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import common.io.RolesParser;
import common.io.TextPair;

//public class RolesMapper extends Mapper<LongWritable, Text, TextPair, Text> {
 public class RolesMapper extends Mapper<LongWritable, Text, Text, Text> {
	private RolesParser parser = new RolesParser();

	@Override
	protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
		// String onlyDirector = parser.getCategory().equals("director") ? "director": "";
		parser.parse(value);
		boolean rowFillter = parser.getCategory().contains("director") || parser.getCategory().contains("actor") || parser.getCategory().contains("actress");
		if (rowFillter) {
			//context.write(new TextPair(parser.getnConst(), "1"), new Text(parser.gettconst()));
			context.write(new Text (parser.getnConst()), new Text(parser.gettconst() + "/" + parser.getOrdering() + "/" + parser.getCategory() + "/" + "R" ));
			
		}
	}
}