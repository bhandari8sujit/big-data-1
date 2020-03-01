import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import common.io.RolesParser;
import common.io.TextPair;

// vv JoinStationMapper
public class RolesMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	//TODO
	private RolesParser parser = new RolesParser();

	@Override
	protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  //TODO
	  if (parser.parse(value)) {
		  context.write(new TextPair(parser.gettconst(), "0"), value);
//        new Text(parser.get()));
	  }
	}
}