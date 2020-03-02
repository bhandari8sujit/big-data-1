import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import common.io.MoviesParser;
import common.io.TextPair;

//vv JoinRecordMapper
public class MoviesMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	//TODO
	private MoviesParser parser = new MoviesParser();
  
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  //TODO
	  parser.parse(value);
	  context.write(new TextPair(parser.gettconst(), "1"), value);
  }

}