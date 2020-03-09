import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import common.io.NamesParser;
import common.io.TextPair;

//vv JoinRecordMapper
//public class NamesMapper extends Mapper<LongWritable, Text, TextPair, Text> {
 public class NamesMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private NamesParser parser = new NamesParser();
  
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	 parser.parse(value);
    // context.write(new TextPair(parser.getNconst(), "0"), new Text(parser.getPrimaryName())); 
    context.write(new Text(parser.getNconst()), new Text(parser.getPrimaryName() + "/" + "N" ));
  }

}