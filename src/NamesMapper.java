import common.io.TextPair;

//vv JoinRecordMapper
public class NamesMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	
	private NamesParser parser = new NamesParser();
  
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	  parser.parse(value);
    context.write(new TextPair(parser.getNconst(), "0"), new Text(parser.getPrimaryName())); 
  }

}
