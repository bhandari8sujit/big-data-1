import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import common.io.MoviesParser;
import common.io.TextPair;

//public class MoviesReducer extends Reducer<TextPair, Text, Text, Text> {
public class MoviesReducer extends Reducer<Text, Text, Text, Text> {

	private Map<String, String> moviesIdToName = new HashMap<String, String>();

	@Override
	// protected void setup(Reducer<TextPair, Text, Text, Text>.Context context)
	// throws IOException, InterruptedException {
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream("movies.tsv")))) {
			MoviesParser parser = new MoviesParser();
			String line;
			while ((line = in.readLine()) != null) {
				if (parser.parseRecord(line)) {
					moviesIdToName.put(parser.gettconst(), parser.getMovieTitle());
				}
			}
		}
	}

	@Override
	// protected void reduce(TextPair key, Iterable<Text> values, Context
	// context) throws IOException, InterruptedException {
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String name = null;

		Set<String> directors = new HashSet<>();
		Set<String> actors = new HashSet<>();
		
		Set<String> names = new HashSet<>();

//		String line = "";

		for (Text value : values) {
//			line += " '" + value + "'";
			String[] temp = value.toString().split("/");
			if (temp[1].equalsIgnoreCase("N")) {
				name = temp[0]; //primaryName
				names.add(temp[0]);
				
			}
			// Roles Spits tconst(0) +/+ ordering(1) +/+ category(2) +/+ "R"(3)
			// else if (temp[3].equalsIgnoreCase("R")) {
			else{
				if (temp[2].equalsIgnoreCase("director")) {
					// directors.put(temp[0], key + '\t' + temp[1] + '\t' +
					directors.add(temp[0]); // value: tconst
				} else {
					// actors.put(temp[0], key + '\t' + temp[1] + '\t' +
					actors.add(temp[0]); // value: tconst
				}
			}
		}

//		if (Math.random() < 0.01)
//			context.write(key, new Text(line));

		if (name == null)			
			return;

		for (String tconst : directors) {
			if (!actors.contains(tconst) || !moviesIdToName.containsKey(tconst))
				continue;
			String outvalue = name + "\t" + moviesIdToName.get(tconst);
			context.write(new Text(""), new Text(outvalue));
		}

	}

}
