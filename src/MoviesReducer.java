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
//  protected void setup(Reducer<TextPair, Text, Text, Text>.Context context) throws IOException, InterruptedException {
   protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	  
    super.setup(context);    

    try (BufferedReader in  = new BufferedReader(new InputStreamReader(new FileInputStream("movies.tsv")))) {
    	
      MoviesParser parser = new MoviesParser();
      
      String line;
      
      while ((line = in.readLine()) != null) {
        if (parser.parseRecord(line)) {
          //TODO
        	
          //**Test whether the release date is > 2010 and if it's a Western movie??  The test below is wrong, have to redo this*
          if(parser.getGenres().contains("Western") && !parser.getReleaseYear().equals("\\N") && Integer.parseInt(parser.getReleaseYear()) > 2010){
            moviesIdToName.put(parser.gettconst(), parser.getMovieTitle() + "\t" + parser.getReleaseYear());
          }          
        }
      }      
    } 

  }
  
  @Override
//  protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
   protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    // Find each tconst, then do moviesIdToName.get(tconst) If null, ignore the tconst.    
	  String name = null;
    Set<String> movieIds = new HashSet<>();
    
    for(Text value : values) {   
      String[] temp = value.toString().split("/");
      if(temp[1].equalsIgnoreCase("N")){
        name = temp[0];
      } else {
        movieIds.add(temp[0]);
      }
    }

    if (name != null) {
        for (String tconst : movieIds) {
          String existing = moviesIdToName.get(tconst);
        if(existing != null){
          String outvalue = name + "\t" + existing;
          context.write(new Text(""), new Text(outvalue));
        }
      }
    }
  }
}
 
//Roles Spits tconst + / + R  
//Name spits PrimaryName + / + N
