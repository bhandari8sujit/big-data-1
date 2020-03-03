import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import common.io.MoviesParser;
import common.io.TextPair;

// vv JoinReducer
public class MoviesReducer extends Reducer<TextPair, Text, Text, Text> {
  
  private Map<String, String> moviesIdToName = new HashMap<String, String>();

  @Override
  protected void setup(Reducer<TextPair, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    //Load movie file, Use hashmap nconst as key
    super.setup(context);    
    BufferedReader in = null;
    try{
      in = new BufferedReader(new InputStreamReader(new FileInputStream("input/movies.tsv")));
      MoviesParser parser = new MoviesParser();
      String line;
      while ((line = in.readLine()) != null) {
        if (parser.parse(line)) {
          //TODO
          //**Test whether the release date is > 2010 ??  The test below is wrong, have to redo this*/
          // String titleCondition = parser.getReleaseYear().toString().trim().compareTo("2010") ? parser.getMovieTitle() : "";
          moviesIdToName.put(parser.gettconst(), parser.getMovieTitle());
        }
      }
    }finally{
      IOUtils.closeStream(in);
    }
    
  }

  @Override
  protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    String movieName = getMoviesName(key.toString());
  }

  public String getMoviesName(String stringNconst){
    String moviesName = moviesIdToName.get(stringNconst);
    if(moviesIdToName == null || stringNconst.trim().length() == 0){
      return stringNconst;
    }
    return moviesName;
  }
}