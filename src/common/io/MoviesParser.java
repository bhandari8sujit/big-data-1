package common.io;

import org.apache.hadoop.io.Text;

public class MoviesParser {
	
	private String tconst;
	private String title;
	private String releaseYear;
	private String genres;

	public boolean parseRecord(String record) {

		String[] item = record.split("	");
		tconst = item[0];
		title = item[1];
		releaseYear = item[2];
		genres = item[3];
		
		return true;
	}
	  
	public void parse(Text record) {
		parseRecord(record.toString());		
	}
	
	public String gettconst() {
		return tconst;
	}

	public String getMovieTitle() {
		return title;
	}
	
	public String getReleaseYear() {
		return releaseYear;
	}
	
	public String getGenres() {
		return genres;
	}
	  
}
