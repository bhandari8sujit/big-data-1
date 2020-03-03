package common.io;

import org.apache.hadoop.io.Text;

public class MoviesParser {
	
	private String tconst;
	private String title;
	private String releaseYear;
	private String genres;

	public boolean parse(String record) {

	String[] item = record.split("	");
	tconst = item[0];
	title = item[1];
	releaseYear = item[2];
	genres = item[3];
	
		try {
			// Integer.parseInt(releaseYear); // USAF identifiers are numeric	    
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	    
	}
	  
	public boolean parse(Text record) {
		return parse(record.toString());
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
