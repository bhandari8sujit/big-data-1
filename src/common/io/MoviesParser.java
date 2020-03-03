package common.io;

import org.apache.hadoop.io.Text;

public class MoviesParser {
	
	private String tconst;
	private String title;
	private String releaseYear;
	private String genres;

	protected void parse(String record) {

		String[] item = record.split("	");
		tconst = item[0];
		title = item[1];
		releaseYear = item[2];
		genres = item[3];
	    
	}
	  
	public boolean parse(Text record) {
		parse(record.toString());
		return true;
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
