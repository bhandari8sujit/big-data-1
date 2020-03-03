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
		  
	    // if (record.length() < 42) {
	    //   return false;
	    // }
	    
	    // String _tconst = record.substring(0, 8);
	    // tconst = _tconst;
	    
	    // int titleLength = 0;
	    // for(int i=9; i<record.length(); i++) {
	    // 	if(record.charAt(i) == '1' || record.charAt(i) == '2') {
	    // 		titleLength = i-1;
	    // 	}
	    // }
	    
	    // String _titleString = record.substring(9, titleLength);
	    // title = _titleString;
	    
	    // String _year = record.substring(titleLength + 1, record.length()-1);
	    // releaseYear = _year;
	    
	    try {
//	      	Integer.parseInt(usaf); // USAF identifiers are numeric	    
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
