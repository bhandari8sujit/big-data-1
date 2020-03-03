package common.io;

import org.apache.hadoop.io.Text;

public class RolesParser {
	 private String tconst;
	 private String ordering;
	 private String nconst;
	 private String category;

	 public boolean parse(String record) {

		String[] item = record.split("	");
		tconst = item[0];
		ordering = item[1];
		nconst = item[2]
		category = item[3];
		  
	    // if (record.length() < 42) {
	    //   return false;
	    // }
	    
	    // String _tconst = record.substring(0, 8);
	    // tconst = _tconst;
	    
	    // int orderingLength = 0;
	    // for(int i=9; i<record.length(); i++) {
	    // 	if(record.charAt(i) == 'n') {
	    // 		orderingLength = i-1;
	    // 	}
	    // }
	    
	    // String _ordering = record.substring(9, orderingLength);
	    // ordering = _ordering;
	    
	    // int nconstLength = orderingLength+1+8;
	    
	    // String _nconst = record.substring(orderingLength + 1, nconstLength);
	    // nconst = _nconst;
	    
	    // String _category = record.substring(nconstLength+1, record.length()-1);
	    // category = _category;
	    
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

	  public String getOrdering() {
	    return ordering;
	  }
	  
	  public String getnConst() {
		return nconst;
	  }
	  
	  public String getCategory() {
		return category;
	  }
}
