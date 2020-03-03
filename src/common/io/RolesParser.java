package common.io;

import org.apache.hadoop.io.Text;

public class RolesParser {
	private String tconst;
	private String ordering;
	private String nconst;
	private String category;

	public void parse(String record) {

		String[] item = record.split("	");
		tconst = item[0];
		ordering = item[1];
		nconst = item[2]
		category = item[3];
	
	}
	
	public void parse(Text record) {
		parse(record.toString());
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
