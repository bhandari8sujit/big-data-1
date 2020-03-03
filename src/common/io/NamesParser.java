package common.io;
import org.apache.hadoop.io.Text;

public class NamesParser {

    private String nconst;
    private String primaryName;
    private String birthYear;
    private String deathYear;

    public boolean parse(String record) {
         
		// if (record.length() < 42) {
		// 	return false;
		// }		      

		String[] item = record.split("	");
		nconst = item[0];
		primaryName = item[1];
		birthYear= item[2]
		deathYear = item[3];       
        try {
          	return true;
        } catch (NumberFormatException e) {
          	return false;
        }
       
    }
     
    public boolean parse(Text record) {
       return parse(record.toString());
    }
     
    public String getNconst() {
       return nconst;
    }

    public String getPrimaryName() {
       return primaryName;
    }
     
    public String getBirthYear() {
       return birthYear;
    }
     
    public String getDeathYear() {
       return deathYear;
	}
	
}
