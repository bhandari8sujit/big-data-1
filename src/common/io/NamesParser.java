package common.io;
import org.apache.hadoop.io.Text;

public class NamesParser {

   private String nconst;
   private String primaryName;
   private String birthYear;
   private String deathYear;

   protected void parse(String record) {
	   String[] item = record.split("\\\t");		
	   nconst = item[0];
		primaryName = item[1];
		birthYear= item[2];
		deathYear = item[3];   
   }
     
   public void parse(Text record) {
      parse(record.toString());
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
