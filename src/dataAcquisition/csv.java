package dataAcquisition;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class csv {

  public static void main(String[] args) {

	csv obj = new csv();
	obj.run();

  }

  public void run() {

	String csvFile = "C:\\Users\\Pritam\\Downloads\\p.csv";
	BufferedReader br = null;
	String line = "";
	String cvsSplitBy = ",";

	try {

		//Map<String, String> maps = new HashMap<String, String>();
        
		ArrayList<model> ent = new ArrayList<model>();
		
		br = new BufferedReader(new FileReader(csvFile));
		while ((line = br.readLine()) != null) {

			// use comma as separator
			String[] s = line.split(cvsSplitBy);

			ent.add(new model(s[0],s[1],s[2],s[3]));

		}

		//loop map
		for (model entry : ent) {

			System.out.println("Country [id= " + entry.getId() + " , name="
				+ entry.getMovid() + "rating"+entry.getRating()+"ts"+entry.getTs()+"]");

		}

	} catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	} finally {
		if (br != null) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	System.out.println("Done");
  }

}
