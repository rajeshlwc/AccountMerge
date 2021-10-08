import com.sforce.soap.partner.*;
import com.sforce.soap.partner.sobject.*;
import com.sforce.ws.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.BufferedReader;
import java.io.FileReader;

public class App {

    public static void main(String[] args) {
        ConnectorConfig config = new ConnectorConfig();
        config.setUsername("rajesh@someOrgThatIHave.com");
        config.setPassword("APasswordThatIWontTellWithSecurityToken");
        try {

            BufferedReader br = new BufferedReader(new FileReader("./data.txt"));
            final CSVFormat csvFormat = CSVFormat.Builder.create().setHeader(HeaderRows.class).setDelimiter(',').setQuote('"').setRecordSeparator("\r\n").setSkipHeaderRecord(true).build();
            CSVParser parser = csvFormat.parse(br);

            PartnerConnection connection = Connector.newConnection(config);

            for(CSVRecord record : parser) {
                System.out.println(record.get("Master"));
                String sQuery = "select id from Account where id = '" + record.get("Master") + "'";
                QueryResult qr = connection.query(sQuery);
                if (qr == null || qr.getSize() == 0){
                    continue;
                }
                SObject masterRecord = new SObject();
                masterRecord.setType("Account");
                masterRecord.setId(qr.getRecords()[0].getId());

                String sDupQuery = "select id from Account where id = '" + record.get("Duplicate") + "'";
                QueryResult dqr = connection.query(sDupQuery);
                if (dqr == null || dqr.getSize() == 0){
                    continue;
                }

                MergeRequest mRequest = new MergeRequest();
                mRequest.setMasterRecord(masterRecord);
                mRequest.setRecordToMergeIds(new String[] {dqr.getRecords()[0].getId()});
                MergeResult mRes = connection.merge(new MergeRequest[]{mRequest})[0];
                if (mRes.isSuccess())
                {
                   System.out.println("Merge successful.");            
                   // Write the IDs of merged records
                   for(String mergedId : mRes.getMergedRecordIds()) {
                      System.out.println("Merged Record ID: " + mergedId);                           
                   }
                } else {
                   System.out.println("Failed to merge records. Error message: " +
                         mRes.getErrors()[0].getMessage());
                }
            }

            System.out.println("Done");          
        } catch (Exception e) {
            System.out.println("error");
            System.out.println(e.toString());
        }

    }

    
}