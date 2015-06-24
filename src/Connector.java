import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;

public class Connector{

	static final byte[] CF = "cf".getBytes();	// column family
	static final byte[] TYPE = "type".getBytes();	// column name

	public static void main(String[] args) throws IOException{ 
		// load rss doc
		final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = null;
		Document doc = null;
		try {
			db = dbf.newDocumentBuilder();
			doc = db.parse(new File("rss/rss.xml"));
		} catch (Exception e) {
			e.printStackTrace();
		}

		// create htable object
		final Configuration config = HBaseConfiguration.create();
		HTable hTable = new HTable(config, "events");

		// get <entry> elements from rss file
		final Element rootElement = doc.getDocumentElement();
		final NodeList entryList = rootElement.getElementsByTagName("entry");
		
		// elements will be put into hbase table
		List<Put> putList = new ArrayList<Put>();

		// process each <entry> separately
		for (int i = 0; i < entryList.getLength(); i++) {
			final Element entry = (Element) entryList.item(i);
			final String cellarId = entry.getElementsByTagName("id").
					item(0).getTextContent();
			final String type = entry.getElementsByTagName("notifEntry:type").
					item(0).getTextContent();
			Put put = new Put(Bytes.toBytes(cellarId.substring(7))); 
			put.add(CF, TYPE, Bytes.toBytes(type));
			putList.add(put);
		}
		// store results in htable
		hTable.put(putList);
		// close connection
		hTable.close();
		System.out.println("DONE...");
	}
}