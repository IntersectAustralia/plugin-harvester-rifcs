package com.googlecode.fascinator.harvester.rifcs;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonSimple;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.harvester.Harvester;
import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;

/**
 * Test cases for {@link RIFCSHarvester}
 * 
 * @author danielt@intersect.org.au
 * 
 */
public class RIFCSHarvesterTest {

	/** In memory storage */
	private Storage ram;

	/**
	 * Sets the "test.dir" and "test.cache.dir" system property for use in the
	 * JSON configuration.
	 * 
	 * @throws Exception
	 */
	@Before
	public void setup() throws Exception {
		File baseDir = new File(RIFCSHarvester.class.getResource("/").toURI());
		System.setProperty("test.dir", baseDir.getAbsolutePath());
		ram = PluginManager.getStorage("ram");
		ram.init("{}");
	}

	/**
	 * Test the registry element that contains service
	 *
	 * @throws Exception
	 */
	@Test
	public void testSampleService() throws Exception {
		RIFCSHarvester rifcsHarvester = getHarvester("/service.json");
		Set<String> idList = rifcsHarvester.getObjectIdList();
		Assert.assertEquals(1, idList.size());
		for (String id : idList) {
			DigitalObject object = ram.getObject(id);
			Payload payload = object.getPayload(object.getSourceId());
			Assert.assertNotNull(payload);
		}
	}

	/**
	 * Test the registry element that contains activity
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSampleActivity() throws Exception {
		RIFCSHarvester rifcsHarvester = getHarvester("/activity.json");
		Set<String> idList = rifcsHarvester.getObjectIdList();
		Assert.assertEquals(1, idList.size());

		String id = idList.toArray(new String[1])[0];

		rifcsHarvester.getStorage().getObject(id);
		DigitalObject object = ram.getObject(id);
		Payload payload = object.getPayload(object.getSourceId());
		Assert.assertNotNull(payload);

		JsonSimple jsonSimple = getContentInJsonSimple(payload);
		System.out.println(jsonSimple.getString("", "data", "ID"));

		Map<String, String> fieldValues = new HashMap<String, String>();
		fieldValues.put("ID", "http://purl.org/au-research/grants/nhmrc/604008");
		fieldValues.put("Submit_Year", "2005");
		fieldValues.put("Start_Year", "2004");
		fieldValues.put("Title", "The Many Rivers Diabetes Prevention Program");
		fieldValues.put("Institution", "");
		fieldValues.put("Investigators", "MQ12345678");
		fieldValues.put("Discipline", "060205");
		fieldValues.put("Description", "A type II diabetes and obesity prevention program for Primary school aged rural indigenous children");

		assertFields(jsonSimple, fieldValues);
	}

	/**
	 * Test the collection element that contains service
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSamplePartiesGroups() throws Exception {
		RIFCSHarvester rifcsHarvester = getHarvester("/Parties_Groups.json");
		Set<String> idList = rifcsHarvester.getObjectIdList();
		Assert.assertEquals(1, idList.size());

		String id = idList.toArray(new String[1])[0];

		rifcsHarvester.getStorage().getObject(id);
		DigitalObject object = ram.getObject(id);
		Payload payload = object.getPayload(object.getSourceId());
		Assert.assertNotNull(payload);

		JsonSimple jsonSimple = getContentInJsonSimple(payload);

		Map<String, String> fieldValues = new HashMap<String, String>();
		fieldValues.put("ID", "2201");
		fieldValues.put("Name", "Cognitive Science");
		fieldValues.put("Email", "cogsci@mq.edu.au");
		fieldValues.put("Phone", "+61298509599");
		fieldValues.put("Parent_Group_ID", "3200");
		fieldValues.put("URI", "http://mq.edu.au/department/3201");
		fieldValues.put("NLA_Party_Identifier", "");
		fieldValues.put("Homepage", "http://www.cogsci.mq.edu.au/");
		fieldValues.put("Description", "The Department of Cognitive Science is a research department which carries out research and PhD supervision across a wide range of domains of cognitive science, including memory, language, belief formation, perception in action, and reading. The Department hosts the ARC Centre of Excellence in Cognition and its Disorders, which brings together researchers from the Departments of Cognitive Science, Psychology and Linguistics at Macquarie University and thirteen other national and international institutions.");

		assertFields(jsonSimple, fieldValues);
	}

	/**
	 * Test the party element that contains service
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSampleParties_People() throws Exception {
		RIFCSHarvester rifcsHarvester = getHarvester("/Parties_People.json");
		Set<String> idList = rifcsHarvester.getObjectIdList();
		Assert.assertEquals(1, idList.size());

		String id = idList.toArray(new String[1])[0];

		rifcsHarvester.getStorage().getObject(id);
		DigitalObject object = ram.getObject(id);
		Payload payload = object.getPayload(object.getSourceId());
		Assert.assertNotNull(payload);

		JsonSimple jsonSimple = getContentInJsonSimple(payload);
		System.out.println(jsonSimple.getString("","data","ID"));

		Map<String, String> fieldValues = new HashMap<String, String>();
		fieldValues.put("ID", "MQ12345678");
		fieldValues.put("Given_Name", "James");
		fieldValues.put("Other_Names","Adam");
		fieldValues.put("Family_Name", "Smith");
		fieldValues.put("Pref_Name", "Jim");
		fieldValues.put("Honorific", "Dr.");
		fieldValues.put("Email","james.smith@mq.edu.au");
		fieldValues.put("Job_Title", "PhD.");
		fieldValues.put("GroupID_1", "4031");
		fieldValues.put("ANZSRC_FOR_1", "0801");
		fieldValues.put("ANZSRC_FOR_2", "0804");
		fieldValues.put("ANZSRC_FOR_3", "0602");
		fieldValues.put("URI", "http://mq.edu.au/people/staff/MQ12345678");
		fieldValues.put("NLA_Party_Identifier", "http://nla.gov.au/nla.party-1234567");
		fieldValues.put("ResearcherID", "12341234");
		fieldValues.put("openID", "http://open.id/12341234");
		fieldValues.put("Personal_Homepage", "http://www.facebook.com/john.smith99");
		fieldValues.put("Staff_Profile_Homepage", "http://mq.edu.au/staff/mq12345678");
		fieldValues.put("Description", "Senior Lecturer in Faculty of Arts");

		assertFields(jsonSimple, fieldValues);
	}

	private void assertFields(JsonSimple jsonSimple, Map<String, String> fieldValues) {
		for(String key : fieldValues.keySet()) {
			assert  fieldValues.get(key).equals(jsonSimple.getString("","data",key));
		}

	}

	/**
	 * Test wrong element in the xml, expecting {@link HarvesterException}
	 * 
	 * @throws Exception
	 */
	@Test(expected = HarvesterException.class)
	public void simpleError() throws Exception {
		RIFCSHarvester rifcsHarvester = getHarvester("/error.json");
		rifcsHarvester.getObjectIdList();
	}

	/**
	 * Gets a RIF-CS harvester instance and initialises it with the specified
	 * configuration file.
	 * 
	 * @param configFile
	 * @return RIF-CS harvester
	 * @throws Exception
	 */
	private RIFCSHarvester getHarvester(String configFile) throws Exception {
		Harvester rifcsHarvester = PluginManager.getHarvester("xml", ram);
		rifcsHarvester
				.init(new File(getClass().getResource(configFile).toURI()));
		return (RIFCSHarvester) rifcsHarvester;
	}

	private JsonSimple getContentInJsonSimple(Payload payload) {
		JsonSimple jsonSimple = null;
		try {
			InputStream inputStream = payload.open();
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
			inputStream.close();

			StringBuffer content = new StringBuffer();
			String line = "";
			while ((line = br.readLine()) != null) {
				content.append(line);
			}
			jsonSimple = new JsonSimple(content.toString());
		} catch (StorageException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return jsonSimple;
	}
}
