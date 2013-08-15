package com.googlecode.fascinator.harvester.rifcs;

import java.io.File;
import java.util.Set;

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
	 * Test getting a list of object identifiers.
	 * 
	 * @throws Exception
	 */
	@Test()
	public void simple() throws Exception {
		RIFCSHarvester rifcsHarvester = getHarvester("/simple.json");
		Set<String> idList = rifcsHarvester.getObjectIdList();
		Assert.assertEquals(3, idList.size());
		for (String id : idList) {
			DigitalObject object = ram.getObject(id);
			Payload payload = object.getPayload(object.getSourceId());
			Assert.assertNotNull(payload);
		}
	}

	/**
	 * Test the registry element that contains service
	 * 
	 * @throws Exception
	 */
	@Test
	public void simpleService() throws Exception {
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
	public void simpleActivity() throws Exception {
		RIFCSHarvester rifcsHarvester = getHarvester("/activity.json");
		Set<String> idList = rifcsHarvester.getObjectIdList();
		Assert.assertEquals(1, idList.size());
		for (String id : idList) {
			DigitalObject object = ram.getObject(id);
			Payload payload = object.getPayload(object.getSourceId());
			Assert.assertNotNull(payload);
		}
	}

	/**
	 * Test the collection element that contains service
	 * 
	 * @throws Exception
	 */
	@Test
	public void simpleColllection() throws Exception {
		RIFCSHarvester rifcsHarvester = getHarvester("/collection.json");
		Set<String> idList = rifcsHarvester.getObjectIdList();
		Assert.assertEquals(1, idList.size());
		for (String id : idList) {
			DigitalObject object = ram.getObject(id);
			Payload payload = object.getPayload(object.getSourceId());
			Assert.assertNotNull(payload);
		}
	}

	/**
	 * Test the party element that contains service
	 * 
	 * @throws Exception
	 */
	@Test
	public void simpleParty() throws Exception {
		RIFCSHarvester rifcsHarvester = getHarvester("/party.json");
		Set<String> idList = rifcsHarvester.getObjectIdList();
		Assert.assertEquals(1, idList.size());
		for (String id : idList) {
			DigitalObject object = ram.getObject(id);
			Payload payload = object.getPayload(object.getSourceId());
			Assert.assertNotNull(payload);
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
}
