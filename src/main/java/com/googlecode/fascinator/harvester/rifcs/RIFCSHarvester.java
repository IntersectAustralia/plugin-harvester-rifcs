package com.googlecode.fascinator.harvester.rifcs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.ands.rifcs.base.AccessPolicy;
import org.ands.rifcs.base.Activity;
import org.ands.rifcs.base.Address;
import org.ands.rifcs.base.AddressPart;
import org.ands.rifcs.base.Arg;
import org.ands.rifcs.base.CitationDate;
import org.ands.rifcs.base.CitationInfo;
import org.ands.rifcs.base.CitationMetadata;
import org.ands.rifcs.base.Collection;
import org.ands.rifcs.base.CommonDateElement;
import org.ands.rifcs.base.Contributor;
import org.ands.rifcs.base.Coverage;
import org.ands.rifcs.base.Description;
import org.ands.rifcs.base.Electronic;
import org.ands.rifcs.base.ExistenceDate;
import org.ands.rifcs.base.Identifier;
import org.ands.rifcs.base.Location;
import org.ands.rifcs.base.Name;
import org.ands.rifcs.base.NamePart;
import org.ands.rifcs.base.Party;
import org.ands.rifcs.base.Physical;
import org.ands.rifcs.base.RIFCS;
import org.ands.rifcs.base.RIFCSElement;
import org.ands.rifcs.base.RIFCSException;
import org.ands.rifcs.base.RIFCSWrapper;
import org.ands.rifcs.base.RegistryObject;
import org.ands.rifcs.base.RelatedInfo;
import org.ands.rifcs.base.RelatedObject;
import org.ands.rifcs.base.Relation;
import org.ands.rifcs.base.Right;
import org.ands.rifcs.base.RightsInfo;
import org.ands.rifcs.base.RightsTypedInfo;
import org.ands.rifcs.base.Service;
import org.ands.rifcs.base.Spatial;
import org.ands.rifcs.base.Subject;
import org.ands.rifcs.base.Temporal;
import org.ands.rifcs.base.TemporalCoverageDate;
import org.ands.rifcs.ch.RIFCSReader;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.harvester.impl.GenericHarvester;
import com.googlecode.fascinator.common.storage.StorageUtils;

/**
 * Harvester for RIF-CS xml files.
 * <p>
 * Configuration options:
 * <ul>
 * <li>fileLocation: The location of the csv file (required)</li>
 * <li>payloadId: The payload identifier used to store the JSON data (defaults
 * to "metadata.json")</li>
 * </ul>
 * <p>
 * This plugin is written based on RIF-CS schema version 1.3
 * <p>
 * Based on Greg Pendlebury's CallistaHarvester.
 * 
 * @author danielt@intersect.org.au
 * 
 */
public class RIFCSHarvester extends GenericHarvester {

	private RIFCSReader rifcsReader;
	private String filename;
	private RIFCS rifcs;
	private boolean hasMore;
	private int currentId = 1;
	private Logger log = LoggerFactory.getLogger(RIFCSHarvester.class);
	private String payloadId;
	private static final String DEFAULT_PAYLOAD_ID = "metadata.json";

	public RIFCSHarvester() {
		super("xml", "RIF-CS Harvester");
	}

	@Override
	public void init() throws HarvesterException {
		JsonSimple options = new JsonSimple(getJsonConfig().getObject(
				"harvester", "xml"));

		String filePath = options.getString(null, "fileLocation");
		if (filePath == null) {
			throw new HarvesterException("No data file provided!");
		}
		File rifcsDataFile = new File(filePath);
		if (rifcsDataFile == null || !rifcsDataFile.exists()) {
			throw new HarvesterException("Could not find rif-cs file '"
					+ filePath + "'");
		}
		filename = rifcsDataFile.getName();
		rifcsReader = new RIFCSReader();
		payloadId = options.getString(DEFAULT_PAYLOAD_ID, "payloadId");
		try {
			rifcsReader.mapToDOM(new FileInputStream(rifcsDataFile));
			Document document = rifcsReader.getDocument();
			RIFCSWrapper rifcsWrapper = new RIFCSWrapper(document);
			rifcs = rifcsWrapper.getRIFCSObject();
			hasMore = true;
		} catch (Exception e) {
			throw new HarvesterException(e);
		}

	}

	@Override
	public Set<String> getObjectIdList() throws HarvesterException {
		Set<String> objectIdList = new HashSet<String>();
		Map<String, RegistryObject> registryObjects = rifcs
				.getRegistryObjects();
		for (Entry<String, RegistryObject> entry : registryObjects.entrySet()) {
			objectIdList.add(parseRegistryObject(entry.getValue()));
		}
		hasMore = false;
		if (objectIdList.size() > 0) {
			log.debug("Created {} objects", objectIdList.size());
		}
		return objectIdList;
	}

	/**
	 * Obtain the registry object of rif-cs xml
	 * 
	 * @param registryObject
	 * @throws HarvesterException
	 */
	private String parseRegistryObject(RegistryObject registryObject)
			throws HarvesterException {
		String recordId = Integer.toString(currentId++);
		JsonObject registryObjectJson = new JsonObject();
		try {
			registryObjectJson.put("group", registryObject.getGroup());
			registryObjectJson.put("key", registryObject.getKey());
			registryObjectJson.put("originatingSource",
					registryObject.getOriginatingSource());
			RIFCSElement element = registryObject.getClassObject();
			parseRIFCSElement(registryObjectJson, element);
		} catch (RIFCSException e) {
			throw new HarvesterException(e);
		}
		log.debug(registryObjectJson.toString());
		JsonObject meta = new JsonObject();
		meta.put("dc.identifier", recordId);

		String oid = DigestUtils.md5Hex(filename
				+ registryObjectJson.get("group") + recordId);
		storeJsonInObject(registryObjectJson, meta, oid);
		return oid;
	}

	/**
	 * Get the child element of registry object which consists of one of
	 * Activity, Collection, Party, or Service.
	 * 
	 * @param registryObjectJson
	 * @param element
	 * @throws HarvesterException
	 */
	private void parseRIFCSElement(JsonObject registryObjectJson,
			RIFCSElement element) throws HarvesterException {
		if (element instanceof Activity) {
			Activity activity = (Activity) element;
			registryObjectJson.put(
					"activity",
					parseElement(activity.getType(), activity.getIdentifiers(),
							activity.getNames(), activity.getLocations(),
							activity.getRelatedObjects(),
							activity.getSubjects(), activity.getDescriptions(),
							activity.getCoverage(), activity.getRelatedInfo(),
							activity.getRights(), activity.getExistenceDates(),
							null, null));
		} else if (element instanceof Collection) {
			Collection collection = (Collection) element;
			registryObjectJson.put(
					"collection",
					parseElement(collection.getType(),
							collection.getIdentifiers(), collection.getNames(),
							collection.getLocations(),
							collection.getRelatedObjects(),
							collection.getSubjects(),
							collection.getDescriptions(),
							collection.getCoverage(),
							collection.getRelatedInfo(),
							collection.getRightsList(), null,
							collection.getCitationInfos(), null));
		} else if (element instanceof Party) {
			Party party = (Party) element;
			registryObjectJson.put(
					"party",
					parseElement(party.getType(), party.getIdentifiers(),
							party.getNames(), party.getLocations(),
							party.getRelatedObjects(), party.getSubjects(),
							party.getDescriptions(), party.getCoverage(),
							party.getRelatedInfo(), party.getRights(),
							party.getExistenceDates(), null, null));
		} else if (element instanceof Service) {
			Service service = (Service) element;
			registryObjectJson.put(
					"service",
					parseElement(service.getType(), service.getIdentifiers(),
							service.getNames(), service.getLocations(),
							service.getRelatedObjects(), service.getSubjects(),
							service.getDescriptions(), service.getCoverage(),
							service.getRelatedInfo(), service.getRights(),
							service.getExistenceDates(), null,
							service.getAccessPolicies()));
		} else {
			throw new HarvesterException(
					"Wrong element found, only supports activity, collection, party, or service");
		}
	}

	private JsonObject parseElement(String type, List<Identifier> identifiers,
			List<Name> names, List<Location> locations,
			List<RelatedObject> relatedObjects, List<Subject> subjects,
			List<Description> descriptions, List<Coverage> coverages,
			List<RelatedInfo> relatedInfos, List<Right> rights,
			List<ExistenceDate> existenceDates,
			List<CitationInfo> citationInfos, List<AccessPolicy> accessPolicies) {
		JsonObject data = new JsonObject();

		// get the type for rifcs element
		if (type != null && !type.isEmpty()) {
			data.put("type", type);
		}

		// get the identifiers for rifcs element
		if (identifiers != null && !identifiers.isEmpty()) {
			getIdentifiersForRIFCSElement(identifiers, data);
		}

		// get names and nameparts for rifcs element
		if (names != null && !names.isEmpty()) {
			getNamesForRIFCSElement(names, data);
		}

		// get locations for rifcs element
		if (locations != null && !locations.isEmpty()) {
			getLocationsForRIFCSElement(locations, data);
		}

		// get the related objects for rifcs element
		if (relatedObjects != null && !relatedObjects.isEmpty()) {
			getRelatedObjectsForRIFCSElement(relatedObjects, data);
		}

		// get the subjects for rifcs element
		if (subjects != null && !subjects.isEmpty()) {
			getSubjectsForRIFCSElement(subjects, data);
		}

		// get the descriptions for rifcs element
		if (descriptions != null && !descriptions.isEmpty()) {
			getDescriptionsForRIFCSElement(descriptions, data);
		}

		// get the coverages for rifcs element
		if (coverages != null && !coverages.isEmpty()) {
			getCoveragesForRIFCSElement(coverages, data);
		}

		// get the related info for rifcs element
		if (relatedInfos != null && !relatedInfos.isEmpty()) {
			getRelatedInfosForRIFCSElement(relatedInfos, data);
		}

		// get the right for rifcs element
		if (rights != null && !rights.isEmpty()) {
			getRightsForRIFCSElement(rights, data);
		}

		// get the existance dates for rifcs element
		if (existenceDates != null && !existenceDates.isEmpty()) {
			getExistenceDatesForRIFCSElement(existenceDates, data);
		}

		// get the citation info for rifcs element
		if (citationInfos != null && !citationInfos.isEmpty()) {
			getCitationInfosForRIFCSElement(citationInfos, data);
		}

		// get the access policy for rifcs element
		if (accessPolicies != null && !accessPolicies.isEmpty()) {
			getAccessPoliciesForRIFCSElement(accessPolicies, data);
		}

		return data;
	}

	// ========= Identifiers =============

	/**
	 * Obtaining identifiers element of the rif-cs xml
	 * 
	 * @param identifiers
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getIdentifiersForRIFCSElement(List<Identifier> identifiers,
			JsonObject data) {
		JSONArray identifiersArray = new JSONArray();
		for (Identifier identifier : identifiers) {
			JsonObject identifierObject = new JsonObject();
			identifierObject.put("value", identifier.getValue());
			identifierObject.put("type", identifier.getType());
			identifiersArray.add(identifierObject);
		}
		data.put("identifier", identifiersArray);
	}

	// ========= Name and NamePart =============
	/**
	 * Obtaining names element of the rif-cs xml
	 * 
	 * @param names
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getNamesForRIFCSElement(List<Name> names, JsonObject data) {
		if (names.size() == 1) {
			Name name = names.get(0);
			JsonObject nameObject = getNameForRIFCSElement(name);
			data.put("name", nameObject);
		} else {
			JSONArray namesArray = new JSONArray();
			for (Name name : names) {
				namesArray.add(getNameForRIFCSElement(name));
			}
			data.put("name", namesArray);
		}
	}

	/**
	 * Get the namePart for name element
	 * 
	 * @param name
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private JsonObject getNameForRIFCSElement(Name name) {
		JsonObject nameObject = new JsonObject();
		if (!name.getType().isEmpty()) {
			nameObject.put("type", name.getType());
		}
		if (name.getNameParts().size() == 1) {
			NamePart namePart = name.getNameParts().get(0);
			if (namePart.getType() != null && !namePart.getType().isEmpty()) {
				JsonObject namePartObject = new JsonObject();
				namePartObject.put("type", namePart.getType());
				namePartObject.put("value", namePart.getValue());
				nameObject.put("namePart", namePartObject);
			} else {
				nameObject.put("namePart", namePart.getValue());
			}
		} else {
			JSONArray namePartArray = new JSONArray();
			for (NamePart namePart : name.getNameParts()) {
				JsonObject namePartObject = new JsonObject();
				namePartObject.put("type", namePart.getType());
				namePartObject.put("value", namePart.getValue());
				namePartArray.add(namePartObject);
			}
			nameObject.put("namePart", namePartArray);
		}
		return nameObject;
	}

	// ========= Locations =============

	/**
	 * Obtaining locations element of the rif-cs xml
	 * 
	 * @param locations
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getLocationsForRIFCSElement(List<Location> locations,
			JsonObject data) {
		if (locations.size() == 1) {
			Location location = locations.get(0);
			JsonObject locationObject = getLocationForRIFCSElement(location);
			data.put("location", locationObject);
		} else {
			JSONArray locationsArray = new JSONArray();
			for (Location location : locations) {
				locationsArray.add(getLocationForRIFCSElement(location));
			}
			data.put("location", locationsArray);
		}
	}

	/**
	 * Get the location element
	 * 
	 * @param location
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private JsonObject getLocationForRIFCSElement(Location location) {
		JsonObject locationObject = new JsonObject();
		if (!location.getDateFrom().isEmpty()) {
			locationObject.put("dateFrom", location.getDateFrom());
		}
		if (!location.getDateTo().isEmpty()) {
			locationObject.put("dateTo", location.getDateTo());
		}

		if (location.getAddresses() != null
				&& !location.getAddresses().isEmpty()) {
			if (location.getAddresses().size() == 1) {
				Address address = location.getAddresses().get(0);
				JsonObject addressObject = getAddressForRIFCSElement(address);
				locationObject.put("address", addressObject);
			} else {
				JSONArray addressArray = new JSONArray();
				for (Address address : location.getAddresses()) {
					addressArray.add(getAddressForRIFCSElement(address));
				}
				locationObject.put("address", addressArray);
			}
		}
		if (location.getSpatials() != null && !location.getSpatials().isEmpty()) {
			if (location.getSpatials().size() == 1) {
				Spatial spatial = location.getSpatials().get(0);
				JsonObject spatialObject = getSpatialForRIFCSElement(spatial);
				locationObject.put("spatial", spatialObject);
			} else {
				JSONArray spatialArray = new JSONArray();
				for (Spatial spatial : location.getSpatials()) {
					spatialArray.add(getSpatialForRIFCSElement(spatial));
				}
				locationObject.put("spatial", spatialArray);
			}
		}
		return locationObject;
	}

	/**
	 * Get the address for location element
	 * 
	 * @param address
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private JsonObject getAddressForRIFCSElement(Address address) {
		JsonObject addressObject = new JsonObject();
		if (address.getElectronics() != null
				&& !address.getElectronics().isEmpty()) {
			if (address.getElectronics().size() == 1) {
				Electronic electronic = address.getElectronics().get(0);
				JsonObject electronicObject = getElectronicForRIFCSElement(electronic);
				addressObject.put("electronic", electronicObject);
			} else {
				JSONArray electronicArray = new JSONArray();
				for (Electronic electronic : address.getElectronics()) {
					electronicArray
							.add(getElectronicForRIFCSElement(electronic));
				}
				addressObject.put("electronic", electronicArray);
			}
		}
		if (address.getPhysicalAddresses() != null
				&& !address.getPhysicalAddresses().isEmpty()) {
			if (address.getPhysicalAddresses().size() == 1) {
				Physical physical = address.getPhysicalAddresses().get(0);
				JsonObject physicalObject = getPhysicalForRIFCSElement(physical);
				addressObject.put("electronic", physicalObject);
			} else {
				JSONArray physicalArray = new JSONArray();
				for (Physical physical : address.getPhysicalAddresses()) {
					physicalArray.add(getPhysicalForRIFCSElement(physical));
				}
				addressObject.put("electronic", physicalArray);
			}
		}
		return addressObject;
	}

	/**
	 * Get the electronic address for address element
	 * 
	 * @param electronic
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private JsonObject getElectronicForRIFCSElement(Electronic electronic) {
		JsonObject electronicObject = new JsonObject();
		electronicObject.put("value", electronic.getValue());
		if (!electronic.getType().isEmpty()) {
			electronicObject.put("type", electronic.getType());
		}

		if (electronic.getArgs() != null && !electronic.getArgs().isEmpty()) {
			if (electronic.getArgs().size() == 1) {
				Arg arg = electronic.getArgs().get(0);
				JsonObject argObject = getArgForRIFCSElement(arg);
				electronicObject.put("arg", argObject);
			} else {
				JSONArray argArray = new JSONArray();
				for (Arg arg : electronic.getArgs()) {
					argArray.add(getArgForRIFCSElement(arg));
				}
				electronicObject.put("arg", argArray);
			}
		}
		return electronicObject;
	}

	/**
	 * Get the arg for electronic address element
	 * 
	 * @param arg
	 * @return {@link JsonObject}
	 */
	private JsonObject getArgForRIFCSElement(Arg arg) {
		JsonObject argObject = new JsonObject();
		argObject.put("required", arg.getRequired());
		argObject.put("type", arg.getType());
		argObject.put("use", arg.getUse());
		argObject.put("name", arg.getName());
		return argObject;
	}

	/**
	 * Get physical address for address element
	 * 
	 * @param physical
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private JsonObject getPhysicalForRIFCSElement(Physical physical) {
		JsonObject physicalObject = new JsonObject();
		if (!physical.getType().isEmpty()) {
			physicalObject.put("type", physical.getType());
		}

		if (physical.getAddressParts().size() == 1) {
			AddressPart addressPart = physical.getAddressParts().get(0);
			JsonObject addressPartObject = getAddressPartForRIFCSElement(addressPart);
			physicalObject.put("addressPart", addressPartObject);
		} else {
			JSONArray addressPartArray = new JSONArray();
			for (AddressPart addressPart : physical.getAddressParts()) {
				addressPartArray
						.add(getAddressPartForRIFCSElement(addressPart));
			}
			physicalObject.put("addressPart", addressPartArray);
		}
		return physicalObject;
	}

	/**
	 * Get address part for physical address element
	 * 
	 * @param addressPart
	 * @return {@link JsonObject}
	 */
	private JsonObject getAddressPartForRIFCSElement(AddressPart addressPart) {
		JsonObject addressPartObject = new JsonObject();
		addressPartObject.put("type", addressPart.getType());
		addressPartObject.put("value", addressPart.getValue());
		return addressPartObject;
	}

	/**
	 * Get spatial for location element
	 * 
	 * @param spatial
	 * @return {@link JsonObject}
	 */
	private JsonObject getSpatialForRIFCSElement(Spatial spatial) {
		JsonObject spatialObject = new JsonObject();
		spatialObject.put("type", spatial.getType());
		spatialObject.put("value", spatial.getValue());
		return spatialObject;
	}

	// ========= Related Objects =============

	/**
	 * Obtaining related objects element of the rif-cs xml
	 * 
	 * @param relatedObjects
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getRelatedObjectsForRIFCSElement(
			List<RelatedObject> relatedObjects, JsonObject data) {
		if (relatedObjects.size() == 1) {
			RelatedObject relatedObject = relatedObjects.get(0);
			JsonObject relatedJsonObject = getRelatedObjectForRIFCSElement(relatedObject);
			data.put("relatedObject", relatedJsonObject);
		} else {
			JSONArray relatedObjectArray = new JSONArray();
			for (RelatedObject relatedObject : relatedObjects) {
				relatedObjectArray
						.add(getRelatedObjectForRIFCSElement(relatedObject));
			}
			data.put("relatedObject", relatedObjectArray);
		}
	}

	/**
	 * Get the relation for related object element
	 * 
	 * @param relatedObject
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private JsonObject getRelatedObjectForRIFCSElement(
			RelatedObject relatedObject) {
		JsonObject relatedJsonObject = new JsonObject();
		relatedJsonObject.put("key", relatedObject.getKey());
		if (relatedObject.getRelations().size() == 1) {
			Relation relation = relatedObject.getRelations().get(0);
			JsonObject relationObject = getRelationObjectForRIFCSElement(relation);
			relatedJsonObject.put("relation", relationObject);
		} else {
			JSONArray relationArray = new JSONArray();
			for (Relation relation : relatedObject.getRelations()) {
				relationArray.add(getRelationObjectForRIFCSElement(relation));
			}
			relatedJsonObject.put("relation", relationArray);
		}
		return relatedJsonObject;
	}

	/**
	 * Get the relation element details
	 * 
	 * @param relation
	 * @return {@link JsonObject}
	 */
	private JsonObject getRelationObjectForRIFCSElement(Relation relation) {
		JsonObject relationObject = new JsonObject();
		relationObject.put("type", relation.getType());
		if (relation.getDescription() != null
				&& !relation.getDescription().isEmpty()) {
			relationObject.put("description", relation.getDescription());
		}
		if (relation.getURL() != null && !relation.getURL().isEmpty()) {
			relationObject.put("url", relation.getURL());
		}
		return relationObject;
	}

	// ========= Subjects =============

	/**
	 * Obtaining subjects element of the rif-cs xml
	 * 
	 * @param subjects
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getSubjectsForRIFCSElement(List<Subject> subjects,
			JsonObject data) {
		if (subjects.size() == 1) {
			Subject subject = subjects.get(0);
			JsonObject subjectObject = getSubjectForRIFCSElement(subject);
			data.put("subject", subjectObject);
		} else {
			JSONArray subjectArray = new JSONArray();
			for (Subject subject : subjects) {
				subjectArray.add(getSubjectForRIFCSElement(subject));
			}
			data.put("subject", subjectArray);
		}

	}

	/**
	 * Get the subject details
	 * 
	 * @param subject
	 * @return {@link JsonObject}
	 */
	private JsonObject getSubjectForRIFCSElement(Subject subject) {
		JsonObject subjectObject = new JsonObject();
		subjectObject.put("type", subject.getType());
		if (!subject.getTermIdentifier().isEmpty()) {
			subjectObject.put("termIdentifier", subject.getTermIdentifier());
		}
		subjectObject.put("value", subject.getValue());
		return subjectObject;
	}

	// ========= Descriptions =============

	/**
	 * Obtaining description element of the rif-cs xml
	 * 
	 * @param descriptions
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getDescriptionsForRIFCSElement(List<Description> descriptions,
			JsonObject data) {
		if (descriptions.size() == 1) {
			Description description = descriptions.get(0);
			JsonObject descriptionObject = getDescriptionForRIFCSElement(description);
			data.put("description", descriptionObject);
		} else {
			JSONArray descriptionArray = new JSONArray();
			for (Description description : descriptions) {
				descriptionArray
						.add(getDescriptionForRIFCSElement(description));
			}
			data.put("description", descriptionArray);
		}
	}

	/**
	 * Get the description details from description element
	 * 
	 * @param description
	 * @return {@link JsonObject}
	 */
	private JsonObject getDescriptionForRIFCSElement(Description description) {
		JsonObject descriptionObject = new JsonObject();
		descriptionObject.put("type", description.getType());
		descriptionObject.put("value", description.getValue());
		return descriptionObject;
	}

	// ========= Coverages =============

	/**
	 * Obtaining coverages element of the rif-cs xml
	 * 
	 * @param coverages
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getCoveragesForRIFCSElement(List<Coverage> coverages,
			JsonObject data) {
		if (coverages.size() == 1) {
			Coverage coverage = coverages.get(0);
			JsonObject coverageObject = getCoverageForRIFCSElement(coverage);
			data.put("coverage", coverageObject);
		} else {
			JSONArray coverageArray = new JSONArray();
			for (Coverage coverage : coverages) {
				coverageArray.add(getCoverageForRIFCSElement(coverage));
			}
			data.put("coverage", coverageArray);
		}
	}

	/**
	 * Get the temporal and spatial for the coverage element
	 * 
	 * @param coverage
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private JsonObject getCoverageForRIFCSElement(Coverage coverage) {
		JsonObject coverageObject = new JsonObject();
		if (coverage.getTemporals() != null
				&& !coverage.getTemporals().isEmpty()) {
			if (coverage.getTemporals().size() == 1) {
				Temporal temporal = coverage.getTemporals().get(0);
				JsonObject temporalObject = getTemporalForRIFCSElement(temporal);
				coverageObject.put("temporal", temporalObject);
			} else {
				JSONArray temporalArray = new JSONArray();
				for (Temporal temporal : coverage.getTemporals()) {
					temporalArray.add(getTemporalForRIFCSElement(temporal));
				}
				coverageObject.put("temporal", temporalArray);
			}
		}
		if (coverage.getSpatials() != null && !coverage.getSpatials().isEmpty()) {
			if (coverage.getSpatials().size() == 1) {
				Spatial spatial = coverage.getSpatials().get(0);
				JsonObject spatialObject = getSpatialForRIFCSElement(spatial);
				coverageObject.put("spatial", spatialObject);
			} else {
				JSONArray spatialArray = new JSONArray();
				for (Spatial spatial : coverage.getSpatials()) {
					spatialArray.add(getSpatialForRIFCSElement(spatial));
				}
				coverageObject.put("spatial", spatialArray);
			}
		}
		return coverageObject;
	}

	/**
	 * Get the date and text of the temporal element
	 * 
	 * @param temporal
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private JsonObject getTemporalForRIFCSElement(Temporal temporal) {
		JsonObject dateObject = new JsonObject();
		if (temporal.getDates() != null && !temporal.getDates().isEmpty()) {
			if (temporal.getDates().size() == 1) {
				TemporalCoverageDate temporalCoverageDate = temporal.getDates()
						.get(0);
				JsonObject temporalObject = getTemporalCoverageDateForRIFCSElement(temporalCoverageDate);
				dateObject.put("date", temporalObject);
			} else {
				JSONArray temporalArray = new JSONArray();
				for (TemporalCoverageDate temporalCoverageDate : temporal
						.getDates()) {
					temporalArray
							.add(getTemporalCoverageDateForRIFCSElement(temporalCoverageDate));
				}
				dateObject.put("date", temporalArray);
			}
		}
		if (temporal.getText() != null && !temporal.getText().isEmpty()) {
			if (temporal.getText().size() == 1) {
				dateObject.put("text", temporal.getText().get(0));
			} else {
				JSONArray textArray = new JSONArray();
				textArray.addAll(temporal.getText());
				dateObject.put("text", textArray);
			}
		}
		return dateObject;
	}

	/**
	 * Get the date details for the temporal date element
	 * 
	 * @param temporalCoverageDate
	 * @return {@link JsonObject}
	 */
	private JsonObject getTemporalCoverageDateForRIFCSElement(
			TemporalCoverageDate temporalCoverageDate) {
		JsonObject temporalCoverageDateObject = new JsonObject();
		temporalCoverageDateObject.put("type", temporalCoverageDate.getType());
		if (!temporalCoverageDate.DateFormat().isEmpty()) {
			temporalCoverageDateObject.put("dateFormat",
					temporalCoverageDate.DateFormat());
		}
		temporalCoverageDateObject
				.put("value", temporalCoverageDate.getValue());
		return temporalCoverageDateObject;
	}

	// ========= Related Info =============

	/**
	 * Obtaining related info element of the rif-cs xml
	 * 
	 * @param relatedInfos
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getRelatedInfosForRIFCSElement(List<RelatedInfo> relatedInfos,
			JsonObject data) {
		if (relatedInfos.size() == 1) {
			RelatedInfo relatedInfo = relatedInfos.get(0);
			JsonObject relatedInfoObject = getRelatedInfoForRIFCSElement(relatedInfo);
			data.put("relatedInfo", relatedInfoObject);
		} else {
			JSONArray relatedInfoArray = new JSONArray();
			for (RelatedInfo relatedInfo : relatedInfos) {
				relatedInfoArray
						.add(getRelatedInfoForRIFCSElement(relatedInfo));
			}
			data.put("relatedInfo", relatedInfoArray);
		}
	}

	/**
	 * Get the related info details for related info element
	 * 
	 * @param relatedInfo
	 * @return {@link JsonObject}
	 */
	private JsonObject getRelatedInfoForRIFCSElement(RelatedInfo relatedInfo) {
		JsonObject relatedInfoDetailsObject = new JsonObject();
		relatedInfoDetailsObject.put("identifier", relatedInfo.getIdentifier());
		if (!relatedInfo.getTitle().isEmpty()) {
			relatedInfoDetailsObject.put("title", relatedInfo.getTitle());
		}
		if (!relatedInfo.getNotes().isEmpty()) {
			relatedInfoDetailsObject.put("notes", relatedInfo.getNotes());
		}
		return relatedInfoDetailsObject;
	}

	// ========= Related Info =============

	/**
	 * Obtaining right element of the rif-cs xml
	 * 
	 * @param rights
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getRightsForRIFCSElement(List<Right> rights, JsonObject data) {
		if (rights.size() == 1) {
			Right right = rights.get(0);
			JsonObject rightObject = getRightForRIFCSElement(right);
			data.put("right", rightObject);
		} else {
			JSONArray rightArray = new JSONArray();
			for (Right right : rights) {
				rightArray.add(getRightForRIFCSElement(right));
			}
			data.put("right", rightArray);
		}
	}

	/**
	 * Get the right details for the rights element
	 * 
	 * @param right
	 * @return {@link JsonObject}
	 */
	private JsonObject getRightForRIFCSElement(Right right) {
		JsonObject rightDetailsObject = new JsonObject();
		if (right.getRightsStatement() != null) {
			JsonObject rightsStatementObject = new JsonObject();
			RightsInfo rightsStatement = right.getRightsStatement();
			if (!rightsStatement.getRightsUri().isEmpty()) {
				rightsStatementObject.put("rightsUri",
						rightsStatement.getRightsUri());
			}
			rightsStatementObject.put("value", rightsStatement.getValue());
			rightDetailsObject.put("rightsStatement", rightsStatementObject);
		}
		if (right.getLicence() != null) {
			JsonObject licenseObject = new JsonObject();
			RightsTypedInfo rightsTypedInfo = right.getLicence();
			if (!rightsTypedInfo.getType().isEmpty()) {
				licenseObject.put("type", rightsTypedInfo.getType());
			}
			if (!rightsTypedInfo.getRightsUri().isEmpty()) {
				licenseObject.put("rightsUri", rightsTypedInfo.getRightsUri());
			}
			licenseObject.put("value", rightsTypedInfo.getValue());
			rightDetailsObject.put("license", licenseObject);
		}
		if (right.getAccessRights() != null) {
			JsonObject accessRightsObject = new JsonObject();
			RightsTypedInfo rightsTypedInfo = right.getAccessRights();
			if (!rightsTypedInfo.getType().isEmpty()) {
				accessRightsObject.put("type", rightsTypedInfo.getType());
			}
			if (!rightsTypedInfo.getRightsUri().isEmpty()) {
				accessRightsObject.put("rightsUri",
						rightsTypedInfo.getRightsUri());
			}
			accessRightsObject.put("value", rightsTypedInfo.getValue());
			rightDetailsObject.put("accessRights", accessRightsObject);
		}
		return rightDetailsObject;
	}

	// ========= Existence Date =============

	/**
	 * Obtaining existence dates element of the rif-cs xml for non-collection
	 * elements
	 * 
	 * @param existenceDates
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getExistenceDatesForRIFCSElement(
			List<ExistenceDate> existenceDates, JsonObject data) {
		if (existenceDates.size() == 1) {
			ExistenceDate existenceDate = existenceDates.get(0);
			JsonObject existenceDatesObject = getExistenceDateForRIFCSElement(existenceDate);
			data.put("existenceDates", existenceDatesObject);
		} else {
			JSONArray existenceDatesArray = new JSONArray();
			for (ExistenceDate existenceDate : existenceDates) {
				existenceDatesArray
						.add(getExistenceDateForRIFCSElement(existenceDate));
			}
			data.put("existenceDates", existenceDatesArray);
		}
	}

	/**
	 * Get existence date details for existence date element
	 * 
	 * @param existenceDate
	 * @return {@link JsonObject}
	 */
	private JsonObject getExistenceDateForRIFCSElement(
			ExistenceDate existenceDate) {
		JsonObject existenceDateObject = new JsonObject();
		if (existenceDate.getStartDate() != null) {
			JsonObject startDateObject = new JsonObject();
			CommonDateElement startDate = existenceDate.getStartDate();
			startDateObject.put("dateFormat", startDate.getDateFormat());
			startDateObject.put("value", startDate.getValue());
			existenceDateObject.put("startDate", startDateObject);
		}
		if (existenceDate.getEndDate() != null) {
			JsonObject endDateObject = new JsonObject();
			CommonDateElement endDate = existenceDate.getStartDate();
			endDateObject.put("dateFormat", endDate.getDateFormat());
			endDateObject.put("value", endDate.getValue());
			existenceDateObject.put("startDate", endDateObject);
		}
		return existenceDateObject;
	}

	// ========= Related Info =============

	/**
	 * Obtaining citation info element of the rif-cs xml
	 * 
	 * @param citationInfos
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getCitationInfosForRIFCSElement(
			List<CitationInfo> citationInfos, JsonObject data) {
		if (citationInfos.size() == 1) {
			CitationInfo citationInfo = citationInfos.get(0);
			JsonObject citationInfosObject = getCitationInfoForRIFCSElement(citationInfo);
			data.put("citationInfo", citationInfosObject);
		} else {
			JSONArray citationInfosArray = new JSONArray();
			for (CitationInfo citationInfo : citationInfos) {
				citationInfosArray
						.add(getCitationInfoForRIFCSElement(citationInfo));
			}
			data.put("citationInfo", citationInfosArray);
		}
	}

	/**
	 * Get full citation info for citation info element
	 * 
	 * @param citationInfo
	 * @return {@link JsonObject}
	 */
	private JsonObject getCitationInfoForRIFCSElement(CitationInfo citationInfo) {
		JsonObject citationInfoObject = new JsonObject();
		if (citationInfo.getCitation() != null
				&& !citationInfo.getCitation().isEmpty()) {
			JsonObject fullCitationObject = new JsonObject();
			if (!citationInfo.getCitationStyle().isEmpty()) {
				fullCitationObject
						.put("style", citationInfo.getCitationStyle());
			}
			fullCitationObject.put("value", citationInfo.getCitation());
			citationInfoObject.put("fullCitation", fullCitationObject);
		}
		if (citationInfo.getCitationMetadata() != null) {
			JsonObject citationMetadataObject = getCitationMetadataForRIFCSElement(citationInfo
					.getCitationMetadata());
			citationMetadataObject.put("citationMetadata",
					citationMetadataObject);
		}
		return citationInfoObject;
	}

	/**
	 * Get the citation metadata for citation info elements
	 * 
	 * @param citationMetadata
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private JsonObject getCitationMetadataForRIFCSElement(
			CitationMetadata citationMetadata) {
		JsonObject citationMetaDataObject = new JsonObject();

		Identifier identifier = citationMetadata.getIdentifier();
		JsonObject identifierObject = new JsonObject();
		identifierObject.put("value", identifier.getValue());
		identifierObject.put("type", identifier.getType());
		citationMetaDataObject.put("identifer", identifierObject);

		if (citationMetadata.getContributors().size() == 1) {
			Contributor contributor = citationMetadata.getContributors().get(0);
			JsonObject contributorObject = getContributorForRIFCSElement(contributor);
			citationMetaDataObject.put("contributor", contributorObject);
		} else {
			JSONArray contributorArray = new JSONArray();
			for (Contributor contributor : citationMetadata.getContributors()) {
				contributorArray
						.add(getContributorForRIFCSElement(contributor));
			}
			citationMetaDataObject.put("contributor", contributorArray);
		}

		citationMetaDataObject.put("title", citationMetadata.getTitle());
		citationMetaDataObject.put("edition", citationMetadata.getEdition());
		if (!citationMetadata.getPublisher().isEmpty()) {
			citationMetaDataObject.put("publisher",
					citationMetadata.getPublisher());
		}
		citationMetaDataObject.put("placePublished",
				citationMetadata.getPlacePublished());
		if (citationMetadata.getDates() != null
				&& !citationMetadata.getDates().isEmpty()) {
			if (citationMetadata.getDates().size() == 1) {
				CitationDate citationDate = citationMetadata.getDates().get(0);
				JsonObject citationDateObject = getCitationDateForRIFCSElement(citationDate);
				citationMetaDataObject.put("date", citationDateObject);
			} else {
				JSONArray citationDateArray = new JSONArray();
				for (CitationDate citationDate : citationMetadata.getDates()) {
					citationDateArray
							.add(getCitationDateForRIFCSElement(citationDate));
				}
				citationMetaDataObject.put("date", citationDateArray);
			}
		}
		citationMetaDataObject.put("url", citationMetadata.getURL());
		citationMetaDataObject.put("context", citationMetadata.getContext());

		return citationMetaDataObject;
	}

	/**
	 * Get citation date details for citation date element
	 * 
	 * @param citationDate
	 * @return {@link JsonObject}
	 */
	private JsonObject getCitationDateForRIFCSElement(CitationDate citationDate) {
		JsonObject citationDateObject = new JsonObject();
		citationDateObject.put("type", citationDate.getType());
		citationDateObject.put("value", citationDate.getValue());
		return citationDateObject;
	}

	/**
	 * Get contributor for citation metadata element
	 * 
	 * @param contributor
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private JsonObject getContributorForRIFCSElement(Contributor contributor) {
		JsonObject contributorObject = new JsonObject();
		if (contributor.getNameParts().size() == 1) {
			NamePart namePart = contributor.getNameParts().get(0);
			if (namePart.getType() != null && !namePart.getType().isEmpty()) {
				JsonObject namePartObject = new JsonObject();
				namePartObject.put("type", namePart.getType());
				namePartObject.put("value", namePart.getValue());
				contributorObject.put("namePart", namePartObject);
			} else {
				contributorObject.put("namePart", namePart.getValue());
			}
		} else {
			JSONArray namePartArray = new JSONArray();
			for (NamePart namePart : contributor.getNameParts()) {
				JsonObject namePartObject = new JsonObject();
				namePartObject.put("type", namePart.getType());
				namePartObject.put("value", namePart.getValue());
				namePartArray.add(namePartObject);
			}
			contributorObject.put("namePart", namePartArray);
		}
		return contributorObject;
	}

	// ========= Access Policy =============

	/**
	 * Obtaining access policy element of the rif-cs xml
	 * 
	 * @param accessPolicies
	 * @param data
	 */
	@SuppressWarnings("unchecked")
	private void getAccessPoliciesForRIFCSElement(
			List<AccessPolicy> accessPolicies, JsonObject data) {
		if (accessPolicies.size() == 1) {
			AccessPolicy accessPolicy = accessPolicies.get(0);
			data.put("accessPolicy", accessPolicy.getValue());
		} else {
			JSONArray accessPolicyArray = new JSONArray();
			for (AccessPolicy accessPolicy : accessPolicies) {
				accessPolicyArray.add(accessPolicy.getValue());
			}
			data.put("accessPolicy", accessPolicyArray);
		}
	}

	@Override
	public boolean hasMoreObjects() {
		return hasMore;
	}

	/**
	 * Store the processed data and metadata in the system
	 * 
	 * @param dataJson
	 * @param metaJson
	 * @throws HarvesterException
	 */
	private void storeJsonInObject(JsonObject dataJson, JsonObject metaJson,
			String oid) throws HarvesterException {
		// Does the object already exist?
		DigitalObject object = null;
		try {
			object = getStorage().getObject(oid);
			storeJsonInPayload(dataJson, metaJson, object);

		} catch (StorageException ex) {
			// This is going to be brand new
			try {
				object = StorageUtils.getDigitalObject(getStorage(), oid);
				storeJsonInPayload(dataJson, metaJson, object);
			} catch (StorageException ex2) {
				throw new HarvesterException(
						"Error creating new digital object: ", ex2);
			}
		}

		// Set the pending flag
		if (object != null) {
			try {
				object.getMetadata().setProperty("render-pending", "true");
				object.close();
			} catch (Exception ex) {
				log.error("Error setting 'render-pending' flag: ", ex);
			}
		}
	}

	/**
	 * Store the processed data and metadata in a payload
	 * 
	 * @param dataJson
	 * @param metaJson
	 * @param object
	 * @throws HarvesterException
	 */
	private void storeJsonInPayload(JsonObject dataJson, JsonObject metaJson,
			DigitalObject object) throws HarvesterException {

		Payload payload = null;
		JsonSimple json = new JsonSimple();
		try {
			// New payloads
			payload = object.getPayload(payloadId);
			// log.debug("Updating existing payload: '{}' => '{}'",
			// object.getId(), payloadId);

			// Get the old JSON to merge
			try {
				json = new JsonSimple(payload.open());
			} catch (IOException ex) {
				log.error("Error parsing existing JSON: '{}' => '{}'",
						object.getId(), payloadId);
				throw new HarvesterException("Error parsing existing JSON: ",
						ex);
			} finally {
				payload.close();
			}

			// Update storage
			try {
				InputStream in = streamMergedJson(dataJson, metaJson, json);
				object.updatePayload(payloadId, in);

			} catch (IOException ex2) {
				throw new HarvesterException("Error processing JSON data: ",
						ex2);
			} catch (StorageException ex2) {
				throw new HarvesterException("Error updating payload: ", ex2);
			}

		} catch (StorageException ex) {
			// Create a new Payload
			try {
				// log.debug("Creating new payload: '{}' => '{}'",
				// object.getId(), payloadId);
				InputStream in = streamMergedJson(dataJson, metaJson, json);
				payload = object.createStoredPayload(payloadId, in);

			} catch (IOException ex2) {
				throw new HarvesterException("Error parsing JSON encoding: ",
						ex2);
			} catch (StorageException ex2) {
				throw new HarvesterException("Error creating new payload: ",
						ex2);
			}
		}

		// Tidy up before we finish
		if (payload != null) {
			try {
				payload.setContentType("application/json");
				payload.close();
			} catch (Exception ex) {
				log.error("Error setting Payload MIME type and closing: ", ex);
			}
		}
	}

	/**
	 * Merge the newly processed data with an (possible) existing data already
	 * present, also convert the completed JSON merge into a Stream for storage.
	 * 
	 * @param dataJson
	 * @param metaJson
	 * @param existing
	 * @throws IOException
	 */
	private InputStream streamMergedJson(JsonObject dataJson,
			JsonObject metaJson, JsonSimple existing) throws IOException {
		// Overwrite and/or create only nodes we consider new data
		JsonObject existingData = existing.writeObject("registryObject");
		existingData.putAll(dataJson);
		JsonObject existingMeta = existing.writeObject("metadata");
		existingMeta.putAll(metaJson);

		// Turn into a stream to return
		String jsonString = existing.toString(true);
		return IOUtils.toInputStream(jsonString, "UTF-8");
	}
}
