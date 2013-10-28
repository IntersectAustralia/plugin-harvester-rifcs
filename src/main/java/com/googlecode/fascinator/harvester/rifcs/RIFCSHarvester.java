package com.googlecode.fascinator.harvester.rifcs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;

import org.ands.rifcs.base.AccessPolicy;
import org.ands.rifcs.base.Activity;
import org.ands.rifcs.base.Address;
import org.ands.rifcs.base.AddressPart;
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
 * <li>payloadId: The payload identifier used to store the JSON this.data (defaults
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

	/**
	 * Ignored field names (column)
	 */
	private List<String> ignoredFields;

	/**
	 * Included field names (column)
	 */
	private List<String> includedFields;

	/**
	 * A prefix for generating the object's ID
	 */
	private String idPrefix;

	private JsonSimple filedsMapping = new JsonSimple();

	// the output json object.
	private JsonObject data;

	public RIFCSHarvester() {
		super("xml", "RIF-CS Harvester");
	}

	@Override
	public void init() throws HarvesterException {
		JsonSimple options = new JsonSimple(getJsonConfig().getObject(
				"harvester", "xml"));

		String filePath = options.getString(null, "fileLocation");
		if (filePath == null) {
			throw new HarvesterException("No this.data file provided!");
		}
		File rifcsDataFile = new File(filePath);
		if (rifcsDataFile == null || !rifcsDataFile.exists()) {
			throw new HarvesterException("Could not find rif-cs file '"
					+ filePath + "'");
		}
		filename = rifcsDataFile.getName();

		idPrefix = options.getString("", "recordIDPrefix");
		ignoredFields = getStringList(options, "ignoreFields");
		includedFields = getStringList(options, "includedFields");

		filedsMapping = new JsonSimple(options.getObject("filedsMapping"));

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

	/**
	 * Gets a string list from a JsonSimple object. Convenience method to return
	 * an empty list instead of null if the node was not found.
	 *
	 * @param json a JsonSimple object
	 * @param path path to the node
	 * @return string list found at node, or empty if not found
	 */
	private List<String> getStringList(JsonSimple json, Object... path) {
		List<String> list = json.getStringList(path);
		if (list == null) {
			list = Collections.emptyList();
		}
		return list;
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
		this.data = new JsonObject();
		try {
			this.data.put("group", registryObject.getGroup());
			this.data.put("key", registryObject.getKey());
			this.data.put("originatingSource",
					registryObject.getOriginatingSource());
			RIFCSElement element = registryObject.getClassObject();
			parseRIFCSElement(element);
		} catch (RIFCSException e) {
			throw new HarvesterException(e);
		}
		log.debug(this.data.toString());
		JsonObject meta = new JsonObject();
		meta.put("dc.identifier", idPrefix + recordId);

		String oid = DigestUtils.md5Hex(filename
				+ this.data.get("group") + recordId);
		storeJsonInObject(this.data, meta, oid);
		return oid;
	}

	/**
	 * Get the child element of registry object which consists of one of
	 * Activity, Collection, Party, or Service.
	 * 
	 * @param element
	 * @throws HarvesterException
	 */
	private void parseRIFCSElement(RIFCSElement element) throws HarvesterException {
		if (element instanceof Activity) {
			Activity activity = (Activity) element;

			parseElement(activity.getType(), activity.getIdentifiers(),
					activity.getNames(), activity.getLocations(),
					activity.getRelatedObjects(),
					activity.getSubjects(), activity.getDescriptions(),
					activity.getCoverage(), activity.getRelatedInfo(),
					activity.getRights(), activity.getExistenceDates(),
					null, null);

		} else if (element instanceof Collection) {
			Collection collection = (Collection) element;

			parseElement(collection.getType(),
					collection.getIdentifiers(), collection.getNames(),
					collection.getLocations(),
					collection.getRelatedObjects(),
					collection.getSubjects(),
					collection.getDescriptions(),
					collection.getCoverage(),
					collection.getRelatedInfo(),
					collection.getRightsList(), null,
					collection.getCitationInfos(), null);
		} else if (element instanceof Party) {
			Party party = (Party) element;

			parseElement(party.getType(), party.getIdentifiers(),
					party.getNames(), party.getLocations(),
					party.getRelatedObjects(), party.getSubjects(),
					party.getDescriptions(), party.getCoverage(),
					party.getRelatedInfo(), party.getRights(),
					party.getExistenceDates(), null, null);
		}
		else if (element instanceof Service) {
			Service service = (Service) element;

			parseElement(service.getType(), service.getIdentifiers(),
					service.getNames(), service.getLocations(),
					service.getRelatedObjects(), service.getSubjects(),
					service.getDescriptions(), service.getCoverage(),
					service.getRelatedInfo(), service.getRights(),
					service.getExistenceDates(), null,
					service.getAccessPolicies());
		}
		else {
			throw new HarvesterException(
					"Wrong element found, only supports activity, collection, party, or service");
		}
	}

	private void parseElement(String type, List<Identifier> identifiers,
			List<Name> names, List<Location> locations,
			List<RelatedObject> relatedObjects, List<Subject> subjects,
			List<Description> descriptions, List<Coverage> coverages,
			List<RelatedInfo> relatedInfos, List<Right> rights,
			List<ExistenceDate> existenceDates,
			List<CitationInfo> citationInfos, List<AccessPolicy> accessPolicies) {

		// get the type for rifcs element
		if (type != null && !type.isEmpty()) {
			this.data.put("type", type);
		}

		// get the identifiers for rifcs element
		if (identifiers != null && !identifiers.isEmpty()) {
			parseIdentifiersForRIFCSElement(identifiers);
		}

		// get names and nameparts for rifcs element
		if (names != null && !names.isEmpty()) {
			parseNamesForRIFCSElement(names);
		}

		// get locations for rifcs element
		if (locations != null && !locations.isEmpty()) {
			parseLocationsForRIFCSElement(locations);
		}

		// get the related objects for rifcs element
		if (relatedObjects != null && !relatedObjects.isEmpty()) {
			parseRelatedObjectsForRIFCSElement(relatedObjects);
		}

		// get the subjects for rifcs element
		if (subjects != null && !subjects.isEmpty()) {
			boolean isMultiple = "person".equals(type);
			parseSubjectsForRIFCSElement(subjects, isMultiple);
		}

		// get the descriptions for rifcs element
		if (descriptions != null && !descriptions.isEmpty()) {
			parseDescriptionsForRIFCSElement(descriptions);
		}

		// get the related info for rifcs element
		if (relatedInfos != null && !relatedInfos.isEmpty()) {
			parseRelatedInfosForRIFCSElement(relatedInfos);
		}

		// get the existance dates for rifcs element
		if (existenceDates != null && !existenceDates.isEmpty()) {
			parseExistenceDatesForRIFCSElement(existenceDates);
		}

//		// get the coverages for rifcs element
//		if (coverages != null && !coverages.isEmpty()) {
//			parseCoveragesForRIFCSElement(coverages);
//		}

//		// get the right for rifcs element
//		if (rights != null && !rights.isEmpty()) {
//			parseRightsForRIFCSElement(rights);
//		}

//		// get the citation info for rifcs element
//		if (citationInfos != null && !citationInfos.isEmpty()) {
//			parseCitationInfosForRIFCSElement(citationInfos);
//		}

//		// get the access policy for rifcs element
//		if (accessPolicies != null && !accessPolicies.isEmpty()) {
//			parseAccessPoliciesForRIFCSElement(accessPolicies);
//		}
	}

	// ========= Identifiers =============

	/**
	 * Obtaining identifiers (included in the mapping) element
	 * of the rif-cs xml，and set the value into output json object.
	 * 
	 * @param identifiers all the identifiers
	 */
	@SuppressWarnings("unchecked")
	private void parseIdentifiersForRIFCSElement(List<Identifier> identifiers) {

		for (Identifier identifier : identifiers) {
			String key = "identifier." + identifier.getType();
			String csvFieldName = filedsMapping.getString("",key);
			if(csvFieldName != "") {
				this.data.put(csvFieldName, identifier.getValue());
			}
		}
	}

	// ========= Name and NamePart =============
	/**
	 * Obtaining names element of the rif-cs xml
	 *
	 * @param names
	 */
	private void parseNamesForRIFCSElement(List<Name> names) {
		for (Name name : names) {
			String nameType = name.getType();

			List<NamePart> nameParts = name.getNameParts();
			for (NamePart namePart : nameParts) {
				StringBuffer key = new StringBuffer();
				key.append("name.");
				key.append(nameType);

				String subType = namePart.getType();
				if(subType != null && !"".equals(subType)) {
					key.append(".");
					key.append(subType);
				}

				String csvFieldName = filedsMapping.getString("",key.toString());
				if (csvFieldName != "") {
					this.data.put(csvFieldName, namePart.getValue());
				}

			}
		}
	}

	// ========= Locations =============

	/**
	 * Obtaining locations element of the rif-cs xml
	 *
	 * @param locations
	 */
	@SuppressWarnings("unchecked")
	private void parseLocationsForRIFCSElement(List<Location> locations) {
		for (Location location : locations) {

			if (location.getAddresses() != null
					&& !location.getAddresses().isEmpty()) {
				for (Address address : location.getAddresses()) {
					parseAddressForRIFCSElement(address);
				}
			}
		}
	}

	/**
	 * Get the address for location element
	 *
	 * @param address
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private void parseAddressForRIFCSElement(Address address) {
		if (address.getElectronics() != null
				&& !address.getElectronics().isEmpty()) {
			for (Electronic electronic : address.getElectronics()) {
				parseElectronicForRIFCSElement(electronic);
			}
		}
		if (address.getPhysicalAddresses() != null
				&& !address.getPhysicalAddresses().isEmpty()) {
			for (Physical physical : address.getPhysicalAddresses()) {
				parsePhysicalForRIFCSElement(physical);
			}
		}
	}

	/**
	 * Get the electronic address for address element
	 *
	 * @param electronic
	 */
	@SuppressWarnings("unchecked")
	private void parseElectronicForRIFCSElement(Electronic electronic) {
		if (!electronic.getType().isEmpty()) {
			String key = "location.address.electronic." + electronic.getType();
			String csvFieldName = filedsMapping.getString("",key);
			if(csvFieldName != "") {
				this.data.put(csvFieldName, electronic.getValue());
			}
		}
	}

	/**
	 * Get physical address for address element
	 *
	 * @param physical
	 * @return {@link JsonObject}
	 */
	@SuppressWarnings("unchecked")
	private void parsePhysicalForRIFCSElement(Physical physical) {
		for (AddressPart addressPart : physical.getAddressParts()) {
			String key = "location.address.physical." + addressPart.getType();
			String csvFieldName = filedsMapping.getString("",key);
			if (csvFieldName != "") {
				this.data.put(csvFieldName, addressPart.getValue());
			}
		}
	}

//	/**
//	 * Get spatial for location element
//	 *
//	 * @param spatial
//	 * @return {@link JsonObject}
//	 */
//	private JsonObject getSpatialForRIFCSElement(Spatial spatial) {
//		JsonObject spatialObject = new JsonObject();
//		spatialObject.put("type", spatial.getType());
//		spatialObject.put("value", spatial.getValue());
//		return spatialObject;
//	}

	// ========= Related Objects =============

	/**
	 * Obtaining related objects element of the rif-cs xml
	 *
	 * @param relatedObjects
	 */
	@SuppressWarnings("unchecked")
	private void parseRelatedObjectsForRIFCSElement(List<RelatedObject> relatedObjects) {
		for (RelatedObject relatedObject : relatedObjects) {
			String relatedObjectKey = relatedObject.getKey();
			List<Relation> relations = relatedObject.getRelations();
			for (Relation relation : relations) {
				String relationType = relation.getType();
				String key = "relatedObject." + relationType;
				String csvFieldName = filedsMapping.getString("",key);

				if (csvFieldName != "") {
//					// for isMemberOf relation.
//					if ("isMemberOf".equalsIgnoreCase(relationType)) {
//						csvFieldName = csvFieldName + "_" + relations.indexOf(relation) + 1;
//					}

					this.data.put(csvFieldName, relatedObjectKey);
				}
			}
		}
	}

	// ========= Subjects =============

	/**
	 * Obtaining subjects element of the rif-cs xml
	 *
	 * @param subjects
	 */
	@SuppressWarnings("unchecked")
	private void parseSubjectsForRIFCSElement(List<Subject> subjects, boolean isMutiple) {
		for (Subject subject : subjects) {
			String key = "subject." + subject.getType();
			String csvFieldName = filedsMapping.getString("",key);
			if (csvFieldName != "") {
				if(isMutiple) {
					this.data.put(csvFieldName + "_" + (subjects.indexOf(subject) + 1), subject.getValue());
				} else {
					this.data.put(csvFieldName, subject.getValue());
					break;
				}
			}
		}
	}

	// ========= Descriptions =============

	/**
	 * Obtaining description element of the rif-cs xml
	 *
	 * @param descriptions
	 */
	@SuppressWarnings("unchecked")
	private void parseDescriptionsForRIFCSElement(List<Description> descriptions) {
		for (Description description : descriptions) {
			String key = "description." + description.getType();
			String csvFieldName = filedsMapping.getString("",key);
			if(csvFieldName != "") {
				this.data.put(csvFieldName, description.getValue());
			}
		}
	}

	// ========= Coverages =============

//	/**
//	 * Obtaining coverages element of the rif-cs xml
//	 *
//	 * @param coverages
//	 */
//	@SuppressWarnings("unchecked")
//	private void parseCoveragesForRIFCSElement(List<Coverage> coverages) {
//		if (coverages.size() == 1) {
//			Coverage coverage = coverages.get(0);
//			JsonObject coverageObject = getCoverageForRIFCSElement(coverage);
//			this.data.put("coverage", coverageObject);
//		} else {
//			JSONArray coverageArray = new JSONArray();
//			for (Coverage coverage : coverages) {
//				coverageArray.add(getCoverageForRIFCSElement(coverage));
//			}
//			this.data.put("coverage", coverageArray);
//		}
//	}
//
//	/**
//	 * Get the temporal and spatial for the coverage element
//	 *
//	 * @param coverage
//	 * @return {@link JsonObject}
//	 */
//	@SuppressWarnings("unchecked")
//	private JsonObject getCoverageForRIFCSElement(Coverage coverage) {
//		JsonObject coverageObject = new JsonObject();
//		if (coverage.getTemporals() != null
//				&& !coverage.getTemporals().isEmpty()) {
//			if (coverage.getTemporals().size() == 1) {
//				Temporal temporal = coverage.getTemporals().get(0);
//				JsonObject temporalObject = getTemporalForRIFCSElement(temporal);
//				coverageObject.put("temporal", temporalObject);
//			} else {
//				JSONArray temporalArray = new JSONArray();
//				for (Temporal temporal : coverage.getTemporals()) {
//					temporalArray.add(getTemporalForRIFCSElement(temporal));
//				}
//				coverageObject.put("temporal", temporalArray);
//			}
//		}
//		if (coverage.getSpatials() != null && !coverage.getSpatials().isEmpty()) {
//			if (coverage.getSpatials().size() == 1) {
//				Spatial spatial = coverage.getSpatials().get(0);
//				JsonObject spatialObject = getSpatialForRIFCSElement(spatial);
//				coverageObject.put("spatial", spatialObject);
//			} else {
//				JSONArray spatialArray = new JSONArray();
//				for (Spatial spatial : coverage.getSpatials()) {
//					spatialArray.add(getSpatialForRIFCSElement(spatial));
//				}
//				coverageObject.put("spatial", spatialArray);
//			}
//		}
//		return coverageObject;
//	}
//
//	/**
//	 * Get the date and text of the temporal element
//	 *
//	 * @param temporal
//	 * @return {@link JsonObject}
//	 */
//	@SuppressWarnings("unchecked")
//	private JsonObject getTemporalForRIFCSElement(Temporal temporal) {
//		JsonObject dateObject = new JsonObject();
//		if (temporal.getDates() != null && !temporal.getDates().isEmpty()) {
//			if (temporal.getDates().size() == 1) {
//				TemporalCoverageDate temporalCoverageDate = temporal.getDates()
//						.get(0);
//				JsonObject temporalObject = getTemporalCoverageDateForRIFCSElement(temporalCoverageDate);
//				dateObject.put("date", temporalObject);
//			} else {
//				JSONArray temporalArray = new JSONArray();
//				for (TemporalCoverageDate temporalCoverageDate : temporal
//						.getDates()) {
//					temporalArray
//							.add(getTemporalCoverageDateForRIFCSElement(temporalCoverageDate));
//				}
//				dateObject.put("date", temporalArray);
//			}
//		}
//		if (temporal.getText() != null && !temporal.getText().isEmpty()) {
//			if (temporal.getText().size() == 1) {
//				dateObject.put("text", temporal.getText().get(0));
//			} else {
//				JSONArray textArray = new JSONArray();
//				textArray.addAll(temporal.getText());
//				dateObject.put("text", textArray);
//			}
//		}
//		return dateObject;
//	}
//
//	/**
//	 * Get the date details for the temporal date element
//	 *
//	 * @param temporalCoverageDate
//	 * @return {@link JsonObject}
//	 */
//	private JsonObject getTemporalCoverageDateForRIFCSElement(
//			TemporalCoverageDate temporalCoverageDate) {
//		JsonObject temporalCoverageDateObject = new JsonObject();
//		temporalCoverageDateObject.put("type", temporalCoverageDate.getType());
//		if (!temporalCoverageDate.DateFormat().isEmpty()) {
//			temporalCoverageDateObject.put("dateFormat",
//					temporalCoverageDate.DateFormat());
//		}
//		temporalCoverageDateObject
//				.put("value", temporalCoverageDate.getValue());
//		return temporalCoverageDateObject;
//	}

	// ========= Related Info =============

	/**
	 * Obtaining related info element of the rif-cs xml
	 *
	 * @param relatedInfos
	 */
	@SuppressWarnings("unchecked")
	private void parseRelatedInfosForRIFCSElement(List<RelatedInfo> relatedInfos) {
		for (RelatedInfo relatedInfo : relatedInfos) {
			String title = relatedInfo.getTitle();
			String key = "relatedInfo." + relatedInfo.getType() + "." + title;
			String csvFieldName = filedsMapping.getString("",key);
			if(csvFieldName != "") {
				this.data.put(csvFieldName, relatedInfo.getIdentifier());
			}
		}
	}

	// ========= Rights =============
//
//	/**
//	 * Obtaining right element of the rif-cs xml
//	 *
//	 * @param rights
//	 */
//	@SuppressWarnings("unchecked")
//	private void parseRightsForRIFCSElement(List<Right> rights) {
//		if (rights.size() == 1) {
//			Right right = rights.get(0);
//			JsonObject rightObject = getRightForRIFCSElement(right);
//			this.data.put("right", rightObject);
//		} else {
//			JSONArray rightArray = new JSONArray();
//			for (Right right : rights) {
//				rightArray.add(getRightForRIFCSElement(right));
//			}
//			this.data.put("right", rightArray);
//		}
//	}
//
//	/**
//	 * Get the right details for the rights element
//	 *
//	 * @param right
//	 * @return {@link JsonObject}
//	 */
//	private JsonObject getRightForRIFCSElement(Right right) {
//		JsonObject rightDetailsObject = new JsonObject();
//		if (right.getRightsStatement() != null) {
//			JsonObject rightsStatementObject = new JsonObject();
//			RightsInfo rightsStatement = right.getRightsStatement();
//			if (!rightsStatement.getRightsUri().isEmpty()) {
//				rightsStatementObject.put("rightsUri",
//						rightsStatement.getRightsUri());
//			}
//			rightsStatementObject.put("value", rightsStatement.getValue());
//			rightDetailsObject.put("rightsStatement", rightsStatementObject);
//		}
//		if (right.getLicence() != null) {
//			JsonObject licenseObject = new JsonObject();
//			RightsTypedInfo rightsTypedInfo = right.getLicence();
//			if (!rightsTypedInfo.getType().isEmpty()) {
//				licenseObject.put("type", rightsTypedInfo.getType());
//			}
//			if (!rightsTypedInfo.getRightsUri().isEmpty()) {
//				licenseObject.put("rightsUri", rightsTypedInfo.getRightsUri());
//			}
//			licenseObject.put("value", rightsTypedInfo.getValue());
//			rightDetailsObject.put("license", licenseObject);
//		}
//		if (right.getAccessRights() != null) {
//			JsonObject accessRightsObject = new JsonObject();
//			RightsTypedInfo rightsTypedInfo = right.getAccessRights();
//			if (!rightsTypedInfo.getType().isEmpty()) {
//				accessRightsObject.put("type", rightsTypedInfo.getType());
//			}
//			if (!rightsTypedInfo.getRightsUri().isEmpty()) {
//				accessRightsObject.put("rightsUri",
//						rightsTypedInfo.getRightsUri());
//			}
//			accessRightsObject.put("value", rightsTypedInfo.getValue());
//			rightDetailsObject.put("accessRights", accessRightsObject);
//		}
//		return rightDetailsObject;
//	}

	// ========= Existence Date =============

	/**
	 * Obtaining existence dates element of the rif-cs xml for non-collection
	 * elements
	 *
	 * @param existenceDates
	 */
	@SuppressWarnings("unchecked")
	private void parseExistenceDatesForRIFCSElement(List<ExistenceDate> existenceDates) {
		W3CDateFormat dateFormat = new W3CDateFormat(W3CDateFormat.Pattern.SECOND);
		String startYear = "";
		String endYear = "";

		for (ExistenceDate existenceDate : existenceDates) {
			String startDateValue = existenceDate.getStartDate().getValue();
			if(startYear.compareTo(startDateValue) >= 0) {
				startYear = startDateValue;
			}

			String endDateValue = existenceDate.getEndDate().getValue();
			if(endYear.compareTo(endDateValue) <= 0) {
				endYear = endDateValue;
			}
		}

		try {
			startYear = dateFormat.parse(startYear).getYear() + "";
			endYear = dateFormat.parse(endYear).getYear() + "";

			String key1 = "existenceDates.startDate";
			String key2 = "existenceDates.endDate";

			String csvfiledName1 = filedsMapping.getString("",key1);
			if(csvfiledName1 != null) {
				this.data.put(csvfiledName1, startYear);
			}

			String csvfiledName2 = filedsMapping.getString("",key2);
			if (csvfiledName2 != null) {
				this.data.put(csvfiledName2, endYear);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	// ========= Related Info =============

//	/**
//	 * Obtaining citation info element of the rif-cs xml
//	 *
//	 * @param citationInfos
//	 */
//	@SuppressWarnings("unchecked")
//	private void parseCitationInfosForRIFCSElement(List<CitationInfo> citationInfos) {
//		if (citationInfos.size() == 1) {
//			CitationInfo citationInfo = citationInfos.get(0);
//			JsonObject citationInfosObject = getCitationInfoForRIFCSElement(citationInfo);
//			this.data.put("citationInfo", citationInfosObject);
//		} else {
//			JSONArray citationInfosArray = new JSONArray();
//			for (CitationInfo citationInfo : citationInfos) {
//				citationInfosArray
//						.add(getCitationInfoForRIFCSElement(citationInfo));
//			}
//			this.data.put("citationInfo", citationInfosArray);
//		}
//	}
//
//	/**
//	 * Get full citation info for citation info element
//	 *
//	 * @param citationInfo
//	 * @return {@link JsonObject}
//	 */
//	private JsonObject getCitationInfoForRIFCSElement(CitationInfo citationInfo) {
//		JsonObject citationInfoObject = new JsonObject();
//		if (citationInfo.getCitation() != null
//				&& !citationInfo.getCitation().isEmpty()) {
//			JsonObject fullCitationObject = new JsonObject();
//			if (!citationInfo.getCitationStyle().isEmpty()) {
//				fullCitationObject
//						.put("style", citationInfo.getCitationStyle());
//			}
//			fullCitationObject.put("value", citationInfo.getCitation());
//			citationInfoObject.put("fullCitation", fullCitationObject);
//		}
//		if (citationInfo.getCitationMetadata() != null) {
//			JsonObject citationMetadataObject = getCitationMetadataForRIFCSElement(citationInfo
//					.getCitationMetadata());
//			citationMetadataObject.put("citationMetadata",
//					citationMetadataObject);
//		}
//		return citationInfoObject;
//	}
//
//	/**
//	 * Get the citation metadata for citation info elements
//	 *
//	 * @param citationMetadata
//	 * @return {@link JsonObject}
//	 */
//	@SuppressWarnings("unchecked")
//	private JsonObject getCitationMetadataForRIFCSElement(
//			CitationMetadata citationMetadata) {
//		JsonObject citationMetaDataObject = new JsonObject();
//
//		Identifier identifier = citationMetadata.getIdentifier();
//		JsonObject identifierObject = new JsonObject();
//		identifierObject.put("value", identifier.getValue());
//		identifierObject.put("type", identifier.getType());
//		citationMetaDataObject.put("identifer", identifierObject);
//
//		if (citationMetadata.getContributors().size() == 1) {
//			Contributor contributor = citationMetadata.getContributors().get(0);
//			JsonObject contributorObject = getContributorForRIFCSElement(contributor);
//			citationMetaDataObject.put("contributor", contributorObject);
//		} else {
//			JSONArray contributorArray = new JSONArray();
//			for (Contributor contributor : citationMetadata.getContributors()) {
//				contributorArray
//						.add(getContributorForRIFCSElement(contributor));
//			}
//			citationMetaDataObject.put("contributor", contributorArray);
//		}
//
//		citationMetaDataObject.put("title", citationMetadata.getTitle());
//		citationMetaDataObject.put("edition", citationMetadata.getEdition());
//		if (!citationMetadata.getPublisher().isEmpty()) {
//			citationMetaDataObject.put("publisher",
//					citationMetadata.getPublisher());
//		}
//		citationMetaDataObject.put("placePublished",
//				citationMetadata.getPlacePublished());
//		if (citationMetadata.getDates() != null
//				&& !citationMetadata.getDates().isEmpty()) {
//			if (citationMetadata.getDates().size() == 1) {
//				CitationDate citationDate = citationMetadata.getDates().get(0);
//				JsonObject citationDateObject = getCitationDateForRIFCSElement(citationDate);
//				citationMetaDataObject.put("date", citationDateObject);
//			} else {
//				JSONArray citationDateArray = new JSONArray();
//				for (CitationDate citationDate : citationMetadata.getDates()) {
//					citationDateArray
//							.add(getCitationDateForRIFCSElement(citationDate));
//				}
//				citationMetaDataObject.put("date", citationDateArray);
//			}
//		}
//		citationMetaDataObject.put("url", citationMetadata.getURL());
//		citationMetaDataObject.put("context", citationMetadata.getContext());
//
//		return citationMetaDataObject;
//	}
//
//	/**
//	 * Get citation date details for citation date element
//	 *
//	 * @param citationDate
//	 * @return {@link JsonObject}
//	 */
//	private JsonObject getCitationDateForRIFCSElement(CitationDate citationDate) {
//		JsonObject citationDateObject = new JsonObject();
//		citationDateObject.put("type", citationDate.getType());
//		citationDateObject.put("value", citationDate.getValue());
//		return citationDateObject;
//	}
//
//	/**
//	 * Get contributor for citation metadata element
//	 *
//	 * @param contributor
//	 * @return {@link JsonObject}
//	 */
//	@SuppressWarnings("unchecked")
//	private JsonObject getContributorForRIFCSElement(Contributor contributor) {
//		JsonObject contributorObject = new JsonObject();
//		if (contributor.getNameParts().size() == 1) {
//			NamePart namePart = contributor.getNameParts().get(0);
//			if (namePart.getType() != null && !namePart.getType().isEmpty()) {
//				JsonObject namePartObject = new JsonObject();
//				namePartObject.put("type", namePart.getType());
//				namePartObject.put("value", namePart.getValue());
//				contributorObject.put("namePart", namePartObject);
//			} else {
//				contributorObject.put("namePart", namePart.getValue());
//			}
//		} else {
//			JSONArray namePartArray = new JSONArray();
//			for (NamePart namePart : contributor.getNameParts()) {
//				JsonObject namePartObject = new JsonObject();
//				namePartObject.put("type", namePart.getType());
//				namePartObject.put("value", namePart.getValue());
//				namePartArray.add(namePartObject);
//			}
//			contributorObject.put("namePart", namePartArray);
//		}
//		return contributorObject;
//	}
//
//	// ========= Access Policy =============
//
//	/**
//	 * Obtaining access policy element of the rif-cs xml
//	 *
//	 * @param accessPolicies
//	 */
//	@SuppressWarnings("unchecked")
//	private void parseAccessPoliciesForRIFCSElement(List<AccessPolicy> accessPolicies) {
//		if (accessPolicies.size() == 1) {
//			AccessPolicy accessPolicy = accessPolicies.get(0);
//			this.data.put("accessPolicy", accessPolicy.getValue());
//		} else {
//			JSONArray accessPolicyArray = new JSONArray();
//			for (AccessPolicy accessPolicy : accessPolicies) {
//				accessPolicyArray.add(accessPolicy.getValue());
//			}
//			this.data.put("accessPolicy", accessPolicyArray);
//		}
//	}

	@Override
	public boolean hasMoreObjects() {
		return hasMore;
	}

	/**
	 * Store the processed data and metadata in the system
	 *
	 * @param dataJson an instantiated JSON object containing data to store
	 * @param metaJson an instantiated JSON object containing metadata to store
	 * @throws HarvesterException if an error occurs
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
	 * @param dataJson an instantiated JSON object containing data to store
	 * @param metaJson an instantiated JSON object containing metadata to store
	 * @param object   the object to put our payload in
	 * @throws HarvesterException if an error occurs
	 */
	private void storeJsonInPayload(JsonObject dataJson, JsonObject metaJson,
									DigitalObject object) throws HarvesterException {

		Payload payload = null;
		JsonSimple json = new JsonSimple();
		try {
			// New payloads
			payload = object.getPayload(payloadId);
			//log.debug("Updating existing payload: '{}' => '{}'",
			//        object.getId(), payloadId);

			// Get the old JSON to merge
			try {
				json = new JsonSimple(payload.open());
			} catch (IOException ex) {
				log.error("Error parsing existing JSON: '{}' => '{}'",
						object.getId(), payloadId);
				throw new HarvesterException(
						"Error parsing existing JSON: ", ex);
			} finally {
				payload.close();
			}

			// Update storage
			try {
				InputStream in = streamMergedJson(dataJson, metaJson, json);
				object.updatePayload(payloadId, in);

			} catch (IOException ex2) {
				throw new HarvesterException(
						"Error processing JSON data: ", ex2);
			} catch (StorageException ex2) {
				throw new HarvesterException(
						"Error updating payload: ", ex2);
			}

		} catch (StorageException ex) {
			// Create a new Payload
			try {
				//log.debug("Creating new payload: '{}' => '{}'",
				//        object.getId(), payloadId);
				InputStream in = streamMergedJson(dataJson, metaJson, json);
				payload = object.createStoredPayload(payloadId, in);

			} catch (IOException ex2) {
				throw new HarvesterException(
						"Error parsing JSON encoding: ", ex2);
			} catch (StorageException ex2) {
				throw new HarvesterException(
						"Error creating new payload: ", ex2);
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
	 * @param dataJson an instantiated JSON object containing data to store
	 * @param metaJson an instantiated JSON object containing metadata to store
	 * @param existing an instantiated JsonSimple object with any existing data
	 * @throws IOException if any character encoding issues effect the Stream
	 */
	private InputStream streamMergedJson(JsonObject dataJson,
										 JsonObject metaJson, JsonSimple existing) throws IOException {
		// Overwrite and/or create only nodes we consider new data
		existing.getJsonObject().put("recordIDPrefix", idPrefix);
		JsonObject existingData = existing.writeObject("data");
		existingData.putAll(dataJson);
		JsonObject existingMeta = existing.writeObject("metadata");
		existingMeta.putAll(metaJson);

		// Turn into a stream to return
		String jsonString = existing.toString(true);
		return IOUtils.toInputStream(jsonString, "UTF-8");
	}
}
