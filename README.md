plugin-harvester-rifcs
======================

(1) How to map data source xml file elements to Mint Fields?

This plugin is designed based on the sample data source xml file given by Macquarie University.
it reads all the elements in these sample files and check the fieldMapping info for each of them. We may need to
update the implementation if any new element is added to xml files and used in fieldMapping configuration.

The fieldMapping configuration is included in harvest json configuration file of each type,
and it use the identifier of data source xml file element as the key and the corresponding Mint field name as the
value.

e.g. Sample json configuration file for Parties_People.

{
    "harvester": {
        "type": "xml",
        "xml": {
            "fileLocation": "${test.dir}/people.xml",
            "recordIDPrefix": "au.edu.uts/parties/people/",
            "ignoredFields": [],
            "includedFields" : ["ID", "Given_Name", "Other_Names", "Family_Name", "Pref_Name", "Honorific", "Email", "Job_Title", "GroupID_1", "GroupID_2", "GroupID_3", "ANZSRC_FOR_1", "ANZSRC_FOR_2", "ANZSRC_FOR_3", "URI", "NLA_Party_Identifier", "ResearcherID", "openID", "Personal_URI", "Personal_Homepage", "Staff_Profile_Homepage", "Description"],
            "fieldsMapping": {
                "identifier.local": "ID",
                "name.primary.given": "Given_Name",
                "name.primary.middle": "Other_Names",
                "name.primary.family": "Family_Name",
                "name.primary.pref": "Pref_Name",
                "name.primary.title": "Honorific",
                "location.address.electronic.email": "Email",
                "name.primary.suffix": "Job_Title",
                "relatedObject.isMemberOf": "GroupID_1",
                "subject.anzsrc-for": "ANZSRC_FOR",
                "identifier.uri": "URI",
                "identifier.AU-ANL:PEAU": "NLA_Party_Identifier",
                "identifier.researcherid": "ResearcherID",
                "identifier.openid": "openID",
                "relatedInfo.website.Personal Homepage": "Personal_Homepage",
                "relatedInfo.website.Staff Homepage": "Staff_Profile_Homepage",
                "description.full": "Description"
	        }
        }
    },

    "transformer": {
        "curation": ["local"],
        "metadata": ["ingest-relations", "jsonVelocity"]
    },
    ....
}

(2) How to build the identifier of data source xml file element?

This plugin will read all the elements in xml data source file and build an identifier for them based on
their path which comprise of element name and the value of attribute 'type' if have (and sometimes other necessary
info if needed).

(3) What are the field name recognised by Mint?
    Parties_People: http://www.redboxresearchdata.com.au/documentation/system-administration/administering-mint/loading-data/loading-people-data
    Parties_Group:  http://www.redboxresearchdata.com.au/documentation/system-administration/administering-mint/loading-data/loading-group-data
    Activities:     http://www.redboxresearchdata.com.au/documentation/system-administration/administering-mint/loading-data/loading-activity-data

(4) An concrete example.

We have a xml data source file for Parties_People which contains info for Dr Jim Smith (shown below).

xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://ands.org.au/standards/rif-cs/registryObjects http://services.ands.org.au/documentation/rifcs/schema/registryObjects.xsd">
	<registryObject group="Macquarie University">
		<key>12345678</key>
		<originatingSource>idbank</originatingSource>
		<party type="person">
			<identifier type="local">MQ12345678</identifier>
			<identifier type="handle">http://hdl.handle.net/1959.19/775</identifier>
			<identifier type="AU-ANL:PEAU">http://nla.gov.au/nla.party-1234567</identifier>
			<identifier type="uri">http://mq.edu.au/people/staff/MQ12345678</identifier>
			<identifier type="researcherid">12341234</identifier>
			<identifier type="openid">http://open.id/12341234</identifier>
			<name type="primary">
				<namePart type="title">Dr.</namePart>
				<namePart type="given">James</namePart>
				<namePart type="pref">Jim</namePart>
				<namePart type="middle">Adam</namePart>
				<namePart type="family">Smith</namePart>
				<namePart type="suffix">PhD.</namePart>
			</name>
			<location>
				<address>
					<electronic type="email">
						<value>james.smith@mq.edu.au</value>
					</electronic>
					<physical>
						<addressPart type="telephoneNumber">0404123456</addressPart>
					</physical>
					<physical>
						<addressPart type="faxNumber">0291234567</addressPart>
					</physical>
					<physical type="streetAddress">
						<addressPart type="text">Level 4, C5A, Macquarie University, North Ryde, NSW 2109</addressPart>
					</physical>
				</address>
			</location>
			<subject type="anzsrc-for">0801</subject>
			<subject type="anzsrc-for">0804</subject>
			<subject type="anzsrc-for">0602</subject>
			<description type="full">Senior Lecturer in Faculty of Arts</description>
			<relatedInfo type="website">
				<identifier>http://mq.edu.au/staff/mq12345678</identifier>
				<title>Staff Homepage</title>
			</relatedInfo>
			<relatedInfo type="website">
				<identifier>http://www.facebook.com/john.smith99</identifier>
				<title>Personal Homepage</title>
			</relatedInfo>
			<relatedObject>
				<!-- key is the department/budget code for groups -->
				<key>4031</key>
				<relation type="isMemberOf"/>
			</relatedObject>
		</party>
	</registryObject>
</registryObjects>


What we are going to do is mapping Jim's email and facebook profile page to Mint field "Email" &
"Personal_Homepage" respectively.

In this file, we can find there is only one element's value is email and 2 are web address.
So for email fieldMapping configuration is quite simple, just map the element below to Mint field "Email".

			<electronic type="email">
				<value>james.smith@mq.edu.au</value>
			</electronic>

And the identifier of this element is "location.address.electronic.email". As you can see "location.address
.electronic" is the path of element names and the "email" is the value of attribute "type".

There are two web address elements in this file and they share the same element path and have the same value for
attribute "type" ("relatedInfo.website"). In this special case, we have to figure out how to differentiate them. We
can find that they have different value for "title", and what we are looking for is the one whose title is "Personal
Homepage".

So the identifier of this element is "relatedInfo.website.Personal Homepage", and we can put the following line to
filedMapping configuration in Parties_People.json.

"relatedInfo.website.Personal Homepage": "Personal_Homepage"
