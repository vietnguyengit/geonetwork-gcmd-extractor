from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsLike
from xml.dom import minidom

# Define the query to search for records containing 'gcmd'
gcmd_query = PropertyIsLike("AnyText", "%gcmd%")

# Connect to the CSW service
csw = CatalogueServiceWeb(
    "https://catalogue.aodn.org.au/geonetwork/srv/eng/csw?request=GetCapabilities&service=CSW&version=2.0.2"
)

csw.getrecords2(
    constraints=[gcmd_query],
    outputschema="http://standards.iso.org/iso/19115/-3/mdb/2.0",
    maxrecords=10,
)

print(csw.results)
print(csw.identification)

for rec in csw.records:
    xmldoc = minidom.parseString(csw.records[rec].xml)
    print("======================================")
    print(xmldoc.toprettyxml())
    print("======================================")
    # gcmdKeywordsList = xmldoc.getElementsByTagName('dc:subject')
    # for keyword in gcmdKeywordsList:
    #     print(keyword.toxml())
