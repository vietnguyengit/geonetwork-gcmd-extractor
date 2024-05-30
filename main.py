from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsLike, Or, And, PropertyIsNotEqualTo
from xml.dom import minidom
from tqdm import tqdm

# Define the queries to search for records using GCMD Keywords
gcmd_query_lower = PropertyIsLike("AnyText", "%gcmd%")
gcmd_query_upper = PropertyIsLike("AnyText", "%GCMD%")
gcmd_query_full = PropertyIsLike("AnyText", "%Global Change Master Directory%")

# Define the query to exclude records containing 'AODN Discovery Parameter Vocabulary'
aodn_exclude_query = PropertyIsNotEqualTo(
    "AnyText", "AODN Discovery Parameter Vocabulary"
)

# Combine all queries using Or and And filters
combined_gcmd_query = Or([gcmd_query_lower, gcmd_query_upper, gcmd_query_full])
final_query = And([combined_gcmd_query, aodn_exclude_query])

# Connect to the CSW service
csw = CatalogueServiceWeb(
    "https://catalogue.aodn.org.au/geonetwork/srv/eng/csw?request=GetCapabilities&service=CSW&version=2.0.2"
)

# Get the initial record to determine the total number of records
csw.getrecords2(
    constraints=[final_query],
    outputschema="http://standards.iso.org/iso/19115/-3/mdb/2.0",
    esn="full",
    maxrecords=1,
)

# Total number of records
total_records = csw.results["matches"]
print(f"Total records: {total_records}")

gcmdKeywordsSet = set()
batch_size = 10

# Loop through all records in batches with a progress bar
with tqdm(total=total_records, desc="Processing records") as pbar:
    for start_position in range(1, total_records + 1, batch_size):
        csw.getrecords2(
            constraints=[final_query],
            outputschema="http://standards.iso.org/iso/19115/-3/mdb/2.0",
            esn="full",
            startposition=start_position,
            maxrecords=batch_size,
        )

        for rec in csw.records:
            xmldoc = minidom.parseString(csw.records[rec].xml)
            keywords = xmldoc.getElementsByTagName("gcx:Anchor")
            for keyword in keywords:
                keywordValue = keyword.firstChild.nodeValue
                if (
                    "gcmd" in keyword.getAttribute("xlink:href").lower()
                    and "geonetwork" not in keywordValue.lower()
                ):
                    gcmdKeywordsSet.add(keywordValue)

        pbar.update(min(batch_size, total_records - start_position + 1))

# Print all unique GCMD keywords
for key in gcmdKeywordsSet:
    print(key)

# Save the keywords to a file for future reference
with open("gcmd_keywords.txt", "w") as file:
    for key in gcmdKeywordsSet:
        file.write(f"{key}\n")
