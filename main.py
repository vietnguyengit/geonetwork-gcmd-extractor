from owslib.csw import CatalogueServiceWeb
from owslib.fes import (
    PropertyIsLike,
    Or,
    And,
    PropertyIsNotEqualTo,
)
from xml.dom import minidom
from tqdm import tqdm
import os

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
    maxrecords=1,
)

# Total number of records
total_records = csw.results["matches"]
# total_records = 50
print(f"Total records: {total_records}")

gcmdKeywordsSet = set()
failedList = set()
batch_size = 10


output_folder = "outputs"
os.makedirs(output_folder, exist_ok=True)
# List of files to check and delete if they exist, prefixed with the output folder
files_to_check = [
    os.path.join(output_folder, "gcmd_keywords.txt"),
    os.path.join(output_folder, "records_failed.txt"),
]
# Check and delete the files if they exist
for file in files_to_check:
    if os.path.exists(file):
        os.remove(file)
        print(f"{file} deleted.")


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

            descriptiveKeywords = xmldoc.getElementsByTagName("mri:descriptiveKeywords")
            if descriptiveKeywords is not None:
                for descriptiveKeyword in descriptiveKeywords:
                    mriKeywords = descriptiveKeyword.getElementsByTagName("mri:keyword")
                    if mriKeywords is not None:
                        for mriKeyword in mriKeywords:
                            gcoString = mriKeyword.getElementsByTagName(
                                "gco:CharacterString"
                            )
                            if gcoString is not None:
                                for content in gcoString:
                                    gcmdKeyword = content.firstChild.nodeValue
                                    if gcmdKeyword is not None and gcmdKeyword != "":
                                        gcmdKeywordsSet.add(gcmdKeyword)

            anchorKeywords = xmldoc.getElementsByTagName("gcx:Anchor")
            if anchorKeywords is not None:
                for anchorKeyword in anchorKeywords:
                    keywordValue = anchorKeyword.firstChild.nodeValue
                    if (
                        keywordValue is not None
                        and keywordValue != ""
                        and "gcmd" in anchorKeyword.getAttribute("xlink:href").lower()
                        and "geonetwork" not in keywordValue.lower()
                    ):
                        gcmdKeywordsSet.add(keywordValue)

            if anchorKeywords is None and descriptiveKeywords is None:
                failedList.add(rec)

        pbar.update(min(batch_size, total_records - start_position + 1))
    print("----------- Completed -----------")

# Save the keywords to a file for future reference
with open(files_to_check[0], "w") as file:
    for key in gcmdKeywordsSet:
        file.write(f"{key}\n")

# Save the failed records to a file for future reference
with open(files_to_check[1], "w") as file:
    for key in failedList:
        file.write(f"{key}\n")
