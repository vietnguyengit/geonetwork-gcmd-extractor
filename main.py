import os
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsLike, Or, And, PropertyIsNotEqualTo, PropertyIsEqualTo
from xml.dom import minidom
from tqdm import tqdm

# Define constants
OUTPUT_FOLDER = "outputs"
FILES_TO_CHECK = [
    os.path.join(OUTPUT_FOLDER, "unique_gcmd_keywords.csv"),
    os.path.join(OUTPUT_FOLDER, "non_unique_gcmd_keywords.csv"),
    os.path.join(OUTPUT_FOLDER, "unique_gcmd_thesaurus.csv"),
    os.path.join(OUTPUT_FOLDER, "records_failed.txt"),
]
BATCH_SIZE = 10


# Define the queries to search for records using GCMD Keywords
def create_queries():
    gcmd_query_lower = PropertyIsLike("AnyText", "%gcmd%")
    gcmd_query_upper = PropertyIsLike("AnyText", "%GCMD%")
    gcmd_query_full = PropertyIsLike("AnyText", "%Global Change Master Directory%")

    # for debugging only
    # uuid_query = PropertyIsLike("Identifier", "f5625450-ee31-4511-9373-53ff4c6ef370")

    combined_gcmd_query = Or([gcmd_query_lower, gcmd_query_upper, gcmd_query_full])
    aodn_exclude_query = PropertyIsNotEqualTo(
        "AnyText", "AODN Discovery Parameter Vocabulary"
    )
    return And([combined_gcmd_query, aodn_exclude_query])


# Setup CSW connection
def setup_csw_service():
    return CatalogueServiceWeb(
        "https://catalogue.aodn.org.au/geonetwork/srv/eng/csw?request=GetCapabilities&service=CSW&version=2.0.2"
    )


# Utility function to get string value from XML element
def get_string_value(element, tag):
    string_tag = element.getElementsByTagName(tag)
    if string_tag is not None:
        for string_element in string_tag:
            try:
                return string_element.firstChild.nodeValue
            except AttributeError:
                pass
    return None


# Initialise output folder and files
def initialise_output():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    for file in FILES_TO_CHECK:
        if os.path.exists(file):
            os.remove(file)
            print(f"{file} deleted.")


# Fetch records in batches and process them
def fetch_and_process_records(csw, final_query, total_records):
    unique_gcmd_keywords_set = set()
    non_unique_gcmd_keywords_set = set()
    unique_gcmd_thesaurus = set()
    failed_list = set()

    with tqdm(total=total_records, desc="Processing records") as pbar:
        for start_position in range(1, total_records + 1, BATCH_SIZE):
            csw.getrecords2(
                constraints=[final_query],
                outputschema="http://standards.iso.org/iso/19115/-3/mdb/2.0",
                esn="full",
                startposition=start_position,
                maxrecords=BATCH_SIZE,
            )
            for rec in csw.records:
                process_record(
                    csw.records[rec].xml,
                    rec,
                    unique_gcmd_keywords_set,
                    non_unique_gcmd_keywords_set,
                    unique_gcmd_thesaurus,
                    failed_list,
                )
            pbar.update(min(BATCH_SIZE, total_records - start_position + 1))

    return (
        unique_gcmd_keywords_set,
        non_unique_gcmd_keywords_set,
        unique_gcmd_thesaurus,
        failed_list,
    )


# Process individual record
def process_record(
    xml_string, rec_id, unique_set, non_unique_set, unique_gcmd_thesaurus, failed_list
):
    xml_doc = minidom.parseString(xml_string)
    descriptive_keywords = xml_doc.getElementsByTagName("mri:descriptiveKeywords")

    if descriptive_keywords is not None:
        for descriptive_keyword in descriptive_keywords:
            if (
                "gcmd" in descriptive_keyword.toxml().lower()
                or "global change master directory"
                in descriptive_keyword.toxml().lower()
            ):
                if (
                    "palaeo temporal coverage"
                    not in descriptive_keyword.toxml().lower()
                ):
                    thesaurus_value = extract_thesaurus_value(descriptive_keyword)
                    unique_gcmd_thesaurus.add(f'"{thesaurus_value}", {rec_id}')

                    keyword = extract_keyword(descriptive_keyword)
                    if keyword:
                        unique_set.add(f'"{thesaurus_value}", "{keyword}"')
                        non_unique_set.add(
                            f'"{thesaurus_value}", "{keyword}", {rec_id}'
                        )
                    else:
                        failed_list.add(rec_id)


# Extract thesaurus value from descriptive keyword
def extract_thesaurus_value(descriptive_keyword):
    mri_thesaurus_name_tag = descriptive_keyword.getElementsByTagName(
        "mri:thesaurusName"
    )
    if mri_thesaurus_name_tag is not None:
        for mri_thesaurus_name_element in mri_thesaurus_name_tag:
            thesaurus_title_tag = mri_thesaurus_name_element.getElementsByTagName(
                "cit:title"
            )
            if thesaurus_title_tag is not None:
                for thesaurus_title_element in thesaurus_title_tag:
                    return get_string_value(
                        thesaurus_title_element, "gco:CharacterString"
                    )
    return ""


# Extract keyword from descriptive keyword
def extract_keyword(descriptive_keyword):
    mri_keywords = descriptive_keyword.getElementsByTagName("mri:keyword")
    if mri_keywords is not None:
        for mri_keyword in mri_keywords:
            keyword = get_string_value(
                mri_keyword, "gco:CharacterString"
            ) or get_string_value(mri_keyword, "gcx:Anchor")
            if keyword:
                return keyword.replace('"', "")
    return None


# Save results to files
def save_results(unique_set, non_unique_set, unique_gcmd_thesaurus, failed_list):
    with open(FILES_TO_CHECK[0], "w") as file:
        file.write("Thesaurus, Keywords\n")
        for key in unique_set:
            file.write(f"{key}\n")

    with open(FILES_TO_CHECK[1], "w") as file:
        file.write("Thesaurus, Keywords, Record's Identifier\n")
        for key in non_unique_set:
            file.write(f"{key}\n")

    with open(FILES_TO_CHECK[2], "w") as file:
        file.write("Thesaurus, Record's Identifier\n")
        for key in unique_gcmd_thesaurus:
            file.write(f"{key}\n")

    with open(FILES_TO_CHECK[3], "w") as file:
        for key in failed_list:
            file.write(f"{key}\n")


# Main script execution
def main():
    final_query = create_queries()
    csw = setup_csw_service()

    # Get the initial record to determine the total number of records
    csw.getrecords2(
        constraints=[final_query],
        outputschema="http://standards.iso.org/iso/19115/-3/mdb/2.0",
        maxrecords=1,
    )
    total_records = csw.results["matches"]

    initialise_output()
    print(f"Total records: {total_records}")

    (
        unique_gcmd_keywords_set,
        non_unique_gcmd_keywords_set,
        unique_gcmd_thesaurus,
        failed_list,
    ) = fetch_and_process_records(csw, final_query, total_records)
    save_results(
        unique_gcmd_keywords_set,
        non_unique_gcmd_keywords_set,
        unique_gcmd_thesaurus,
        failed_list,
    )

    print("----------- Completed -----------")


if __name__ == "__main__":
    main()
