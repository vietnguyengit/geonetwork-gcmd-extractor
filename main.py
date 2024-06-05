import os
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsLike, Or, And, PropertyIsNotEqualTo
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


# Initialise output folder and files
def initialise_output():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    for file in FILES_TO_CHECK:
        if os.path.exists(file):
            os.remove(file)
            print(f"{file} deleted.")


def get_is_harvested(xml_string):
    xml_doc = minidom.parseString(xml_string)
    isHarvested_tag = xml_doc.getElementsByTagName("isHarvested")
    if isHarvested_tag:
        for node in isHarvested_tag:
            status = node.firstChild.nodeValue
            if status == "y":
                return True
    return False


def record_process(
    record,
    unique_set,
    non_unique_set,
    unique_gcmd_thesaurus_set,
    failed_list_file,
):
    metadata_identifier = record.identifier
    metadata_title = ""
    thesaurus_title = ""
    thesaurus_type = ""
    gcmd_keywords = list()

    is_harvested = get_is_harvested(record.xml)
    for item in record.identification:
        try:
            metadata_title = item.title
        except TypeError:
            pass
        for md_keywords in item.keywords:
            if md_keywords is not None:
                try:
                    thesaurus = md_keywords.thesaurus
                    try:
                        thesaurus_title = thesaurus["title"]
                        if thesaurus_title is not None and (
                            "gcmd" in thesaurus_title.lower()
                            or "global change master directory"
                            in thesaurus_title.lower()
                        ):
                            if (
                                "palaeo temporal coverage"
                                not in thesaurus_title.lower()
                            ):
                                for keyword in md_keywords.keywords:
                                    if keyword.name:
                                        gcmd_keywords.append(keyword.name)
                    except TypeError:
                        pass
                    try:
                        thesaurus_type = md_keywords.type
                    except TypeError:
                        pass
                except TypeError:
                    pass

    if not gcmd_keywords:
        failed_list_file.write(f"{metadata_identifier}\n")
    else:
        unique_gcmd_thesaurus_set.add((thesaurus_title, metadata_identifier))
        for keyword in gcmd_keywords:
            unique_set.add((thesaurus_title, keyword))
            non_unique_set.add(
                (
                    metadata_identifier,
                    metadata_title,
                    is_harvested,
                    thesaurus_title,
                    thesaurus_type,
                    keyword,
                )
            )


# Fetch records in batches and process them
def fetch_and_process_records(csw, final_query, total_records):
    unique_set = set()
    non_unique_set = set()
    unique_gcmd_thesaurus_set = set()

    with open(FILES_TO_CHECK[3], "w") as failed_list_file:

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
                    record_process(
                        csw.records[rec],
                        unique_set,
                        non_unique_set,
                        unique_gcmd_thesaurus_set,
                        failed_list_file,
                    )
                pbar.update(min(BATCH_SIZE, total_records - start_position + 1))

    # Write unique and non-unique sets to their respective files
    with open(FILES_TO_CHECK[0], "w") as unique_set_file:
        unique_set_file.write("thesaurus_title, gcmd_keyword\n")
        for thesaurus_title, keyword in unique_set:
            unique_set_file.write(f'"{thesaurus_title}", "{keyword}"\n')

    with open(FILES_TO_CHECK[1], "w") as non_unique_set_file:
        non_unique_set_file.write(
            "metadata_identifier, metadata_title, is_harvested, thesaurus_title, thesaurus_type, gcmd_keyword\n"
        )
        for (
            metadata_identifier,
            metadata_title,
            is_harvested,
            thesaurus_title,
            thesaurus_type,
            keyword,
        ) in non_unique_set:
            non_unique_set_file.write(
                f'{metadata_identifier}, "{metadata_title}", {is_harvested}, "{thesaurus_title}", {thesaurus_type}, "{keyword}"\n'
            )

    with open(FILES_TO_CHECK[2], "w") as unique_gcmd_thesaurus_file:
        unique_gcmd_thesaurus_file.write("thesaurus_title, metadata_identifier\n")
        for thesaurus_title, metadata_identifier in unique_gcmd_thesaurus_set:
            unique_gcmd_thesaurus_file.write(
                f'"{thesaurus_title}", {metadata_identifier}\n'
            )


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

    fetch_and_process_records(csw, final_query, total_records)

    print("----------- Completed -----------")


if __name__ == "__main__":
    main()
