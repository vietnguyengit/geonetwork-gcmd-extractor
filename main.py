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


def create_queries():
    """Define the queries to search for records using GCMD Keywords."""
    gcmd_query_lower = PropertyIsLike("AnyText", "%gcmd%")
    gcmd_query_upper = PropertyIsLike("AnyText", "%GCMD%")
    gcmd_query_full = PropertyIsLike("AnyText", "%Global Change Master Directory%")

    combined_gcmd_query = Or([gcmd_query_lower, gcmd_query_upper, gcmd_query_full])
    aodn_exclude_query = PropertyIsNotEqualTo(
        "AnyText", "AODN Discovery Parameter Vocabulary"
    )

    return And([combined_gcmd_query, aodn_exclude_query])


def setup_csw_service():
    """Setup CSW connection."""
    try:
        csw = CatalogueServiceWeb(
            "https://catalogue.aodn.org.au/geonetwork/srv/eng/csw?request=GetCapabilities&service=CSW&version=2.0.2"
        )
        return csw
    except Exception as e:
        print(f"Error setting up CSW service: {e}")
        return None


def initialise_output():
    """Initialise output folder and files."""
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    for file in FILES_TO_CHECK:
        if os.path.exists(file):
            os.remove(file)
            print(f"{file} deleted.")


def get_is_harvested(xml_string):
    """Determine if the record is harvested."""
    try:
        xml_doc = minidom.parseString(xml_string)
        is_harvested_tag = xml_doc.getElementsByTagName("isHarvested")
        if is_harvested_tag:
            for node in is_harvested_tag:
                if node.firstChild.nodeValue == "y":
                    return True
    except Exception as e:
        print(f"Error parsing XML: {e}")
    return False


def process_keywords(md_keywords):
    """Process keywords from metadata."""
    gcmd_keywords = []
    thesaurus_title = ""
    thesaurus_type = ""

    if md_keywords is None:
        return gcmd_keywords, thesaurus_title, thesaurus_type

    thesaurus = getattr(md_keywords, "thesaurus", {})
    thesaurus_title = thesaurus.get("title", "")

    if (
        thesaurus_title
        and (
            "gcmd" in thesaurus_title.lower()
            or "global change master directory" in thesaurus_title.lower()
        )
        and "palaeo temporal coverage" not in thesaurus_title.lower()
    ):

        for keyword in md_keywords.keywords:
            if keyword.name:
                gcmd_keywords.append(keyword.name)

    thesaurus_type = getattr(md_keywords, "type", "")

    return gcmd_keywords, thesaurus_title, thesaurus_type


def process_and_store_record(
    record, unique_set, non_unique_set, unique_gcmd_thesaurus_set, failed_list_file
):
    """Process an individual record and update the respective sets and file."""
    metadata_identifier = record.identifier
    metadata_title = ""
    gcmd_keywords_total = []

    is_harvested = get_is_harvested(record.xml)

    for item in record.identification:
        metadata_title = getattr(item, "title", "")

        for md_keywords in item.keywords:
            gcmd_keywords, thesaurus_title, thesaurus_type = process_keywords(
                md_keywords
            )
            if gcmd_keywords:
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
            gcmd_keywords_total.extend(gcmd_keywords)

    if not gcmd_keywords_total:
        failed_list_file.write(f"{metadata_identifier}\n")


def fetch_and_process_records(csw, final_query, total_records):
    """Fetch records in batches and process them."""
    unique_set = set()
    non_unique_set = set()
    unique_gcmd_thesaurus_set = set()

    with open(FILES_TO_CHECK[3], "w") as failed_list_file:
        with tqdm(total=total_records, desc="Processing records") as pbar:
            for start_position in range(1, total_records + 1, BATCH_SIZE):
                try:
                    csw.getrecords2(
                        constraints=[final_query],
                        outputschema="http://standards.iso.org/iso/19115/-3/mdb/2.0",
                        esn="full",
                        startposition=start_position,
                        maxrecords=BATCH_SIZE,
                    )

                    for rec in csw.records.values():
                        process_and_store_record(
                            rec,
                            unique_set,
                            non_unique_set,
                            unique_gcmd_thesaurus_set,
                            failed_list_file,
                        )

                    pbar.update(min(BATCH_SIZE, total_records - start_position + 1))
                except Exception as e:
                    print(f"Error fetching records: {e}")

    write_output_files(unique_set, non_unique_set, unique_gcmd_thesaurus_set)


def write_output_files(unique_set, non_unique_set, unique_gcmd_thesaurus_set):
    """Write the unique and non-unique sets to their respective files."""
    try:
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
    except Exception as e:
        print(f"Error writing output files: {e}")


def main():
    """Main script execution."""
    final_query = create_queries()
    csw = setup_csw_service()

    if not csw:
        print("Failed to set up CSW service. Exiting.")
        return

    # Get the initial record to determine the total number of records
    try:
        csw.getrecords2(
            constraints=[final_query],
            outputschema="http://standards.iso.org/iso/19115/-3/mdb/2.0",
            maxrecords=1,
        )
        total_records = csw.results["matches"]
    except Exception as e:
        print(f"Error querying CSW: {e}")
        return

    initialise_output()
    print(f"Total records: {total_records}")

    fetch_and_process_records(csw, final_query, total_records)


if __name__ == "__main__":
    main()
