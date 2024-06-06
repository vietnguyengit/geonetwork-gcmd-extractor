import os
import json
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsLike, Or, And, PropertyIsNotEqualTo
from xml.dom import minidom
from tqdm import tqdm

# from utils.nlp_grouping import GroupingSimilarTexts


def create_queries():
    gcmd_query_lower = PropertyIsLike("AnyText", "%%gcmd%%")
    gcmd_query_upper = PropertyIsLike("AnyText", "%%GCMD%%")
    gcmd_query_full = PropertyIsLike("AnyText", "%%Global Change Master Directory%%")
    combined_gcmd_query = Or([gcmd_query_lower, gcmd_query_upper, gcmd_query_full])
    aodn_exclude_query = PropertyIsNotEqualTo(
        "AnyText", "AODN Discovery Parameter Vocabulary"
    )
    return And([combined_gcmd_query, aodn_exclude_query])


def load_config(config_path):
    with open(config_path, "r") as f:
        return json.load(f)


def process_keyword(keyword):
    if "|" in keyword:
        return keyword.split("|")[-1].strip()
    elif ">" in keyword:
        return keyword.split(">")[-1].strip()
    return keyword.strip()


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
    gcmd_keywords = []
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
                    if (
                        thesaurus["title"] is not None
                        and (
                            "gcmd" in thesaurus["title"].lower()
                            or "global change master directory"
                            in thesaurus["title"].lower()
                        )
                        and "palaeo temporal coverage" not in thesaurus["title"].lower()
                    ):
                        thesaurus_title = thesaurus["title"]
                        try:
                            thesaurus_type = md_keywords.type
                        except TypeError:
                            pass
                        for keyword in md_keywords.keywords:
                            if keyword.name:
                                gcmd_keywords.append(keyword.name)
                except TypeError:
                    pass
    if not gcmd_keywords:
        failed_list_file.write(f"{metadata_identifier}\n")
    else:
        unique_gcmd_thesaurus_set.add((thesaurus_title, metadata_identifier))
        for keyword in gcmd_keywords:
            unique_set.add((thesaurus_title, keyword.replace('"', "")))
            non_unique_set.add(
                (
                    metadata_identifier,
                    metadata_title.replace('"', ""),
                    is_harvested,
                    thesaurus_title,
                    thesaurus_type,
                    keyword.replace('"', ""),
                )
            )


class GCMDProcessor:
    def __init__(self, config_path):
        self.config = load_config(config_path)
        self.output_folder = self.config["output_folder"]
        self.files_to_check = {
            "unique_gcmd_keywords_file": os.path.join(
                self.output_folder, self.config["unique_gcmd_keywords_file"]
            ),
            "non_unique_gcmd_keywords_file": os.path.join(
                self.output_folder, self.config["non_unique_gcmd_keywords_file"]
            ),
            "unique_gcmd_thesaurus_file": os.path.join(
                self.output_folder, self.config["unique_gcmd_thesaurus_file"]
            ),
            "records_failed_file": os.path.join(
                self.output_folder, self.config["records_failed_file"]
            ),
            "non_unique_last_words_file": os.path.join(
                self.output_folder, self.config["non_unique_last_words_file"]
            ),
        }
        self.csw_url = self.config["csw_url"]
        self.output_schema = self.config["output_schema"]
        self.batch_size = self.config["batch_size"]
        self.csw = None

    def setup_csw_service(self):
        self.csw = CatalogueServiceWeb(self.csw_url)

    def initialise_output(self):
        os.makedirs(self.output_folder, exist_ok=True)
        for file in self.files_to_check.values():
            if os.path.exists(file):
                os.remove(file)
                print(f"{file} deleted.")

    def fetch_and_process_records(self, final_query, total_records):
        unique_set = set()
        non_unique_set = set()
        unique_gcmd_thesaurus_set = set()

        with open(self.files_to_check["records_failed_file"], "w") as failed_list_file:
            with tqdm(total=total_records, desc="Processing records") as pbar:
                for start_position in range(1, total_records + 1, self.batch_size):
                    self.csw.getrecords2(
                        constraints=[final_query],
                        outputschema="http://standards.iso.org/iso/19115/-3/mdb/2.0",
                        esn="full",
                        startposition=start_position,
                        maxrecords=self.batch_size,
                    )
                    for rec in self.csw.records:
                        record_process(
                            self.csw.records[rec],
                            unique_set,
                            non_unique_set,
                            unique_gcmd_thesaurus_set,
                            failed_list_file,
                        )
                    pbar.update(
                        min(self.batch_size, total_records - start_position + 1)
                    )

        # Write unique and non-unique sets to their respective files
        with open(
            self.files_to_check["unique_gcmd_keywords_file"], "w"
        ) as unique_set_file:
            unique_set_file.write("thesaurus_title, gcmd_keyword\n")
            for thesaurus_title, keyword in unique_set:
                unique_set_file.write(f'"{thesaurus_title}", "{keyword}"\n')

        with open(
            self.files_to_check["non_unique_gcmd_keywords_file"], "w"
        ) as non_unique_set_file:
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

        with open(
            self.files_to_check["non_unique_last_words_file"], "w"
        ) as non_unique_set_file:
            non_unique_set_file.write(
                "metadata_identifier, metadata_title, is_harvested, thesaurus_title, thesaurus_type, last_word_term\n"
            )
            for (
                metadata_identifier,
                metadata_title,
                is_harvested,
                thesaurus_title,
                thesaurus_type,
                keyword,
            ) in non_unique_set:
                last_term_word = str(process_keyword(keyword.replace('"', ""))).upper()
                non_unique_set_file.write(
                    f'{metadata_identifier}, "{metadata_title}", {is_harvested}, "{thesaurus_title}", {thesaurus_type}, "{last_term_word}"\n'
                )

        with open(
            self.files_to_check["unique_gcmd_thesaurus_file"], "w"
        ) as unique_gcmd_thesaurus_file:
            unique_gcmd_thesaurus_file.write("thesaurus_title, metadata_identifier\n")
            for thesaurus_title, metadata_identifier in unique_gcmd_thesaurus_set:
                unique_gcmd_thesaurus_file.write(
                    f'"{thesaurus_title}", {metadata_identifier}\n'
                )

    def run(self, total_records=None):
        final_query = create_queries()
        self.setup_csw_service()
        # Get the initial record to determine the total number of records if not in test mode
        if total_records is None:
            self.csw.getrecords2(
                constraints=[final_query],
                outputschema=self.output_schema,
                maxrecords=1,
            )
            total_records = self.csw.results["matches"]
        self.initialise_output()
        print("---------------------------------")
        print(f"Total records: {total_records}")
        self.fetch_and_process_records(final_query, total_records)
        print("---------------------------------")
        print("Completed")
