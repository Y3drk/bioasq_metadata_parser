import csv
import sys
import json
import re
import xml.etree.ElementTree as ET
from argparse import ArgumentParser
from datasets import load_dataset

PRINT_SEPARATOR = "-" * 40
ARBITRARY_BOOK_LENGTH = 125


def read_xml_from_source(filename: str) -> dict[str, str]:
    metadata_xmls = {}

    with open(filename, 'r', encoding="utf-8") as src:
        lines = src.readlines()

        for line in lines:
            json_line = json.loads(line)
            metadata_xmls.update({json_line["id"]: json_line["xml"].replace("'", "").replace("\"", "'").strip()})

    return metadata_xmls


def parse_xml_to_metadata(xmls: dict[str, str], to_csv: bool = True) -> dict[str, dict]:
    metadata_xmls = {id: {} for id in xmls.keys()}

    for id, xml in xmls.items():
        tree_root = ET.fromstring(xml)

        data = {"authors": [], "publish_year": 0, "no_pages": 0, "keywords": [], "country": "", "publish_type": []}

        base_path = "./PubmedArticle/MedlineCitation"
        tags_paths = {
            "authors": {"last_name": "./AuthorList/Author/LastName", "first_name": "./AuthorList/Author/ForeName"},
            "publish_year": "./Journal/JournalIssue/PubDate/Year",
            "no_pages": {"end_page": "./Pagination/EndPage", "start_page": "./Pagination/StartPage"},
            "keywords": "./MeshHeading/DescriptorName",
            "country": "./Country",
            "publish_type": "./PublicationTypeList/PublicationType"
        }

        found_items = tree_root.findall(base_path)

        if len(found_items) == 0:
            book_publish_year_path = "./PubmedBookArticle/BookDocument/Book/PubDate/Year"
            book_author_path = ["./PubmedBookArticle/BookDocument/Book/AuthorList/Author/LastName", "./PubmedBookArticle/BookDocument/Book/AuthorList/Author/ForeName"]
            book_country_path = "./PubmedBookArticle/BookDocument/Book/Publisher/PublisherLocation"

            data["no_pages"] = ARBITRARY_BOOK_LENGTH

            if to_csv:
                data["publish_type"] = "Book"

            else:
                data["publish_type"] = ["Book"]

            try:
                data["publish_year"] = int(tree_root.findall(book_publish_year_path)[0].text)
                data["country"] = tree_root.findall(book_country_path)[0].text

            except:
                print(f"No data found for XML:\n{xml}")
                print(PRINT_SEPARATOR)


            authors_lastnames = [i.text for i in child.findall(book_author_path[0])]
            authors_firstnames = [i.text for i in child.findall(book_author_path[1])]
            authors = [".".join(elem) for elem in zip(authors_firstnames, authors_lastnames)]

            if to_csv:
                data["authors"] = "| ".join(authors)
            else:
                data["authors"] = authors

        else:
            for item in found_items:
                for child in item:
                    if child.tag == "Article":
                        authors_lastnames = [i.text for i in child.findall(tags_paths["authors"]["last_name"])]
                        authors_firstnames = [i.text for i in child.findall(tags_paths["authors"]["first_name"])]
                        authors = [".".join(elem) for elem in zip(authors_firstnames, authors_lastnames)]

                        if to_csv:
                            data["authors"] = "| ".join(authors)
                        else:
                            data["authors"] = authors

                        probe_year = child.findall(tags_paths["publish_year"])

                        if len(probe_year) > 0:
                            data["publish_year"] = int(probe_year[0].text)

                        else:
                            data["publish_year"] = int(
                                (child.findall("./Journal/JournalIssue/PubDate/MedlineDate")[0].text).split(" ")[0].split(
                                    "-")[0])

                        publish_types = [i.text for i in child.findall(tags_paths["publish_type"])]
                        if to_csv:
                            data["publish_type"] = "| ".join(publish_types)

                        else:
                            data["publish_type"] = publish_types

                        try:
                            probe_end_page = child.findall(tags_paths["no_pages"]["end_page"])[0].text
                            probe_start_page = child.findall(tags_paths["no_pages"]["start_page"])[0].text

                            num_start_page = re.sub(r'[A-Za-z]', "", probe_start_page)
                            num_end_page = re.sub(r'[A-Za-z]', "", probe_end_page)

                            int_end_page = int(num_end_page)
                            int_start_page = int(num_start_page)

                            if ("S" in probe_end_page and "S" in probe_start_page):
                                data["no_pages"] = int_end_page - int(num_start_page[-len(num_end_page):])

                            elif "e" in probe_end_page:
                                pages_data = (child.findall("./Pagination/MedlinePgn")[0].text).split(";")[0].split(",")[0]
                                start_page_parsed = int(pages_data.split("-")[0])
                                end_page_parsed = int(pages_data.split("-")[1])
                                data["no_pages"] = max(end_page_parsed - start_page_parsed, 1)

                            else:
                                if int_end_page < int_start_page:
                                    data["no_pages"] = min(int_end_page, 100)

                                else:
                                    data["no_pages"] = int_end_page - int_start_page

                        except:
                            data["no_pages"] = 1

                    if child.tag == "MedlineJournalInfo":
                        data["country"] = child.findall(tags_paths["country"])[0].text

                    if child.tag == "MeshHeadingList":
                        probe_keywords = [i.text for i in child.findall(tags_paths["keywords"])]

                        if to_csv:
                            data["keywords"] = "| ".join(probe_keywords)

                        elif len(probe_keywords) > 0:
                            data["keywords"] = probe_keywords

                        else:
                            data["keywords"] = None

        metadata_xmls[id] = data

    return metadata_xmls


def save_to_CSV(items, filename: str) -> None:
    fields = ['passage_id', 'pubType', 'pubYear', 'noPages', 'country', 'authors', 'keywords']

    # writing to csv file
    with open(filename, 'w', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)

        writer.writeheader()

        for id, data in items.items():
            writer.writerow({'passage_id': id, 'pubType': data["publish_type"], 'pubYear': data["publish_year"],
                             'noPages': data["no_pages"], 'country': data["country"], 'authors': data["authors"],
                             'keywords': data["keywords"]})


def add_metadata_to_dataset(metadata: dict, dataset_name: str, new_set_name: str, commit_msg: str) -> None:
    def add_new_fields(example) -> None:
        example.update(metadata[example["id"]])

        if example["no_pages"] <= 0:
            print(example["id"], example["no_pages"])
            print(PRINT_SEPARATOR)

        return example

    base_dataset = load_dataset(dataset_name, "text-corpus")
    # QAP_triplets = load_dataset(dataset_name, 'question-answer-passages')

    new_dataset = base_dataset.map(add_new_fields, desc="adding metadata")

    new_dataset.push_to_hub(new_set_name, "text-corpus", commit_message=f"{commit_msg}")
    # QAP_triplets.push_to_hub(new_set_name, 'question-answer-passages', commit_message="Add QAP triplets") #TODO: test if it works that way


def process_metadata(argv=None) -> None:
    parser = ArgumentParser(description="Parsing of bioasq metadata from jsonl containing XML data")

    parser.add_argument("--source_path", required=True, type=str, help="Path to source .jsonl file")
    parser.add_argument("--save_path", required=True, type=str,
                        help="Path to the .csv file where the results of parsing should be placed")

    args = parser.parse_args(argv)

    print(f"{PRINT_SEPARATOR}\nParsing the XML sourcefiles\n{PRINT_SEPARATOR}")
    xmls = read_xml_from_source(args.source_path)

    metadata = parse_xml_to_metadata(xmls, False)

    # save_to_CSV(metadata, args.save_path)

    print(f"{PRINT_SEPARATOR}\nAddition of metadata do the dataset\n{PRINT_SEPARATOR}")
    add_metadata_to_dataset(metadata, "enelpol/rag-mini-bioasq", "enelpol/rag-mini-bioasq-with-metadata",
                            "negative no_pages fix + different treatment for books")


if __name__ == '__main__':
    sys.exit(process_metadata())
