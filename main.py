import csv
import sys
import json
import re
import xml.etree.ElementTree as ET
from argparse import ArgumentParser
from datasets import load_dataset
# from hugginface_hub import login


def read_xml_from_source(filename: str) -> dict[str, str]:
    metadata_xmls = {}

    with open(filename, 'r', encoding="utf-8") as src:
        lines = src.readlines()

        for line in lines:
            json_line = json.loads(line)
            metadata_xmls.update({json_line["id"]: json_line["xml"].replace("'", "").replace("\"", "'").strip()})

    return metadata_xmls


def parse_xml_to_metadata(xmls: dict[str, str], to_csv: bool = True) -> dict[str, dict]:
    separator = "| "
    if not to_csv:
        separator = ", "

    metadata_xmls = {id: {} for id in xmls.keys()}

    for id, xml in xmls.items():
        tree_root = ET.fromstring(xml)

        data = {"authors": "", "publish_year": "", "no_pages": 0, "keywords": "", "country": "", "publish_type": ""}

        base_path = "./PubmedArticle/MedlineCitation"
        tags_paths = {
            "authors": {"last_name": "./AuthorList/Author/LastName", "first_name": "./AuthorList/Author/ForeName"},
            "publish_year": "./Journal/JournalIssue/PubDate/Year",
            "no_pages": {"end_page": "./Pagination/EndPage", "start_page": "./Pagination/StartPage"},
            "keywords": "./MeshHeading/DescriptorName",
            "country": "./Country",
            "publish_type": "./PublicationTypeList/PublicationType"
        }

        for item in tree_root.findall(base_path):
            for child in item:
                if child.tag == "Article":
                    authors_lastnames = [i.text for i in child.findall(tags_paths["authors"]["last_name"])]
                    authors_firstnames = [i.text for i in child.findall(tags_paths["authors"]["first_name"])]
                    data["authors"] = [".".join(elem) for elem in zip(authors_firstnames, authors_lastnames)]

                    probe_year = child.findall(tags_paths["publish_year"])

                    if len(probe_year) > 0:
                        data["publish_year"] = probe_year[0].text

                    else:
                        data["publish_year"] = (child.findall("./Journal/JournalIssue/PubDate/MedlineDate")[0].text).split(" ")[0]

                    data["publish_type"] = separator.join([i.text for i in child.findall(tags_paths["publish_type"])])


                    try:
                        data["no_pages"] = int(re.sub(r'[A-Za-z]', "", child.findall(tags_paths["no_pages"]["end_page"])[0].text)) - int(re.sub(r'[A-Za-z]', "", child.findall(tags_paths["no_pages"]["start_page"])[0].text))

                    except:
                        data["no_pages"] = 1

                if child.tag == "MedlineJournalInfo":
                    data["country"] = child.findall(tags_paths["country"])[0].text

                if child.tag == "MeshHeadingList":
                    data["keywords"] = [i.text for i in child.findall(tags_paths["keywords"])]

        metadata_xmls[id] = data
        # print(data)
        # break

    return metadata_xmls


# this is a skippable step?
def save_to_CSV(items, filename: str) -> None:
    fields = ['passage_id', 'pubType', 'pubYear', 'noPages', 'country', 'authors', 'keywords']

    # writing to csv file
    with open(filename, 'w', encoding="utf-8") as csvfile:
        # creating a csv dict writer object
        writer = csv.DictWriter(csvfile, fieldnames=fields)

        # writing headers (field names)
        writer.writeheader()

        # writing data rows
        for id, data in items.items():
            print(data)
            print("-------------------")
            writer.writerow({'passage_id': id, 'pubType': data["publish_type"], 'pubYear': data["publish_year"], 'noPages': data["no_pages"], 'country': data["country"], 'authors': data["authors"], 'keywords': data["keywords"]})



def add_metadata_to_dataset(metadata:dict, dataset_name: str, new_set_name: str, commit_msg:str) -> None:

    def add_new_fields(example):
        example.update(metadata[example["id"]])

        return example

    base_dataset = load_dataset(dataset_name, "text-corpus")
    QAP_triplets = mini_bioasq_qas_ds = load_dataset(dataset_name, 'question-answer-passages')

    # print("-" * 50)
    # print(base_dataset)
    # print("-"*50)
    # print(base_dataset.column_names)
    # print("#"*50)

    new_dataset = base_dataset.map(add_new_fields)

    # print(new_dataset)
    # print("-"*50)
    # print(new_dataset.column_names)
    # print("-" * 50)
    # print(new_dataset["test"][0])

    new_dataset.push_to_hub(new_set_name, commit_message=f"{commit_msg}")
    QAP_triplets.push_to_hub(new_set_name, commit_message="Add QAP triplets") #TODO: test if it works that way


def process_metadata(argv=None) -> None:
    parser = ArgumentParser(description="Parsing of bioasq metadata from jsonl containing XML data")

    parser.add_argument("--source_path", required=True, type=str, help="Path to source .jsonl file")
    parser.add_argument("--save_path", required=True, type=str,
                        help="Path to the .csv file where the results of parsing should be placed")

    args = parser.parse_args(argv)

    xmls = read_xml_from_source(args.source_path)

    metadata = parse_xml_to_metadata(xmls, False)

    # save_to_CSV(metadata, args.save_path)

    add_metadata_to_dataset(metadata, "enelpol/rag-mini-bioasq", "enelpol/rag-mini-bioasq-with-metadata", "Authors & keywords as sequences")


if __name__ == '__main__':
    sys.exit(process_metadata())
