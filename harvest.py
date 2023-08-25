"""Script for harvesting metadata / full text from UNT Digital Library."""
import argparse
import random
import time
from untdl_harvest import oai, pdf
import urllib.request


SERVER_URL = "https://digital.library.unt.edu"
PDF_QUALIFIER = "/m2/1/high_res_d/"
COLLECTIONS = {'UNTETD': 667, 'CRSR': 667, 'EOT': 666}
OPTIONS = {
    'metadataPrefix': 'untl_raw',
    'set': 'access_rights:public'
}
NAMESPACES = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'untl': 'http://digital2.library.unt.edu/untl/'
}


def parse_arguments():
    """Parses script arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--path", dest="path", help="Output data path")
    return parser.parse_args()


def get_oai_harvester(collection):
    """Returns an oai.Harvester for the given collection."""
    oai_url = f"{SERVER_URL}/explore/collections/{collection}/oai"
    return oai.Harvester(oai_url, OPTIONS, NAMESPACES)


def parse_ark_id(ark_id):
    """Returns the parts of an ark identifier."""
    _, ark_naan, ark_name = ark_id.split('/')
    return ark_naan, ark_name


def harvest_pdf(ark_naan, ark_name):
    """Tries to fetch the PDF data for a given item."""
    pdf_url = f"{SERVER_URL}/ark:/{ark_naan}/{ark_name}/{PDF_QUALIFIER}"
    with urllib.request.urlopen(pdf_url) as request:    
        return request.read()


def save_xml_file(path_to_file, xml_doc):
    """Saves the given XML file to disk."""
    with open(path_to_file, 'wb') as file:
        file.write(xml_doc.tostring(
            encoding='utf-8',
            xml_declaration=True
        ))


def harvest_all_from_collection(collection, num, path, sleep=0.5):
    """Harvest 'num' metadata/fulltext items from a UNTDL collection."""
    items = []
    harvester = get_oai_harvester(collection)
    print(f'Getting all IDs for collection {collection}.')
    ark_ids = set(harvester.get_ids())
    while ark_ids and len(items) < num:
        print()
        print(f'Item {len(items) + 1} of {num}.')
        ark_id = random.choice(list(ark_ids))
        ark_ids = ark_ids - {'ark_id'}
        ark_naan, ark_name = parse_ark_id(ark_id)
        try:
            print(f'Trying to get PDF for item {ark_id}.')
            pdf_bytes = harvest_pdf(ark_naan, ark_name)
        except urllib.request.HTTPError as e:
            print(f'Error! (HTTP {e.code}) Trying another item.')
            continue
        try:
            print(f'Extracting text for item {ark_id}.')
            parsed_doc_text = pdf.extract_text_as_xml_from_bytes(pdf_bytes)
        except Exception:
            print('Error! Trying another item.')
            continue
        try:
            print(f'Getting metadata record for item {ark_id}.')
            md_record = harvester.get_record(ark_id)
        except Exception:
            print('Error! Trying another item.')
            continue
        print('Saving files.')
        path_to_doc_text = f'{path}/{ark_name}-fulltext.xml'
        save_xml_file(path_to_doc_text, parsed_doc_text)
        path_to_metadata = f'{path}/{ark_name}-metadata.xml'
        save_xml_file(path_to_metadata, md_record)
        items.append(ark_id)
        time.sleep(sleep)
    return items


if __name__ == "__main__":
    args = parse_arguments()
    for collection, num_needed in COLLECTIONS.items():
        items = harvest_all_from_collection(collection, num, args['path'])
