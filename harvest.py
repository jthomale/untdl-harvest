"""Script for harvesting metadata / full text from UNT Digital Library."""
import random
import time
from untdl_harvest import oai
import urllib.request


SERVER_URL = "https://digital.library.unt.edu"
COLLECTIONS = {'UNTETD': 667, 'CRSR': 667, 'EOT': 666}
OPTIONS = {
    'metadataPrefix': 'untl_raw',
    'set': 'access_rights:public'
}
NAMESPACES = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'untl': 'http://digital2.library.unt.edu/untl/'
}


if __name__ == "__main__":
    for collection, num_needed in COLLECTIONS.items():
        items = []
        oai_url = f"{SERVER_URL}/explore/collections/{collection}/oai"
        harvester = oai.Harvester(oai_url, OPTIONS, NAMESPACES)
        oai_ids = set(harvester.get_ids())
        while oai_ids and len(items) < num_needed:
            id_ = random.choice(oai_ids)
            oai_ids = oai_ids - id_
            (_, id_pre, id_post) = id_.split('/')
            pdf_url = (
                f"https://digital.library.unt.edu/ark:/{id_pre}/{id_post}"
                f"/m2/1/high_res_d/"
            )
            try:
                # Get PDF data, something like:
                # pdf = urllib.request.Request(pdf_url)
            except urllib.request.HTTPError:
                # Skip this and try another if there is no PDF
                next
            untl_md = harvester.get_record(id_)
            with open(f"{path_to_metadata}/ark-{id_pre}-{id_post}") as md_file:
                md_file.write(untl_md.tostring(
                    encoding='utf-8',
                    xml_declaration=True
                ))

            # Do PDF text extraction here

            time.sleep(0.5)
