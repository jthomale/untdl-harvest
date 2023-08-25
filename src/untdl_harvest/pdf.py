"""Contains classes and functions for extracting and parsing PDF text."""
from io import BytesIO
import xml.etree.ElementTree as ET

import pypdf

from untdl_harvest.oai import ETreeXmlDoc


def make_pdf_reader_from_bytes(pdf_bytes):
    """Initialize a pypdf.PdfReader from PFD bytes."""
    return pypdf.PdfReader(BytesIO(pdf_bytes))


def extract_text_as_xml(reader):
    """Extract text as basic XML from the given PdfReader object."""
    root = ET.Element('document')
    root.text = '\n'
    for pdf_page in reader.pages:
        text_page = ET.SubElement(root, 'page')
        text_page.text = f'\n{pdf_page.extract_text()}\n'
        text_page.tail = '\n'
    return ETreeXmlDoc(root)


def extract_text_as_xml_from_bytes(pdf_bytes):
    """Extract text from PDF bytes, return as basic XML."""
    reader = make_pdf_reader_from_bytes(pdf_bytes)
    return extract_text_as_xml(reader)
