"""Contains classes and functions for interacting with OAI endpoints."""
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
import zlib


def make_querystring(verb, arguments):
    """Makes a querystring for an OAI request from a verb + arguments."""
    pairs = [('verb', verb)] + [(k, v) for k, v in arguments.items()]
    qstring = '&'.join(f'{k}={v}' for k, v in pairs if v is not None)
    return f'?{qstring}'


class EndpointError(Exception):
    """An endpoint error that prevents data from being returned."""
    pass


class OAIError(Exception):
    """An OAI-specific error returned by the OAI server."""
    
    def __init__(self, oai_code, oai_message):
        self.code = oai_code
        self.message = oai_message
        super().__init__(
            f"code={oai_code} '{oai_message}'"
        )


class ETreeXmlDoc:
    """Simple wrapper around xml.etree.ElementTree."""

    def __init__(self, etree_node, namespaces):
        """Initializes an ETreeXmlDoc object."""
        self.root = etree_node
        self.namespaces = namespaces
        for prefix, namespace in namespaces.items():
            ET.register_namespace(prefix, namespace)

    @classmethod
    def fromstring(cls, xml_str, namespaces):
        """Creates a new ETreeXmlDoc from an XML string."""
        root_node = ET.fromstring(xml_str)
        return cls(root_node, namespaces)

    def tostring(self, encoding='us-ascii', method='xml', xml_declaration=None,
                 default_namespace=None, short_empty_elements=True):
        """Returns the current XML document as a string."""
        return ET.tostring(
            self.root, encoding=encoding, method=method,
            xml_declaration=xml_declaration,
            default_namespace=default_namespace,
            short_empty_elements=short_empty_elements
        )

    def __repr__(self):
        return f"<{type(self).__name__} Root Element '{self.root.tag}'>"

    def expand_tagname(self, tagname):
        """Expands a tagname that uses a namespace prefix."""
        if tagname is not None and ':' in tagname:
            prefix, field = tagname.split(':')
            try:
                return f"{{{self.namespaces[prefix]}}}{field}"
            except KeyError:
                raise ValueError(f"Unknown namespace prefix in '{tagname}'")
        return tagname

    def find_tag(self, tag, text=None):
        """Finds the first matching element by tag."""
        for el in self.root.iter(self.expand_tagname(tag)):
            if text is None or text == el.text:
                return type(self)(el, self.namespaces)
        return None

    def find_path(self, path, text=None):
        """Finds the first matching element by xpath."""
        for el in self.root.iterfind(path):
            if text is None or text == el.text:
                return type(self)(el, self.namespaces)
        return None

    def find_text(self, text):
        """Finds the first matching element by text."""
        return self.find_tag(None, text=text)

    def findall_tag(self, tag, text=None):
        """Returns a generator yielding all matching elements by tag."""
        for el in self.root.iter(self.expand_tagname(tag)):
            if text is None or text == el.text:
                yield type(self)(el, self.namespaces)

    def findall_path(self, path, text=None):
        """Returns a generator yielding all matching elements by xpath."""
        for el in self.root.iterfind(path):
            if text is None or text == el.text:
                yield type(self)(el, self.namespaces)
    
    def findall_text(self, text, tag=None):
        """Returns a generator yielding all matching elements by text."""
        return self.findall_tag(None, text=text)


class Endpoint:
    """Class for interacting with an OAI endpoint."""

    def __init__(self, url, namespaces=None, verbose=True, sleep_time=0,
                 max_recoveries=3):
        """Initializes an Endpoint object."""
        if hasattr(url, 'url'):
            self = url
        else:
            self.url = url
            self.verbose = verbose
            self.sleep_time = sleep_time
            self.max_recoveries = max_recoveries
            self.num_recoveries = 0
            self.raw_bytes = 0
            self.data_bytes = 0
            self.default_recovery_time = 60
            self.http_error_class = urllib.request.HTTPError
            self.http_errors = []
            self.oai_error = ''
            self.headers = {
                'User-Agent': 'untdl_harvest',
                'Accept': 'text/html',
                'Accept-Encoding': 'compress, deflate'
            }
            self.namespaces = namespaces or {}
            if 'oai' not in self.namespaces:
                self.namespaces['oai'] = 'http://www.openarchives.org/OAI/2.0/'
            self.xml_doc_class = ETreeXmlDoc
            self.last_page = None

    def _bail(self, why_bail=None):
        http_errstr = ', '.join([e.code for e in self.http_errors])
        deets = ''.join([
            f" {why_bail}" if why_bail else "",
            f" HTTP Errors: {http_errstr}" if http_errstr else "",
            f" OAI Error: {self.oai_error}" if self.oai_error else ""
        ])
        raise EndpointError(
            f"Encountered fatal errors while contacting the OAI server.{deets}"
        )

    def _send_request(self, req_url):
        req = urllib.request.Request(req_url, headers=self.headers)
        with urllib.request.urlopen(req) as response:
            return response.read()

    def _handle_http_error(self, error, verb, arguments):
        if self.verbose:
            print(f'Error: {error}')
        self.http_errors.append(error)
        if error.code == 503:
            retry_wait = int(error.hdrs.get('Retry-After', '-1'))
        else:
            if self.num_recoveries >= self.max_recoveries:
                self._bail('Exceeded max number of recovery attempts.')
            self.max_recoveries += 1
            retry_wait = self.default_recovery_time
        if retry_wait < 0:
            self._bail('Retries are disabled.')
        if self.verbose:
            print(f'Retrying in {retry_wait} seconds.')
        return self.get_page(verb, arguments, retry_wait)

    def _catch_and_handle_oai_error(self, data):
        oai_error = re.search('<error *code=\"([^"]*)">(.*)</error>', data)
        if oai_error:
            code = oai_error.group(1)
            msg = oai_error.group(2)
            self.oai_error = OAIError(code, msg)
            self._bail()
        return data

    def _decompress(self, data):
        try:
            return zlib.decompressobj().decompress(data)
        except zlib.error:
            pass
        return data

    def get_page(self, verb, arguments, sleep_time=None):
        """Gets one page of data from this endpoint."""
        sleep_time = self.sleep_time if sleep_time is None else sleep_time
        if sleep_time:
            time.sleep(sleep_time)
        req_url = ''.join([self.url, make_querystring(verb, arguments)])
        if self.verbose:
            print("\r", f"Endpoint.get_page ...'{req_url[-90:]}'")
        try:
            data = self._send_request(req_url)
        except self.http_error_class as error:
            data = self._handle_http_error(error, verb, arguments)
        self.raw_bytes += len(data)
        data = self._decompress(data).decode('utf-8')
        self.data_bytes += len(data)
        data = self._catch_and_handle_oai_error(data)
        self.last_page = self.xml_doc_class.fromstring(data, self.namespaces)
        return self.last_page

    def compile_data(self, verb, arguments, docfilter):
        """Gets and compiles all pages of data from this endpoint."""
        data = []
        while True:
            page = self.get_page(verb, arguments)
            data.extend(docfilter(page))
            rtoken = page.find_tag('oai:resumptionToken')
            if rtoken is None:
                break
            arguments = {'resumptionToken': rtoken.text}
        return data


def docfilter_ids(page):
    """Compiles only IDs from a page of records."""
    return [el.text for el in page.findall_tag('oai:identifier')]


def docfilter_records(page):
    """Compiles records from a page of records."""
    return list(page.findall_tag('oai:record'))


class Harvester:
    """Class for harvesting data from an Endpoint."""

    def __init__(self, endpoint, options, namespaces, verbose=True):
        """Initialize a Harvester object."""
        self.endpoint = Endpoint(endpoint, namespaces=namespaces)
        self.verbose = verbose
        self.options = {
            'metadataPrefix': options.get('metadataPrefix', 'oai_dc'),
            'from': options.get('from'),
            'until': options.get('until'),
            'set': options.get('set')
        }

    def get_ids(self, docfilter=docfilter_ids):
        """Gets a list of IDs available from a given OAI endpoint."""
        return self.endpoint.compile_data(
            'ListIdentifiers', self.options, docfilter
        )

    def list_records(self, docfilter=docfilter_records):
        """Gets a list of records from a given OAI endpoint."""
        command = f'ListRecords&{self.make_option_string()}'
        return self.endpoint.compile_data(
            'ListRecords', self.options, docfilter
        )

    def get_record(self, identifier):
        """Gets a single record from a given OAI endpoint."""
        arguments = {
            'metadataPrefix': self.options['metadataPrefix'],
            'identifier': identifier
        }
        page = self.endpoint.get_page('GetRecord', arguments)
        return page.find_tag('oai:record')
