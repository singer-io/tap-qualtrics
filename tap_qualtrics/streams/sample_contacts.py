from typing import ClassVar

from tap_qualtrics.streams.abstracts import SampleChildStream


class SampleContacts(SampleChildStream):
    tap_stream_id = "sample_contacts"
    key_properties: ClassVar = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/samples/{sample_id}/contacts"
    page_size = 100
    parent = "samples"
