import urllib.parse
from singer import get_bookmark, metrics, write_bookmark, write_record
from tap_qualtrics.streams.abstracts import DirectoryChildStream


class OptedOutContacts(DirectoryChildStream):
    tap_stream_id = "opted_out_contacts"
    key_properties = ["contactId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/contacts/optedOutContacts"
    page_size = 100
    parent = "directories"

    def get_records(self, parent_id=None, bookmark=""):
        directory_id = (parent_id or {}).get("directoryId") or parent_id
        if not directory_id:
            return
        path = self.path.format(directory_id=directory_id)
        params = {"pageSize": self.page_size}
        if bookmark:
            params["since"] = urllib.parse.quote(bookmark)
        yield from self._paginate(path, params)

    def sync(self, state, transformer, parent_id=None):
        bookmark = get_bookmark(
            state, self.tap_stream_id, self.replication_keys[0], self.client.start_date
        )
        max_bk = bookmark
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(parent_id, bookmark):
                transformed = transformer.transform(record, self.schema, self.mdata)
                record_bk = transformed.get(self.replication_keys[0], "")
                if record_bk >= bookmark:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed)
                        counter.increment()
                    if record_bk and record_bk > max_bk:
                        max_bk = record_bk
        state = write_bookmark(state, self.tap_stream_id, self.replication_keys[0], max_bk)
        return counter.value

