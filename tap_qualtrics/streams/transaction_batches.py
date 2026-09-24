from tap_qualtrics.streams.abstracts import DirectoryChildStream


class TransactionBatches(DirectoryChildStream):
    tap_stream_id = "transaction_batches"
    key_properties = ["batchId"]
    replication_method = "FULL_TABLE"
    data_key = "result.elements"
    path = "directories/{directory_id}/transactionbatches"
    page_size = 200
    parent = "directories"
