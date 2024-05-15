import pyarrow.parquet as pq
import pyarrow.flight as flight
import pyarrow as pa
import s3fs
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FlightServer(flight.FlightServerBase):
    def __init__(self, host="0.0.0.0", port=8815, s3_uri=None, aws_access_key_id=None, aws_secret_access_key=None):
        location = f"grpc://{host}:{port}"
        super().__init__(location=location)
        self.location = location
        self.s3_uri = s3_uri
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def _parse_s3_uri(self, s3_uri):
        _, _, bucket, key = s3_uri.split("/", 3)
        return bucket, key

    def list_flights(self, context, criteria):
        bucket, key = self._parse_s3_uri(self.s3_uri)
        descriptor = flight.FlightDescriptor.for_path(key.encode())
        endpoint = flight.FlightEndpoint(flight.Ticket(self.s3_uri.encode()), [self.location])
        fs = s3fs.S3FileSystem(anon=False, key=self.aws_access_key_id, secret=self.aws_secret_access_key)
        schema = pq.read_schema(self.s3_uri, filesystem=fs)
        return [flight.FlightInfo(schema, descriptor, [endpoint], -1, -1)]

    def get_flight_info(self, context, descriptor):
        bucket, key = self._parse_s3_uri(self.s3_uri)
        fs = s3fs.S3FileSystem(anon=False, key=self.aws_access_key_id, secret=self.aws_secret_access_key)
        try:
            parquet_file = pq.ParquetFile(self.s3_uri, filesystem=fs)
            content_length = parquet_file.metadata.num_rows
            schema = parquet_file.schema_arrow
        except Exception as e:
            logger.error(f"Error occurred while accessing S3 object: {str(e)}")
            raise flight.FlightUnavailableError("Failed to retrieve flight info")

        endpoint = flight.FlightEndpoint(flight.Ticket(self.s3_uri.encode()), [self.location])
        return flight.FlightInfo(schema, descriptor, [endpoint], content_length, content_length)

    def do_get(self, context, ticket):
        logger.info("Entering do_get method")
        try:
            fs = s3fs.S3FileSystem(anon=False, key=self.aws_access_key_id, secret=self.aws_secret_access_key)
            logger.info(f"Created S3FileSystem with access key: {self.aws_access_key_id}")
            logger.info(f"Reading Parquet file from S3 URI: {self.s3_uri}")
            parquet_file = pq.ParquetFile(self.s3_uri, filesystem=fs)
            schema = parquet_file.schema_arrow

            # Specify the desired batch size and chunk size
            batch_size = 50000  
            chunk_size = 1024 * 1024  

            # Enable Arrow IPC compression for data transfer
            options = pa.ipc.IpcWriteOptions(compression='zstd')

            # Use Arrow's streaming functionality to process data in chunks
            def batch_iterator():
                writer = None
                for batch in parquet_file.iter_batches(batch_size=batch_size):
                    if writer is None:
                        # Initialize the RecordBatchStreamWriter on the first batch
                        sink = pa.BufferOutputStream()
                        writer = pa.ipc.new_stream(sink, batch.schema)
                    writer.write_batch(batch)
                    logger.info(f"Yielding batch with {batch.num_rows} rows")
                    yield batch

                if writer is not None:
                    writer.close()

            logger.info("Returning GeneratorStream")
            return flight.GeneratorStream(schema, batch_iterator())

        except Exception as e:
            logger.error(f"Error occurred while retrieving data: {str(e)}")
            raise flight.FlightUnavailableError("Failed to retrieve data")

        finally:
            logger.info("Exiting do_get method")

def serve(host="0.0.0.0", port=8815, s3_uri=None, aws_access_key_id=None, aws_secret_access_key=None):
    server = FlightServer(host, port, s3_uri, aws_access_key_id, aws_secret_access_key)
    server.serve()

if __name__ == "__main__":
    s3_uri = "s3://pathto.parquet"
    aws_access_key_id = ""
    aws_secret_access_key = ""
    serve(s3_uri=s3_uri, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)