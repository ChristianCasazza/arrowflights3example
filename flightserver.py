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
            content_length = pq.read_metadata(self.s3_uri, filesystem=fs).num_rows
            schema = pq.read_schema(self.s3_uri, filesystem=fs)
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
            
            logger.info("Returning GeneratorStream")
            return flight.GeneratorStream(parquet_file.schema_arrow, parquet_file.iter_batches())
        except Exception as e:
            logger.error(f"Error occurred while retrieving data: {str(e)}")
            raise flight.FlightUnavailableError("Failed to retrieve data")
        finally:
            logger.info("Exiting do_get method")

def serve(host="0.0.0.0", port=8815, s3_uri=None, aws_access_key_id=None, aws_secret_access_key=None):
    server = FlightServer(host, port, s3_uri, aws_access_key_id, aws_secret_access_key)
    server.serve()

if __name__ == "__main__":
    s3_uri = "s3:/path/to/parquet/file"
    aws_access_key_id = "YOUR_ACCESS_KEY"
    aws_secret_access_key = "YOUR_SECRET_KEY"
    serve(s3_uri=s3_uri, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)