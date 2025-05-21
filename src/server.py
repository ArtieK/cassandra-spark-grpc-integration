import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
import os
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        # Step 1.1: Connect to Cassandra cluster
        project = os.environ.get('PROJECT', 'p6')
        cluster = Cluster([f'{project}-db-1', f'{project}-db-2', f'{project}-db-3'])
        self.session = cluster.connect()

        # Step 1.2: Create Cassandra schema
        # Drop and recreate keyspace
        self.session.execute("DROP KEYSPACE IF EXISTS weather")
        self.session.execute("""
            CREATE KEYSPACE weather
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
        """)

        # Create custom type
        self.session.execute("""
            CREATE TYPE weather.station_record (
                tmin INT,
                tmax INT
            )
        """)

        # Create table
        self.session.execute("""
            CREATE TABLE weather.stations (
                id TEXT,
                date DATE,
                name TEXT STATIC,
                record station_record,
                PRIMARY KEY (id, date)
            ) WITH CLUSTERING ORDER BY (date ASC)
        """)

        # Step 1.4: Create Spark session
        self.spark = SparkSession.builder.appName("p6").getOrCreate()

        # Step 2.1: Parse NOAA station file with Spark
        df = self.spark.read.text("ghcnd-stations.txt")

        # Extract fields using SUBSTRING (1-indexed in SQL)
        df = df.selectExpr(
            "SUBSTRING(value, 1, 11) AS id",
            "SUBSTRING(value, 39, 2) AS state",
            "SUBSTRING(value, 42, 30) AS name"
        )

        # Filter for Wisconsin stations
        wi_stations = df.filter(df.state == "WI").collect()

        # Step 2.2: Insert station data into Cassandra
        for row in wi_stations:
            self.session.execute(
                "INSERT INTO weather.stations (id, name) VALUES (%s, %s)",
                (row.id.strip(), row.name.strip())
            )

        # ============ Server Stated Successfully =============
        print("Server started") # Don't delete this line!


    def StationSchema(self, request, context):
        try:
            result = self.session.execute("DESCRIBE TABLE weather.stations")
            schema = result.one().create_statement
            return station_pb2.StationSchemaReply(schema=schema, error="")
        except Exception as e:
            return station_pb2.StationSchemaReply(schema="", error=str(e))


    def StationName(self, request, context):
        try:
            result = self.session.execute(
                "SELECT name FROM weather.stations WHERE id = %s",
                (request.station,)
            )
            row = result.one()
            if row and row.name:
                return station_pb2.StationNameReply(name=row.name, error="")
            else:
                return station_pb2.StationNameReply(name="", error="Station not found")
        except Exception as e:
            return station_pb2.StationNameReply(name="", error=str(e))


    def RecordTemps(self, request, context):
        return station_pb2.RecordTempsReply(error="TODO")


    def StationMax(self, request, context):
        return station_pb2.StationMaxReply(tmax=-1, error="TODO")


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=9),
        options=[("grpc.so_reuseport", 0)],
    )
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port('0.0.0.0:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()

