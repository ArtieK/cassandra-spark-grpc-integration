import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc

class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        # TODO: create schema for weather data; 
        # TODO: load station data from ghcnd-stations.txt; 

        # ============ Server Stated Successfully =============
        print("Server started") # Don't delete this line!


    def StationSchema(self, request, context):
        return station_pb2.StationSchemaReply(schema="", error="TODO")


    def StationName(self, request, context):
        return station_pb2.StationNameReply(name="", error="TODO")


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

