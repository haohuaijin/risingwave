import os
import argparse
import json
import grpc
import connector_service_pb2_grpc
import connector_service_pb2
import psycopg2


def make_mock_schema():
    # todo
    schema = connector_service_pb2.TableSchema(
        columns=[
            connector_service_pb2.TableSchema.Column(name="id", data_type=2),
            connector_service_pb2.TableSchema.Column(name="name", data_type=7)
        ],
        pk_indices=[0]
    )
    return schema


def load_input(input_file):
    with open(input_file, 'r') as file:
        sink_input = json.load(file)
    return sink_input


def test_upsert_sink(type, prop, input_file):
    sink_input = load_input(input_file)
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = connector_service_pb2_grpc.ConnectorServiceStub(channel)
        request_list = [
            connector_service_pb2.SinkStreamRequest(start=connector_service_pb2.SinkStreamRequest.StartSink(
                sink_config=connector_service_pb2.SinkConfig(
                    sink_type=type,
                    properties=prop,
                    table_schema=make_mock_schema()
                )
            ))]
        epoch = 0
        batch_id = 1
        for batch in sink_input:
            request_list.append(connector_service_pb2.SinkStreamRequest(
                start_epoch=connector_service_pb2.SinkStreamRequest.StartEpoch(epoch=epoch)))
            row_ops = []
            for row in batch:
                row_ops.append(connector_service_pb2.SinkStreamRequest.WriteBatch.JsonPayload.RowOp(
                    op_type=row['op_type'], line=str(row['line'])))
            request_list.append(connector_service_pb2.SinkStreamRequest(write=connector_service_pb2.SinkStreamRequest.WriteBatch(
                json_payload=connector_service_pb2.SinkStreamRequest.WriteBatch.JsonPayload(row_ops=row_ops),
                batch_id=batch_id,
                epoch=epoch
            )))
            request_list.append(connector_service_pb2.SinkStreamRequest(sync=connector_service_pb2.SinkStreamRequest.SyncBatch(epoch=epoch)))
            epoch += 1
            batch_id += 1

        response_iter = stub.SinkStream(iter(request_list))
        for req in request_list:
            try:
                print("REQUEST", req)
                print("RESPONSE OK:", next(response_iter))
            except Exception as e:
                print("Integration test failed: ", e)
                exit(1)

def test_sink(type, prop, input_file):
    sink_input = load_input(input_file)
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = connector_service_pb2_grpc.ConnectorServiceStub(channel)
        request_list = [
            connector_service_pb2.SinkStreamRequest(start=connector_service_pb2.SinkStreamRequest.StartSink(
                sink_config=connector_service_pb2.SinkConfig(
                    sink_type=type,
                    properties=prop,
                    table_schema=make_mock_schema()
                )
            ))]
        epoch = 0
        batch_id = 1
        for batch in sink_input:
            request_list.append(connector_service_pb2.SinkStreamRequest(
                start_epoch=connector_service_pb2.SinkStreamRequest.StartEpoch(epoch=epoch)))
            row_ops = []
            for row in batch:
                row_ops.append(connector_service_pb2.SinkStreamRequest.WriteBatch.JsonPayload.RowOp(
                    op_type=1, line=str(row)))
            request_list.append(connector_service_pb2.SinkStreamRequest(write=connector_service_pb2.SinkStreamRequest.WriteBatch(
                json_payload=connector_service_pb2.SinkStreamRequest.WriteBatch.JsonPayload(row_ops=row_ops),
                batch_id=batch_id,
                epoch=epoch
            )))
            request_list.append(connector_service_pb2.SinkStreamRequest(sync=connector_service_pb2.SinkStreamRequest.SyncBatch(epoch=epoch)))
            epoch += 1
            batch_id += 1

        response_iter = stub.SinkStream(iter(request_list))
        for req in request_list:
            try:
                print("REQUEST", req)
                print("RESPONSE OK:", next(response_iter))
            except Exception as e:
                print("Integration test failed: ", e)
                exit(1)


def test_file_sink(input_file):
    test_sink("file", {"output.path": "/tmp/connector",}, input_file)


def test_jdbc_sink(input_file):
    test_sink("jdbc",
              {"jdbc.url": "jdbc:postgresql://localhost:5432/test?user=test&password=connector",
               "table.name": "test"},
              input_file)

    # validate results
    validate_jdbc_sink(input_file)


def validate_jdbc_sink(input_file):
    conn = psycopg2.connect("dbname=test user=test password=connector host=localhost port=5432")
    cur = conn.cursor()
    cur.execute("SELECT * FROM test")
    rows = cur.fetchall()
    expected = [list(row.values()) for batch in load_input(input_file) for row in batch]
    if len(rows) != len(expected):
        print("Integration test failed: expected {} rows, but got {}".format(len(expected), len(rows)))
        exit(1)
    for i in range(len(rows)):
        if len(rows[i]) != len(expected[i]):
            print("Integration test failed: expected {} columns, but got {}".format(len(expected[i]), len(rows[i])))
            exit(1)
        for j in range(len(rows[i])):
            if rows[i][j] != expected[i][j]:
                print(
                    "Integration test failed: expected {} at row {}, column {}, but got {}".format(expected[i][j], i, j,
                                                                                                   rows[i][j]))
                exit(1)


def test_print_sink(input_file):
    test_sink("print", {}, input_file)


def test_iceberg_sink(input_file):
    test_sink("iceberg",
              {"sink.mode":"append-only",
               "location.type":"minio",
               "warehouse.path":"minio://minioadmin:minioadmin@127.0.0.1:9000/bucket",
               "database.name":"demo_db",
               "table.name":"demo_table"},
              input_file)

def test_upsert_iceberg_sink(input_file):
    test_upsert_sink("iceberg",
              {"sink.mode":"upsert",
               "location.type":"minio",
               "warehouse.path":"minio://minioadmin:minioadmin@127.0.0.1:9000/bucket",
               "database.name":"demo_db",
               "table.name":"demo_table"},
              input_file)

def test_deltalake_sink(input_file):
    test_sink("deltalake",
              {"location":"minio://minioadmin:minioadmin@127.0.0.1:9000/bucket/delta",
               "location.type":"minio",
               "storage_options.s3a_endpoint":"http://127.0.0.1:9000",
               "storage_options.s3a_access_key":"minioadmin",
               "storage_options.s3a_secret_key":"minioadmin"},
              input_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--file_sink', action='store_true', help="run file sink test")
    parser.add_argument('--jdbc_sink', action='store_true', help="run jdbc sink test")
    parser.add_argument('--print_sink', action='store_true', help="run print sink test")
    parser.add_argument('--iceberg_sink', action='store_true', help="run iceberg sink test")
    parser.add_argument('--upsert_iceberg_sink', action='store_true', help="run upsert iceberg sink test")
    parser.add_argument('--deltalake_sink', action='store_true', help="run deltalake sink test")
    parser.add_argument('--input_file', default="./data/sink_input.json", help="input data to run tests")
    args = parser.parse_args()
    if args.file_sink:
        test_file_sink(args.input_file)
    if args.jdbc_sink:
        test_jdbc_sink(args.input_file)
    if args.print_sink:
        test_print_sink(args.input_file)
    if args.iceberg_sink:
        test_iceberg_sink(args.input_file)
    if args.upsert_iceberg_sink:
        test_upsert_iceberg_sink(args.input_file)
    if args.deltalake_sink:
        test_deltalake_sink(args.input_file)
