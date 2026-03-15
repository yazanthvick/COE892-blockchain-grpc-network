from node import serve

serve(
    node_id="node1",
    host="localhost",
    port=50051,
    peers=["localhost:50051", "localhost:50052", "localhost:50053"],
)