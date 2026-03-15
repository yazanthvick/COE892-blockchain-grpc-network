from node import serve

serve(
    node_id="node3",
    host="localhost",
    port=50053,
    peers=["localhost:50051", "localhost:50052", "localhost:50053"],
)