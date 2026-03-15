from node import serve

serve(
    node_id="node2",
    host="localhost",
    port=50052,
    peers=["localhost:50051", "localhost:50052", "localhost:50053"],
)