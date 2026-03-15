import json
import time
import uuid
import grpc
import hashlib
import threading
from concurrent import futures
from typing import List, Optional

import blockchain_pb2
import blockchain_pb2_grpc


# =========================
# Core Data Classes
# =========================

class Transaction:
    def __init__(
        self,
        sender: str,
        receiver: str,
        amount: float,
        transaction_id: Optional[str] = None,
        timestamp: Optional[int] = None,
    ):
        self.transaction_id = transaction_id or str(uuid.uuid4())
        self.sender = sender
        self.receiver = receiver
        self.amount = amount
        self.timestamp = timestamp or int(time.time())

    def to_dict(self) -> dict:
        return {
            "transaction_id": self.transaction_id,
            "sender": self.sender,
            "receiver": self.receiver,
            "amount": self.amount,
            "timestamp": self.timestamp,
        }

    def to_proto(self) -> blockchain_pb2.Transaction:
        return blockchain_pb2.Transaction(
            transaction_id=self.transaction_id,
            sender=self.sender,
            receiver=self.receiver,
            amount=self.amount,
            timestamp=self.timestamp,
        )

    @staticmethod
    def from_proto(tx_msg: blockchain_pb2.Transaction) -> "Transaction":
        return Transaction(
            sender=tx_msg.sender,
            receiver=tx_msg.receiver,
            amount=tx_msg.amount,
            transaction_id=tx_msg.transaction_id,
            timestamp=tx_msg.timestamp,
        )


class Block:
    def __init__(
        self,
        index: int,
        transactions: List[Transaction],
        previous_hash: str,
        timestamp: Optional[int] = None,
        block_hash: Optional[str] = None,
    ):
        self.index = index
        self.timestamp = timestamp or int(time.time())
        self.transactions = transactions
        self.previous_hash = previous_hash
        self.hash = block_hash or self.compute_hash()

    def compute_hash(self) -> str:
        block_data = {
            "index": self.index,
            "timestamp": self.timestamp,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "previous_hash": self.previous_hash,
        }
        block_string = json.dumps(block_data, sort_keys=True)
        return hashlib.sha256(block_string.encode()).hexdigest()

    def to_proto(self) -> blockchain_pb2.Block:
        return blockchain_pb2.Block(
            index=self.index,
            timestamp=self.timestamp,
            transactions=[tx.to_proto() for tx in self.transactions],
            previous_hash=self.previous_hash,
            hash=self.hash,
        )

    @staticmethod
    def from_proto(block_msg: blockchain_pb2.Block) -> "Block":
        transactions = [Transaction.from_proto(tx) for tx in block_msg.transactions]
        return Block(
            index=block_msg.index,
            timestamp=block_msg.timestamp,
            transactions=transactions,
            previous_hash=block_msg.previous_hash,
            block_hash=block_msg.hash,
        )


class Blockchain:
    def __init__(self):
        self.chain: List[Block] = [self.create_genesis_block()]

    def create_genesis_block(self) -> Block:
        # Fixed genesis block so all nodes start the same way
        genesis = Block(
            index=0,
            transactions=[],
            previous_hash="0",
            timestamp=1,
        )
        genesis.hash = genesis.compute_hash()
        return genesis

    def latest_block(self) -> Block:
        return self.chain[-1]

    def add_block(self, block: Block) -> bool:
        latest = self.latest_block()

        if block.index != latest.index + 1:
            return False

        if block.previous_hash != latest.hash:
            return False

        if block.hash != block.compute_hash():
            return False

        self.chain.append(block)
        return True

    def is_valid_chain(self, chain: List[Block]) -> bool:
        if not chain:
            return False

        # Check genesis block matches expected genesis
        expected_genesis = self.create_genesis_block()
        actual_genesis = chain[0]
        if (
            actual_genesis.index != expected_genesis.index
            or actual_genesis.previous_hash != expected_genesis.previous_hash
            or actual_genesis.hash != expected_genesis.hash
        ):
            return False

        for i in range(1, len(chain)):
            curr = chain[i]
            prev = chain[i - 1]

            if curr.previous_hash != prev.hash:
                return False

            if curr.hash != curr.compute_hash():
                return False

        return True

    def replace_chain(self, new_chain: List[Block]) -> bool:
        if len(new_chain) > len(self.chain) and self.is_valid_chain(new_chain):
            self.chain = new_chain
            return True
        return False

    def get_balance(self, user: str) -> float:
        # Simple starter balance for demo purposes
        balance = 100.0
        for block in self.chain:
            for tx in block.transactions:
                if tx.sender == user:
                    balance -= tx.amount
                if tx.receiver == user:
                    balance += tx.amount
        return balance


# =========================
# Node Service
# =========================

class BlockchainNode(blockchain_pb2_grpc.BlockchainNodeServicer):
    def __init__(self, node_id: str, address: str, peers: List[str]):
        self.node_id = node_id
        self.address = address
        self.peers = [peer for peer in peers if peer != address]

        self.blockchain = Blockchain()
        self.pending_transactions: List[Transaction] = []
        self.seen_transaction_ids = set()
        self.lock = threading.Lock()

    # -------------------------
    # Validation
    # -------------------------

    def validate_transaction(self, tx: Transaction) -> tuple[bool, str]:
        if tx.amount <= 0:
            return False, "Amount must be positive."

        if tx.sender == tx.receiver:
            return False, "Sender and receiver cannot be the same."

        if tx.transaction_id in self.seen_transaction_ids:
            return False, "Duplicate transaction."

        if self.blockchain.get_balance(tx.sender) < tx.amount:
            return False, "Insufficient balance."

        return True, "Valid transaction."

    # -------------------------
    # gRPC RPC Methods
    # -------------------------

    def SubmitTransaction(self, request, context):
        tx = Transaction.from_proto(request.transaction)

        with self.lock:
            valid, message = self.validate_transaction(tx)
            if not valid:
                return blockchain_pb2.SubmitTransactionResponse(
                    accepted=False,
                    message=message,
                )

            self.pending_transactions.append(tx)
            self.seen_transaction_ids.add(tx.transaction_id)

        print(f"[{self.node_id}] Accepted transaction {tx.transaction_id}: "
              f"{tx.sender} -> {tx.receiver} ({tx.amount})")

        return blockchain_pb2.SubmitTransactionResponse(
            accepted=True,
            message="Transaction accepted.",
        )

    def PropagateBlock(self, request, context):
        incoming_block = Block.from_proto(request.block)

        with self.lock:
            if not self.blockchain.add_block(incoming_block):
                return blockchain_pb2.PropagateBlockResponse(
                    accepted=False,
                    message="Block rejected."
                )

            block_tx_ids = {tx.transaction_id for tx in incoming_block.transactions}
            self.pending_transactions = [
                tx for tx in self.pending_transactions
                if tx.transaction_id not in block_tx_ids
            ]

        print(f"[{self.node_id}] Accepted block #{incoming_block.index}")

        return blockchain_pb2.PropagateBlockResponse(
            accepted=True,
            message="Block accepted.",
        )

    def GetChain(self, request, context):
        with self.lock:
            proto_chain = [block.to_proto() for block in self.blockchain.chain]

        return blockchain_pb2.ChainResponse(
            chain=proto_chain,
            length=len(proto_chain),
        )

    def RegisterPeer(self, request, context):
        peer_address = request.address
        if peer_address and peer_address != self.address and peer_address not in self.peers:
            self.peers.append(peer_address)

        return blockchain_pb2.Ack(success=True, message="Peer registered.")

    def Ping(self, request, context):
        return blockchain_pb2.Ack(success=True, message=f"{self.node_id} is alive.")

    # -------------------------
    # Local Node Logic
    # -------------------------

    def broadcast_transaction(self, tx: Transaction):
        for peer in self.peers:
            try:
                channel = grpc.insecure_channel(peer)
                stub = blockchain_pb2_grpc.BlockchainNodeStub(channel)
                response = stub.SubmitTransaction(
                    blockchain_pb2.SubmitTransactionRequest(transaction=tx.to_proto()),
                    timeout=2,
                )
                print(f"[{self.node_id}] Sent tx to {peer}: {response.accepted}")
            except grpc.RpcError as e:
                print(f"[{self.node_id}] Failed to send tx to {peer}: {e.code().name}")

    def broadcast_block(self, block: Block):
        for peer in self.peers:
            try:
                channel = grpc.insecure_channel(peer)
                stub = blockchain_pb2_grpc.BlockchainNodeStub(channel)
                response = stub.PropagateBlock(
                    blockchain_pb2.PropagateBlockRequest(block=block.to_proto()),
                    timeout=2,
                )
                print(f"[{self.node_id}] Sent block #{block.index} to {peer}: {response.accepted}")
            except grpc.RpcError as e:
                print(f"[{self.node_id}] Failed to send block to {peer}: {e.code().name}")

    def sync_with_peers(self):
        longest_chain = None
        max_length = len(self.blockchain.chain)

        for peer in self.peers:
            try:
                channel = grpc.insecure_channel(peer)
                stub = blockchain_pb2_grpc.BlockchainNodeStub(channel)
                response = stub.GetChain(blockchain_pb2.GetChainRequest(), timeout=2)

                peer_chain = [Block.from_proto(b) for b in response.chain]
                if response.length > max_length and self.blockchain.is_valid_chain(peer_chain):
                    max_length = response.length
                    longest_chain = peer_chain
            except grpc.RpcError as e:
                print(f"[{self.node_id}] Failed to sync from {peer}: {e.code().name}")

        if longest_chain:
            with self.lock:
                replaced = self.blockchain.replace_chain(longest_chain)
            if replaced:
                print(f"[{self.node_id}] Replaced local chain with longer valid peer chain.")

    def create_block_from_pending(self) -> Optional[Block]:
        with self.lock:
            if not self.pending_transactions:
                return None

            latest = self.blockchain.latest_block()
            new_block = Block(
                index=latest.index + 1,
                transactions=self.pending_transactions.copy(),
                previous_hash=latest.hash,
            )

            if self.blockchain.add_block(new_block):
                self.pending_transactions.clear()
                print(f"[{self.node_id}] Created block #{new_block.index}")
                return new_block

        return None

    def submit_local_transaction(self, sender: str, receiver: str, amount: float):
        tx = Transaction(sender=sender, receiver=receiver, amount=amount)

        with self.lock:
            valid, message = self.validate_transaction(tx)
            if not valid:
                print(f"[{self.node_id}] Local transaction rejected: {message}")
                return

            self.pending_transactions.append(tx)
            self.seen_transaction_ids.add(tx.transaction_id)

        print(f"[{self.node_id}] Local transaction accepted: {sender} -> {receiver} ({amount})")
        self.broadcast_transaction(tx)

    def print_chain(self):
        with self.lock:
            print(f"\n[{self.node_id}] Current chain:")
            for block in self.blockchain.chain:
                print(f"  Block {block.index} | prev={block.previous_hash[:8]}... "
                      f"| hash={block.hash[:8]}... | txs={len(block.transactions)}")
            print()

    # -------------------------
    # Background Threads
    # -------------------------

    def mining_loop(self, interval: int = 15):
        while True:
            time.sleep(interval)
            block = self.create_block_from_pending()
            if block:
                self.broadcast_block(block)

    def sync_loop(self, interval: int = 10):
        while True:
            time.sleep(interval)
            self.sync_with_peers()


# =========================
# Server Startup
# =========================

def serve(node_id: str, host: str, port: int, peers: List[str]):
    address = f"{host}:{port}"
    node = BlockchainNode(node_id=node_id, address=address, peers=peers)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    blockchain_pb2_grpc.add_BlockchainNodeServicer_to_server(node, server)
    server.add_insecure_port(address)
    server.start()

    print(f"{node_id} running at {address}")
    print(f"Peers: {node.peers}")

    # Start background threads
    threading.Thread(target=node.mining_loop, daemon=True).start()
    threading.Thread(target=node.sync_loop, daemon=True).start()

    try:
        while True:
            print("\nOptions:")
            print("1. Submit transaction")
            print("2. Print chain")
            print("3. Sync now")
            print("4. Create block now")
            print("5. Exit")
            choice = input("Select: ").strip()

            if choice == "1":
                sender = input("Sender: ").strip()
                receiver = input("Receiver: ").strip()
                amount = float(input("Amount: ").strip())
                node.submit_local_transaction(sender, receiver, amount)

            elif choice == "2":
                node.print_chain()

            elif choice == "3":
                node.sync_with_peers()

            elif choice == "4":
                block = node.create_block_from_pending()
                if block:
                    node.broadcast_block(block)
                else:
                    print("No pending transactions.")

            elif choice == "5":
                break

            else:
                print("Invalid option.")

    except KeyboardInterrupt:
        pass
    finally:
        print(f"Stopping {node_id}...")
        server.stop(0)


if __name__ == "__main__":
    # Example defaults for local testing
    ALL_PEERS = [
        "localhost:50051",
        "localhost:50052",
        "localhost:50053",
    ]

    # Change these per node
    serve(
        node_id="node1",
        host="localhost",
        port=50051,
        peers=ALL_PEERS,
    )