import hashlib
import json
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from pathlib import Path

class Block:
    def __init__(self, index: int, data: Dict[str, Any], previous_hash: str, timestamp: Optional[float] = None):
        self.index = index
        self.data = data
        self.previous_hash = previous_hash
        self.timestamp = timestamp or time.time()
        self.nonce = 0
        self.hash = self.calculate_hash()
    
    def calculate_hash(self) -> str:
        """Calculate SHA-256 hash of the block"""
        block_string = json.dumps({
            "index": self.index,
            "data": self.data,
            "previous_hash": self.previous_hash,
            "timestamp": self.timestamp,
            "nonce": self.nonce
        }, sort_keys=True)
        return hashlib.sha256(block_string.encode()).hexdigest()

    def mine_block(self, difficulty: int = 4):
        """Mine block with proof of work"""
        target = "0" * difficulty
        while self.hash[:difficulty] != target:
            self.nonce += 1
            self.hash = self.calculate_hash()
        print(f"Block mined: {self.hash}")
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "data": self.data,
            "previous_hash": self.previous_hash,
            "timestamp": self.timestamp,
            "nonce": self.nonce,
            "hash": self.hash
        }

class ETLBlockchain:
    def __init__(self):
        self.chain: List[Block] = []
        self.difficulty = 4
        self.create_genesis_block()
    
    def create_genesis_block(self):
        """Create the first block in the chain"""
        genesis_data = {
            "type": "genesis",
            "message": "ETL Data Integrity Blockchain Genesis Block",
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        }
        genesis_block = Block(0, genesis_data, "0")
        genesis_block.mine_block(self.difficulty)
        self.chain.append(genesis_block)
    
    def get_latest_block(self) -> Block:
        return self.chain[-1]
    
    def add_etl_data_block(self, s1_hash: str, s2_hash: str, dataset_hash: str, etl_metadata: Dict[str, Any] = None):
        """Add a new block containing ETL data hashes"""
        data = {
            "type": "etl_data",
            "extraction_hashes": {
                "s1.csv": s1_hash,
                "s2.csv": s2_hash
            },
            "deterministic_dataset_sha256": dataset_hash,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "etl_metadata": etl_metadata or {}
        }
        
        new_block = Block(
            index=len(self.chain),
            data=data,
            previous_hash=self.get_latest_block().hash
        )
        new_block.mine_block(self.difficulty)
        self.chain.append(new_block)
        return new_block
    
    def add_block_from_ledger(self, ledger_path: Path = Path("../ETL_Pipeline/output/ledger.json")):
        """Add block using data from the ETL ledger"""
        if not ledger_path.exists():
            raise FileNotFoundError(f"Ledger file not found: {ledger_path}")
        
        ledger_data = json.loads(ledger_path.read_text())
        latest_entry = ledger_data[-1]  # Get the most recent ETL run
        
        # Extract hashes from ledger
        extraction_hashes = latest_entry["extraction_hashes"]
        s1_hash = next(h["sha256"] for h in extraction_hashes if "s1.csv" in h["path"])
        s2_hash = next(h["sha256"] for h in extraction_hashes if "s2.csv" in h["path"])
        dataset_hash = latest_entry["output"]["deterministic_dataset_sha256"]
        
        # Add ETL metadata
        etl_metadata = {
            "input_paths": latest_entry["input_paths"],
            "etl_config": latest_entry["etl"],
            "output_stats": {
                "rows": latest_entry["output"]["rows"],
                "cols": latest_entry["output"]["cols"]
            },
            "etl_timestamp": latest_entry["timestamp_utc"]
        }
        
        return self.add_etl_data_block(s1_hash, s2_hash, dataset_hash, etl_metadata)
    
    def is_chain_valid(self) -> bool:
        """Validate the entire blockchain"""
        for i in range(1, len(self.chain)):
            current_block = self.chain[i]
            previous_block = self.chain[i - 1]
            
            # Check if current block's hash is valid
            if current_block.hash != current_block.calculate_hash():
                print(f"Invalid hash at block {i}")
                return False
            
            # Check if previous hash matches
            if current_block.previous_hash != previous_block.hash:
                print(f"Invalid previous hash at block {i}")
                return False
        
        return True
    
    def save_blockchain(self, path: Path = Path("output/blockchain.json")):
        """Save blockchain to JSON file"""
        path.parent.mkdir(parents=True, exist_ok=True)
        blockchain_data = {
            "blockchain": [block.to_dict() for block in self.chain],
            "metadata": {
                "total_blocks": len(self.chain),
                "difficulty": self.difficulty,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
        }
        path.write_text(json.dumps(blockchain_data, indent=2))
        print(f"Blockchain saved to: {path.resolve()}")
    
    def load_blockchain(self, path: Path = Path("output/blockchain.json")):
        """Load blockchain from JSON file"""
        if not path.exists():
            print(f"Blockchain file not found: {path}")
            return False
        
        data = json.loads(path.read_text())
        self.chain = []
        
        for block_data in data["blockchain"]:
            block = Block(
                index=block_data["index"],
                data=block_data["data"],
                previous_hash=block_data["previous_hash"],
                timestamp=block_data["timestamp"]
            )
            block.nonce = block_data["nonce"]
            block.hash = block_data["hash"]
            self.chain.append(block)
        
        print(f"Blockchain loaded from: {path.resolve()}")
        return True
    
    def display_chain(self):
        """Display the blockchain in a readable format"""
        print("\n" + "="*80)
        print("ETL DATA INTEGRITY BLOCKCHAIN")
        print("="*80)
        
        for block in self.chain:
            print(f"\nBlock #{block.index}")
            print(f"Hash: {block.hash}")
            print(f"Previous Hash: {block.previous_hash}")
            print(f"Timestamp: {datetime.fromtimestamp(block.timestamp, timezone.utc)}")
            print(f"Nonce: {block.nonce}")
            print("Data:")
            print(json.dumps(block.data, indent=2))
            print("-" * 80)

def main():
    """Example usage"""
    # Create blockchain
    blockchain = ETLBlockchain()
    
    # Add block from existing ledger
    try:
        block = blockchain.add_block_from_ledger()
        print(f"Added block #{block.index} with ETL data")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Creating sample block with manual data...")
        
        # Manual example with your actual hashes from the ledger
        blockchain.add_etl_data_block(
            s1_hash="992b48c00b68d472f28f1f906b8f80a066671c15221fce47b80ffa289302ef42",
            s2_hash="afddc09764f287f2eb27a194478a5d4b85a644dcf917bfd12955b852d85eeb42",
            dataset_hash="b1f1b1e234ce5ddc2fae879ddbd768a7a73c3c094516c06a679ecaef155cbf82",
            etl_metadata={"mode": "union", "files": ["s1.csv", "s2.csv"]}
        )
    
    # Validate and save blockchain
    print(f"\nBlockchain valid: {blockchain.is_chain_valid()}")
    blockchain.save_blockchain()
    blockchain.display_chain()

if __name__ == "__main__":
    main()