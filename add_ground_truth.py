from neo4j import GraphDatabase
import random
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection details
uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
user = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "password")

class GroundTruthAdder:
    def __init__(self):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def add_ground_truth(self):
        with self.driver.session() as session:
            # 1. Mark some high fraud score accounts as known_fraud
            high_fraud_accounts = session.run("""
                MATCH (a:Account) 
                WHERE a.fraud_score >= 0.7
                RETURN a.id as id
                LIMIT 50
            """).data()
            
            print(f"Found {len(high_fraud_accounts)} high score accounts to mark as fraudulent")
            
            if high_fraud_accounts:
                # Mark these accounts as known fraud
                for account in high_fraud_accounts:
                    session.run("""
                        MATCH (a:Account {id: $id})
                        SET a.known_fraud = true
                        SET a.is_fraud = 1
                    """, id=account['id'])
                
                print(f"Marked {len(high_fraud_accounts)} accounts as fraudulent")
            
            # 2. Mark transactions involving these accounts as is_fraud
            transactions_marked = session.run("""
                MATCH (a:Account)-[r:SENT]->(b:Account)
                WHERE a.known_fraud = true OR b.known_fraud = true
                SET r.is_fraud = 1
                RETURN count(r) as count
            """).single()
            
            if transactions_marked:
                print(f"Marked {transactions_marked['count']} transactions as fraudulent")
            else:
                print("No transactions were marked as fraudulent")
    
    def verify_ground_truth(self):
        with self.driver.session() as session:
            # Verify accounts marked as fraudulent
            fraud_accounts = session.run("""
                MATCH (a:Account)
                WHERE a.known_fraud = true OR a.is_fraud = 1
                RETURN count(a) as count
            """).single()
            
            # Verify transactions marked as fraudulent
            fraud_transactions = session.run("""
                MATCH ()-[r:SENT]->()
                WHERE r.is_fraud = 1
                RETURN count(r) as count
            """).single()
            
            print(f"Verification results:")
            print(f"- Fraudulent accounts: {fraud_accounts['count'] if fraud_accounts else 0}")
            print(f"- Fraudulent transactions: {fraud_transactions['count'] if fraud_transactions else 0}")

if __name__ == "__main__":
    adder = GroundTruthAdder()
    try:
        print("Adding ground truth data to the database...")
        adder.add_ground_truth()
        adder.verify_ground_truth()
        print("Ground truth data added successfully")
    finally:
        adder.close()
