from neo4j import GraphDatabase
import os
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class FraudChecker:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    def close(self):
        self.driver.close()
    
    def check_fraud_data(self):
        with self.driver.session() as session:
            # Kiểm tra các giao dịch có nhãn is_fraud
            fraud_txn = session.run("""
                MATCH ()-[r:SENT]->()
                WHERE r.is_fraud = 1
                RETURN count(r) as fraud_count
            """).single()
            
            if fraud_txn:
                print(f"Số giao dịch đã được đánh dấu gian lận (r.is_fraud = 1): {fraud_txn['fraud_count']}")
            else:
                print("Không tìm thấy giao dịch nào được đánh dấu gian lận")
            
            # Kiểm tra các giao dịch từ tài khoản có fraud_score cao
            high_score_txn = session.run("""
                MATCH (a:Account)-[r:SENT]->()
                WHERE a.fraud_score >= 0.7
                RETURN count(r) as count
            """).single()
            
            if high_score_txn:
                print(f"Số giao dịch từ tài khoản có fraud_score >= 0.7: {high_score_txn['count']}")
            else:
                print("Không tìm thấy giao dịch nào từ tài khoản có fraud_score cao")
            
            # Kiểm tra các tài khoản có fraud_score cao có tham gia giao dịch không
            high_score_accounts = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score >= 0.7
                RETURN count(a) as total_accounts
            """).single()
            
            high_score_active = session.run("""
                MATCH (a:Account)-[r:SENT]-() 
                WHERE a.fraud_score >= 0.7
                RETURN count(DISTINCT a) as active_accounts
            """).single()
            
            print(f"Tổng số tài khoản có fraud_score >= 0.7: {high_score_accounts['total_accounts'] if high_score_accounts else 0}")
            print(f"Số tài khoản có fraud_score >= 0.7 tham gia giao dịch: {high_score_active['active_accounts'] if high_score_active else 0}")

if __name__ == "__main__":
    checker = FraudChecker()
    try:
        print("Kiểm tra dữ liệu gian lận trong database...")
        checker.check_fraud_data()
    finally:
        checker.close()
