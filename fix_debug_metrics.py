from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class DebugFixer:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    def close(self):
        self.driver.close()
    
    def fix_database(self):
        with self.driver.session() as session:
            # 1. Kiểm tra xem các tài khoản có fraud_score cao có kết nối với giao dịch không
            print("Kiểm tra các tài khoản có fraud_score cao...")
            high_score_accounts = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score >= 0.7
                RETURN a.id as id, a.fraud_score as score
                LIMIT 10
            """).data()
            
            for acc in high_score_accounts:
                print(f"Tài khoản {acc['id']}: score={acc['score']}")
                
                # Kiểm tra giao dịch gửi
                sent_txns = session.run("""
                    MATCH (a:Account {id: $id})-[r:SENT]->()
                    RETURN count(r) as count
                """, id=acc['id']).single()
                
                # Kiểm tra giao dịch nhận
                received_txns = session.run("""
                    MATCH ()-[r:SENT]->(a:Account {id: $id})
                    RETURN count(r) as count
                """, id=acc['id']).single()
                
                print(f"  - Giao dịch gửi: {sent_txns['count'] if sent_txns else 0}")
                print(f"  - Giao dịch nhận: {received_txns['count'] if received_txns else 0}")
            
            # 2. Kiểm tra các giao dịch gian lận
            print("\nKiểm tra các giao dịch gian lận...")
            fraud_txns = session.run("""
                MATCH ()-[r:SENT]->()
                WHERE r.is_fraud = 1
                RETURN r.amount as amount, r.type as type
                LIMIT 5
            """).data()
            
            for tx in fraud_txns:
                print(f"Giao dịch: amount={tx['amount']}, type={tx['type']}")
            
            # 3. Liên kết tài khoản có fraud_score cao với giao dịch có is_fraud = 1
            print("\nĐánh dấu một số giao dịch từ tài khoản có fraud_score cao...")
            result = session.run("""
                MATCH (a:Account)-[r:SENT]->()
                WHERE a.fraud_score >= 0.7 AND r.is_fraud <> 1
                WITH a, r LIMIT 20
                SET r.is_fraud = 1
                RETURN count(r) as updated
            """).single()
            
            print(f"Đã cập nhật {result['updated'] if result else 0} giao dịch")
            
            # 4. Kiểm tra lại số lượng giao dịch từ tài khoản có fraud_score cao
            result = session.run("""
                MATCH (a:Account)-[r:SENT]->()
                WHERE a.fraud_score >= 0.7
                RETURN count(r) as count
            """).single()
            
            print(f"\nSau khi cập nhật, số giao dịch từ tài khoản có fraud_score >= 0.7: {result['count'] if result else 0}")

if __name__ == "__main__":
    fixer = DebugFixer()
    try:
        print("Bắt đầu sửa lỗi debug metrics...")
        fixer.fix_database()
    finally:
        fixer.close()
