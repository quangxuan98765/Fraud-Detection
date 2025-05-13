from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class QueryFixer:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    def close(self):
        self.driver.close()
    
    def fix_queries(self):
        with self.driver.session() as session:
            # Kiểm tra các query với cả hai hướng - tài khoản là nguồn hoặc đích
            print("Kiểm tra với query mới tính cả tài khoản nguồn và đích...")
            
            # Query 1: Số giao dịch từ hoặc đến tài khoản có fraud_score cao
            high_score_txn = session.run("""
                MATCH (a:Account)-[r:SENT]->(b:Account)
                WHERE a.fraud_score >= 0.7 OR b.fraud_score >= 0.7
                RETURN count(r) as count
            """).single()
            
            print(f"Số giao dịch từ HOẶC đến tài khoản có fraud_score >= 0.7: {high_score_txn['count'] if high_score_txn else 0}")
            
            # Thống kê tài khoản source và target 
            stats = session.run("""
                MATCH (src:Account)-[r:SENT]->(tgt:Account)
                WHERE src.fraud_score >= 0.7 OR tgt.fraud_score >= 0.7
                RETURN 
                    count(DISTINCT r) as transactions,
                    count(DISTINCT src) as source_accounts,
                    count(DISTINCT tgt) as target_accounts,
                    count(DISTINCT CASE WHEN src.fraud_score >= 0.7 THEN src END) as high_score_sources,
                    count(DISTINCT CASE WHEN tgt.fraud_score >= 0.7 THEN tgt END) as high_score_targets
            """).single()
            
            if stats:
                print(f"Tổng số giao dịch: {stats['transactions']}")
                print(f"Tổng số tài khoản nguồn: {stats['source_accounts']}")
                print(f"Tổng số tài khoản đích: {stats['target_accounts']}")
                print(f"Số tài khoản nguồn có fraud_score cao: {stats['high_score_sources']}")
                print(f"Số tài khoản đích có fraud_score cao: {stats['high_score_targets']}")
            
            # Kiểm tra các tài khoản fraud_score cao
            print("\nTài khoản có fraud_score cao là nguồn hay đích?")
            high_score_accounts = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score >= 0.7
                WITH a
                OPTIONAL MATCH (a)-[out:SENT]->()
                WITH a, count(out) as out_count
                OPTIONAL MATCH ()-[in:SENT]->(a)
                RETURN 
                    a.id as id, 
                    a.fraud_score as score,
                    out_count,
                    count(in) as in_count
                ORDER BY in_count DESC
                LIMIT 10
            """).data()
            
            for acc in high_score_accounts:
                print(f"Tài khoản {acc['id']}: score={acc['score']}, gửi={acc['out_count']}, nhận={acc['in_count']}")

if __name__ == "__main__":
    fixer = QueryFixer()
    try:
        print("Kiểm tra và sửa queries...")
        fixer.fix_queries()
    finally:
        fixer.close()
