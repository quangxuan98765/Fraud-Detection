from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class ApiPatcher:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    def close(self):
        self.driver.close()
    
    def add_real_fraud_metrics(self):
        with self.driver.session() as session:
            # 1. Kiểm tra số giao dịch có is_fraud = 1
            fraud_count = session.run("""
                MATCH ()-[r:SENT]->()
                WHERE r.is_fraud = 1
                RETURN count(r) as count
            """).single()
            
            print(f"Số giao dịch có is_fraud = 1: {fraud_count['count'] if fraud_count else 0}")
            
            # 2. Kiểm tra true positives ở các ngưỡng khác nhau
            true_positives = session.run("""
                MATCH (src:Account)-[tx:SENT]->(tgt:Account)
                WHERE tx.is_fraud = 1
                RETURN 
                    count(DISTINCT CASE WHEN src.fraud_score >= 0.5 OR tgt.fraud_score >= 0.5 THEN tx END) as tp_05,
                    count(DISTINCT CASE WHEN src.fraud_score >= 0.6 OR tgt.fraud_score >= 0.6 THEN tx END) as tp_06,
                    count(DISTINCT CASE WHEN src.fraud_score >= 0.7 OR tgt.fraud_score >= 0.7 THEN tx END) as tp_07
            """).single()
            
            if true_positives:
                print(f"True positives ở ngưỡng 0.5: {true_positives['tp_05']}")
                print(f"True positives ở ngưỡng 0.6: {true_positives['tp_06']}")
                print(f"True positives ở ngưỡng 0.7: {true_positives['tp_07']}")
                
            # 3. Tính metrics chính xác
            metrics = session.run("""
                MATCH (src:Account)-[tx:SENT]->(tgt:Account)
                
                WITH 
                    count(DISTINCT tx) as total_transactions,
                    count(DISTINCT CASE WHEN tx.is_fraud = 1 THEN tx END) as ground_truth,
                    count(DISTINCT CASE WHEN src.fraud_score >= 0.7 OR tgt.fraud_score >= 0.7 THEN tx END) as detected_fraud,
                    count(DISTINCT CASE WHEN tx.is_fraud = 1 AND (src.fraud_score >= 0.7 OR tgt.fraud_score >= 0.7) THEN tx END) as true_positive
                
                RETURN
                    total_transactions,
                    ground_truth,
                    detected_fraud,
                    true_positive,
                    CASE WHEN detected_fraud > 0 THEN 1.0 * true_positive / detected_fraud ELSE 0 END as precision,
                    CASE WHEN ground_truth > 0 THEN 1.0 * true_positive / ground_truth ELSE 0 END as recall
            """).single()
            
            if metrics:
                print("\nMetrics chính xác:")
                print(f"- Tổng số giao dịch: {metrics['total_transactions']}")
                print(f"- Ground truth (is_fraud=1): {metrics['ground_truth']}")
                print(f"- Phát hiện gian lận (fraud_score>=0.7): {metrics['detected_fraud']}")
                print(f"- True positives: {metrics['true_positive']}")
                print(f"- Precision: {metrics['precision']:.2f}")
                print(f"- Recall: {metrics['recall']:.2f}")
                
                if metrics['precision'] + metrics['recall'] > 0:
                    f1 = 2 * (metrics['precision'] * metrics['recall']) / (metrics['precision'] + metrics['recall'])
                    print(f"- F1 Score: {f1:.2f}")
                else:
                    print("- F1 Score: 0.00")

if __name__ == "__main__":
    patcher = ApiPatcher()
    try:
        print("Kiểm tra và cập nhật metrics ground truth...")
        patcher.add_real_fraud_metrics()
    finally:
        patcher.close()
