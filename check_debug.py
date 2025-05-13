from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class DebugChecker:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    def close(self):
        self.driver.close()
    
    def check_debug_metrics(self):
        with self.driver.session() as session:
            # Kiểm tra số liệu debug sau khi sửa
            debug_metrics = session.run("""
                MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
                
                RETURN count(DISTINCT sender) + count(DISTINCT receiver) AS total_accounts,
                       count(DISTINCT tx) AS total_transactions,
                       count(DISTINCT CASE WHEN sender.fraud_score > 0.7 THEN sender END) + 
                          count(DISTINCT CASE WHEN receiver.fraud_score > 0.7 THEN receiver END) AS fraud_accounts_07,
                       
                       count(DISTINCT CASE WHEN sender.fraud_score > 0.7 OR receiver.fraud_score > 0.7 THEN tx END) AS fraud_transactions_07,
                       
                       // Count of actual fraud transactions from ground truth (for evaluation only)
                       count(DISTINCT CASE WHEN tx.is_fraud = 1 THEN tx END) AS real_fraud_transactions,
                       
                       // True positives at different thresholds (using is_fraud for evaluation only)
                       count(DISTINCT CASE WHEN tx.is_fraud = 1 AND (sender.fraud_score > 0.7 OR receiver.fraud_score > 0.7) THEN tx END) AS true_positives_07
            """).single()
            
            if debug_metrics:
                print("Số liệu debug hiện tại:")
                print(f"- Tổng số tài khoản: {debug_metrics['total_accounts']}")
                print(f"- Tổng số giao dịch: {debug_metrics['total_transactions']}")
                print(f"- Tài khoản có fraud_score > 0.7: {debug_metrics['fraud_accounts_07']}")
                print(f"- Giao dịch liên quan đến tài khoản có fraud_score > 0.7: {debug_metrics['fraud_transactions_07']}")
                print(f"- Giao dịch có is_fraud = 1 (thực sự gian lận): {debug_metrics['real_fraud_transactions']}")
                print(f"- True positives ở ngưỡng 0.7: {debug_metrics['true_positives_07']}")
            else:
                print("Không thể lấy số liệu debug")
            
            # Kiểm tra model metrics
            model_metrics = session.run("""
                MATCH (src:Account)-[t:SENT]->(tgt:Account)
                RETURN 
                    count(CASE WHEN src.model1_score > 0.5 OR tgt.model1_score > 0.5 THEN t END) as model1_txs,
                    count(CASE WHEN src.model2_score > 0.5 OR tgt.model2_score > 0.5 THEN t END) as model2_txs,
                    count(CASE WHEN src.model3_score > 0.5 OR tgt.model3_score > 0.5 THEN t END) as model3_txs,
                    count(CASE WHEN src.high_confidence_pattern = true OR tgt.high_confidence_pattern = true THEN t END) as high_confidence_txs,
                    count(CASE WHEN src.funnel_pattern = true OR tgt.funnel_pattern = true THEN t END) as funnel_txs,
                    count(CASE WHEN src.round_pattern = true OR tgt.round_pattern = true THEN t END) as round_txs,
                    count(CASE WHEN src.chain_pattern = true OR tgt.chain_pattern = true THEN t END) as chain_txs,
                    count(CASE WHEN src.similar_to_fraud = true OR tgt.similar_to_fraud = true THEN t END) as similar_txs,
                    count(CASE WHEN src.high_velocity = true OR tgt.high_velocity = true THEN t END) as velocity_txs
            """).single()
            
            if model_metrics:
                print("\nSố liệu từ các mô hình:")
                print(f"- Model 1: {model_metrics['model1_txs']}")
                print(f"- Model 2: {model_metrics['model2_txs']}")
                print(f"- Model 3: {model_metrics['model3_txs']}")
                print(f"- High confidence: {model_metrics['high_confidence_txs']}")
                print(f"- Funnel: {model_metrics['funnel_txs']}")
                print(f"- Round: {model_metrics['round_txs']}")
                print(f"- Chain: {model_metrics['chain_txs']}")
                print(f"- Similarity: {model_metrics['similar_txs']}")
                print(f"- Velocity: {model_metrics['velocity_txs']}")

if __name__ == "__main__":
    checker = DebugChecker()
    try:
        print("Kiểm tra số liệu debug sau khi sửa...")
        checker.check_debug_metrics()
    finally:
        checker.close()
