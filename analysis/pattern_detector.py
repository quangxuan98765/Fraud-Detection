from config import FRAUD_SCORE_THRESHOLD

class PatternDetector:
    def __init__(self, driver):
        self.driver = driver

    def calculate_fraud_scores(self):
        """Kết hợp tất cả điểm để tính điểm gian lận"""
        with self.driver.session() as session:
            print("Đang tính toán fraud_score cho tất cả tài khoản...")
            
            # Đánh dấu tài khoản liên quan đến giao dịch gian lận đã biết
            session.run("""
                MATCH (s:Account)-[r:SENT {is_fraud: 1}]->(t:Account)
                SET s.known_fraud = true, t.known_fraud = true
            """)

            # Tính điểm cơ bản dựa trên thuộc tính
            base_score_query = """
                MATCH (a:Account)
                
                WITH a, 
                    COALESCE(a.pagerank_score, 0) AS pagerank,
                    COALESCE(a.degree_score, 0) AS degree,
                    COALESCE(a.similarity_score, 0) AS similarity,
                    COALESCE(a.tx_imbalance, 0) AS imbalance,
                    COALESCE(a.high_tx_volume, false) AS high_volume,
                    COALESCE(a.tx_anomaly, false) AS anomaly,
                    COALESCE(a.only_sender, false) AS only_sender,
                    COALESCE(a.high_value_tx, false) AS high_value

                WITH a,
                    pagerank * 0.05 +
                    degree * 0.03 + 
                    similarity * 0.02 +
                    
                    CASE 
                        WHEN imbalance > 0.8 THEN 0.40
                        WHEN imbalance > 0.65 THEN 0.30
                        WHEN imbalance > 0.5 THEN 0.20
                        ELSE imbalance * 0.15
                    END +
                    
                    CASE 
                        WHEN high_volume AND anomaly AND imbalance > 0.65 THEN 0.35
                        WHEN high_volume AND anomaly AND imbalance > 0.5 THEN 0.25
                        WHEN high_volume AND imbalance > 0.6 THEN 0.15
                        WHEN high_volume THEN 0.05
                        ELSE 0 
                    END +
                    
                    CASE 
                        WHEN anomaly AND high_value AND imbalance > 0.6 THEN 0.40
                        WHEN anomaly AND high_value AND imbalance > 0.4 THEN 0.30
                        WHEN anomaly AND high_value THEN 0.20
                        WHEN anomaly THEN 0.10
                        ELSE 0 
                    END +
                    
                    CASE WHEN a.known_fraud THEN 0.90 ELSE 0 END +
                    CASE WHEN high_value AND only_sender AND imbalance > 0.5 THEN 0.25
                         WHEN high_value AND only_sender THEN 0.12
                         WHEN high_value THEN 0.07
                         ELSE 0 
                    END AS base_score
                
                SET a.base_score = base_score
                
                RETURN count(a) AS updated_count
            """
            
            result = session.run(base_score_query)
            print(f"Đã tính điểm cơ bản cho {result.single()['updated_count']} tài khoản")

            # Phát hiện mẫu giao dịch
            self._detect_transaction_patterns(session)
            
            # Phát hiện chu trình
            self._detect_cycles(session)

            # Tổng hợp điểm cuối cùng
            self._calculate_final_scores(session)

    def _detect_transaction_patterns(self, session):
        """Phát hiện mẫu giao dịch bất thường"""
        transaction_pattern_query = """
            MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
            WHERE tx.amount > 50000
            
            WITH sender, receiver, collect(tx) AS transactions,
                 sum(tx.amount) AS total_amount,
                 count(tx) AS tx_count,
                 sender.base_score AS sender_score,
                 receiver.base_score AS receiver_score,
                 sender.tx_imbalance AS sender_imbalance,
                 receiver.tx_imbalance AS receiver_imbalance
            
            WHERE tx_count >= 2 AND total_amount > 80000 AND 
                 ((sender_score > 0.6 AND sender_imbalance > 0.5) OR 
                  (receiver_score > 0.6 AND receiver_imbalance > 0.5))
            
            WITH sender, receiver, 
                 CASE 
                     WHEN sender_score > 0.75 AND receiver_score > 0.6 THEN 0.18
                     WHEN sender_score > 0.6 OR receiver_score > 0.6 THEN 0.12
                     ELSE 0.08
                 END AS boost
            
            SET sender.relation_boost = COALESCE(sender.relation_boost, 0) + boost,
                receiver.relation_boost = COALESCE(receiver.relation_boost, 0) + boost
            
            RETURN count(sender) + count(receiver) AS boosted_accounts
        """
        
        relation_result = session.run(transaction_pattern_query)
        boosted_accounts = relation_result.single()["boosted_accounts"] if relation_result else 0
        print(f"Đã điều chỉnh điểm cho {boosted_accounts} tài khoản dựa trên mẫu giao dịch")

    def _detect_cycles(self, session):
        """Phát hiện chu trình giao dịch"""
        cycle_detection_query = """
            MATCH path = (a:Account)-[r:SENT*2..4]->(a)
            WITH path, 
                 [node IN nodes(path) | node] AS cycle_nodes,
                 reduce(total = 0, r IN relationships(path) | total + r.amount) AS cycle_amount,
                 reduce(max_val = 0, r IN relationships(path) | CASE WHEN r.amount > max_val THEN r.amount ELSE max_val END) AS max_tx_amount
            
            WHERE cycle_amount > 80000 AND max_tx_amount > 30000
            
            WITH DISTINCT cycle_nodes, cycle_amount, max_tx_amount
            UNWIND cycle_nodes AS cycle_account
            WITH cycle_account, cycle_nodes, cycle_amount, max_tx_amount,
                 count(CASE WHEN cycle_account.tx_anomaly THEN 1 END) as anomalies,
                 count(CASE WHEN cycle_account.high_value_tx THEN 1 END) as high_value_txs,
                 avg(CASE WHEN cycle_account.tx_imbalance IS NOT NULL THEN cycle_account.tx_imbalance ELSE 0 END) as avg_imbalance
            
            WHERE 
                anomalies > 0 OR
                high_value_txs > 0 AND     
                cycle_account.tx_imbalance > 0.4 AND
                avg_imbalance > 0.35
            
            SET 
                cycle_account.cycle_boost = CASE
                    WHEN size(cycle_nodes) = 2 AND cycle_amount > 120000 THEN 0.45
                    WHEN size(cycle_nodes) = 3 AND cycle_amount > 100000 THEN 0.35
                    WHEN size(cycle_nodes) <= 4 AND cycle_amount > 80000 THEN 0.25
                    ELSE 0.15
                END,
                cycle_account.known_fraud = CASE 
                    WHEN size(cycle_nodes) <= 3 AND cycle_amount > 120000 THEN true
                    ELSE cycle_account.known_fraud 
                END
            
            RETURN count(DISTINCT cycle_account) AS cycle_accounts
        """
        
        cycle_result = session.run(cycle_detection_query)
        cycle_accounts = cycle_result.single()["cycle_accounts"] if cycle_result else 0
        print(f"Đã phát hiện {cycle_accounts} tài khoản thuộc các chu trình giao dịch")

    def _calculate_final_scores(self, session):
        """Tính toán điểm gian lận cuối cùng"""
        final_score_query = """
            MATCH (a:Account)
            
            WITH a,
                 COALESCE(a.base_score, 0) AS base,
                 COALESCE(a.relation_boost, 0) AS relation,
                 COALESCE(a.cycle_boost, 0) AS cycle
            
            SET a.fraud_score = CASE
                WHEN base + relation + cycle > 1 THEN 1
                ELSE base + relation + cycle
            END
            
            RETURN count(a) as processed_count
        """
        
        final_result = session.run(final_score_query)
        print(f"Đã hoàn thành tính điểm gian lận cho {final_result.single()['processed_count']} tài khoản")
