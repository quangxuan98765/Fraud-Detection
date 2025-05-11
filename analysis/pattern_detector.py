from config import FRAUD_SCORE_THRESHOLD

class PatternDetector:
    def __init__(self, driver):
        self.driver = driver
        
    def calculate_fraud_scores(self):
        """Kết hợp tất cả điểm để tính điểm gian lận"""
        with self.driver.session() as session:
            print("Đang tính toán fraud_score cho tất cả tài khoản...")
            
            # Tìm các tài khoản có đặc điểm bất thường rõ ràng
            session.run("""
                MATCH (s:Account)-[r:SENT]->(t:Account)
                WHERE r.amount > 200000 AND 
                      ((s.tx_anomaly = true AND s.tx_imbalance > 0.85) OR 
                       (t.tx_anomaly = true AND t.tx_imbalance > 0.85))
                SET s.potential_fraud = true, t.potential_fraud = true
            """)

            # Tính điểm cơ bản dựa trên thuộc tính
            base_score_query = """                MATCH (a:Account)
                
                WITH a, 
                    COALESCE(a.pagerank_score, 0) AS pagerank,
                    COALESCE(a.degree_score, 0) AS degree,
                    COALESCE(a.similarity_score, 0) AS similarity,
                    COALESCE(a.tx_imbalance, 0) AS imbalance,
                    COALESCE(a.high_tx_volume, false) AS high_volume,                    COALESCE(a.tx_anomaly, false) AS anomaly,
                    COALESCE(a.only_sender, false) AS only_sender,
                    COALESCE(a.high_value_tx, false) AS high_value,
                    COALESCE(a.potential_fraud, false) AS potential_fraud
                
                WITH a,
                    pagerank * 0.05 +
                    degree * 0.03 + 
                    similarity * 0.02 +
                    
                    CASE 
                        WHEN imbalance > 0.9 THEN 0.30  // Tăng ngưỡng và giảm trọng số
                        WHEN imbalance > 0.8 THEN 0.25
                        WHEN imbalance > 0.7 THEN 0.15
                        WHEN imbalance > 0.6 THEN 0.10
                        ELSE imbalance * 0.08  // Giảm hệ số nhân
                    END +
                    
                    CASE 
                        // Yêu cầu sự kết hợp mạnh mẽ hơn để coi là đáng ngờ
                        WHEN high_volume AND anomaly AND imbalance > 0.8 THEN 0.30
                        WHEN high_volume AND anomaly AND imbalance > 0.7 THEN 0.20
                        WHEN high_volume AND imbalance > 0.7 THEN 0.10
                        WHEN high_volume THEN 0.03
                        ELSE 0 
                    END +
                      CASE 
                        // Thêm điều kiện khắt khe hơn
                        WHEN anomaly AND high_value AND imbalance > 0.8 THEN 0.35
                        WHEN anomaly AND high_value AND imbalance > 0.6 THEN 0.25
                        WHEN anomaly AND high_value THEN 0.15
                        WHEN anomaly AND imbalance > 0.7 THEN 0.08
                        WHEN anomaly THEN 0.05
                        ELSE 0 
                    END +
                    
                    // Thay thế known_fraud bằng potential_fraud
                    CASE WHEN potential_fraud THEN 0.80 ELSE 0 END +
                    
                    CASE
                         WHEN high_value AND only_sender AND imbalance > 0.7 THEN 0.20
                         WHEN high_value AND only_sender AND imbalance > 0.5 THEN 0.12
                         WHEN high_value AND only_sender THEN 0.08
                         WHEN high_value AND imbalance > 0.6 THEN 0.05
                         WHEN high_value THEN 0.03
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
            WHERE tx.amount > 70000  // Tăng ngưỡng số tiền
            
            WITH sender, receiver, collect(tx) AS transactions,
                 sum(tx.amount) AS total_amount,
                 count(tx) AS tx_count,
                 sender.base_score AS sender_score,
                 receiver.base_score AS receiver_score,
                 sender.tx_imbalance AS sender_imbalance,
                 receiver.tx_imbalance AS receiver_imbalance
            
            // Tăng ngưỡng và yêu cầu sự kết hợp của nhiều điều kiện
            WHERE tx_count >= 3 AND total_amount > 100000 AND 
                 ((sender_score > 0.7 AND sender_imbalance > 0.6 AND sender.tx_anomaly = true) OR 
                  (receiver_score > 0.7 AND receiver_imbalance > 0.6 AND receiver.tx_anomaly = true))
            
            WITH sender, receiver, 
                 CASE 
                     // Điều chỉnh giảm điểm tăng cường
                     WHEN sender_score > 0.8 AND receiver_score > 0.7 THEN 0.15
                     WHEN sender_score > 0.7 OR receiver_score > 0.7 THEN 0.10
                     ELSE 0.05
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
            
            // Tăng ngưỡng chu trình và giá trị giao dịch
            WHERE cycle_amount > 100000 AND max_tx_amount > 40000
            
            WITH DISTINCT cycle_nodes, cycle_amount, max_tx_amount
            UNWIND cycle_nodes AS cycle_account
            WITH cycle_account, cycle_nodes, cycle_amount, max_tx_amount,
                 count(CASE WHEN cycle_account.tx_anomaly THEN 1 END) as anomalies,
                 count(CASE WHEN cycle_account.high_value_tx THEN 1 END) as high_value_txs,
                 avg(CASE WHEN cycle_account.tx_imbalance IS NOT NULL THEN cycle_account.tx_imbalance ELSE 0 END) as avg_imbalance
            
            // Thêm điều kiện khắt khe hơn cho các chu trình giao dịch
            WHERE 
                (anomalies > 0 AND high_value_txs > 0) OR  // Yêu cầu cả hai điều kiện 
                (cycle_account.tx_imbalance > 0.7 AND     // Tăng ngưỡng mất cân bằng
                avg_imbalance > 0.6)
            
            SET 
                cycle_account.cycle_boost = CASE
                    // Giảm điểm tăng cường cho mỗi loại chu trình
                    WHEN size(cycle_nodes) = 2 AND cycle_amount > 150000 THEN 0.35
                    WHEN size(cycle_nodes) = 3 AND cycle_amount > 120000 THEN 0.25
                    WHEN size(cycle_nodes) <= 4 AND cycle_amount > 100000 THEN 0.15
                    ELSE 0.10
                END,
                cycle_account.suspected_fraud = CASE 
                    // Đánh dấu nghi ngờ gian lận cho những chu trình rõ ràng
                    WHEN size(cycle_nodes) <= 3 AND cycle_amount > 150000 AND max_tx_amount > 80000 THEN true
                    ELSE cycle_account.suspected_fraud 
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
