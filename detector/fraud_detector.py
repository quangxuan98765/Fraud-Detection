from detector.database_manager import DatabaseManager
from detector.data_importer import DataImporter
from analysis.account_analyzer import AccountAnalyzer
from analysis.transaction_analyzer import TransactionAnalyzer
from analysis.pattern_detector import PatternDetector
from config import FRAUD_SCORE_THRESHOLD

class FraudDetector:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.data_importer = DataImporter(self.db_manager)
        self.account_analyzer = AccountAnalyzer(self.db_manager.driver)
        self.transaction_analyzer = TransactionAnalyzer(self.db_manager.driver)
        self.pattern_detector = PatternDetector(self.db_manager.driver)
        # Thêm property driver để đảm bảo khả năng tương thích với các API
        self.driver = self.db_manager.driver
        
    def check_data(self):
        """Kiểm tra xem đã có dữ liệu trong database chưa"""
        return self.db_manager.check_data()
        
    def clear_database(self):
        """Xóa toàn bộ dữ liệu trong database"""
        return self.db_manager.clear_database()
            
    def import_data(self, csv_path):
        """Import dữ liệu sử dụng API Neo4j thay vì LOAD CSV"""
        return self.data_importer.import_data(csv_path)
        
    def finalize_and_evaluate(self):
        """Chuẩn hóa điểm và đánh giá kết quả với thuật toán tối ưu"""
        with self.db_manager.driver.session() as session:
            print("🔍 Đang hoàn tất phân tích với thuật toán tối ưu...")
            
            # Sử dụng Optimized Pattern Detector để tính điểm
            print("  Applying optimized multi-model fraud detection algorithms...")
            self.pattern_detector.calculate_fraud_scores()
            
            # Detecting specialized fraud patterns
            print("  Detecting specialized fraud patterns...")
            self.pattern_detector.detect_specialized_patterns(session)
            
            # Analyze transaction velocity if possible
            print("  Analyzing transaction velocity patterns...")
            self.pattern_detector.analyze_transaction_velocity(session)
            
            # Final score calibration
            print("  Calibrating final risk scores...")
            self.pattern_detector.calculate_final_risk_score(session)
            
            # Mark high-risk accounts with optimized criteria
            print("  Đang đánh dấu tài khoản có rủi ro cao với tiêu chí tối ưu...")
            session.run(f"""
                MATCH (a:Account)
                WHERE 
                    (a.fraud_score > {FRAUD_SCORE_THRESHOLD}) OR  // Use configured threshold
                    (a.risk_level = 'very_high') OR
                    (a.risk_level = 'high')
                SET a.high_risk = true,
                    a.risk_factors = CASE WHEN a.fraud_score > {FRAUD_SCORE_THRESHOLD} THEN ['high_fraud_score'] ELSE [] END
                        + CASE WHEN a.model1_score > 0.6 THEN ['network_structure'] ELSE [] END
                        + CASE WHEN a.model2_score > 0.6 THEN ['behavior_patterns'] ELSE [] END
                        + CASE WHEN a.model3_score > 0.6 THEN ['complex_patterns'] ELSE [] END
                        + CASE WHEN a.tx_anomaly = true THEN ['transaction_anomaly'] ELSE [] END
                        + CASE WHEN a.cycle_boost > 0.2 THEN ['suspicious_cycle'] ELSE [] END
                        + CASE WHEN a.potential_mule = true THEN ['money_mule'] ELSE [] END
                        + CASE WHEN a.high_confidence_pattern = true THEN ['high_confidence_pattern'] ELSE [] END
                        + CASE WHEN a.similar_to_fraud = true THEN ['similar_to_fraud'] ELSE [] END
                        + CASE WHEN a.funnel_disperse_pattern = true THEN ['funnel_disperse'] ELSE [] END
                        + CASE WHEN a.round_tx_pattern = true THEN ['round_transactions'] ELSE [] END
                        + CASE WHEN a.increasing_chain = true THEN ['increasing_chain'] ELSE [] END
                        + CASE WHEN a.high_velocity = true THEN ['high_velocity'] ELSE [] END
            """)
            
            # Validate detection effectiveness
            print("  Đang kiểm tra hiệu quả phát hiện với thuật toán tối ưu...")
            validation = session.run("""
                MATCH (a:Account)
                WHERE a.high_risk = true
                
                WITH collect(DISTINCT a.risk_factors) AS all_factors,
                     count(DISTINCT a) AS flagged_accounts,
                     count(DISTINCT CASE WHEN size(a.risk_factors) > 1 THEN a END) AS multi_factor,
                     count(DISTINCT CASE WHEN 'high_fraud_score' IN a.risk_factors THEN a END) AS fraud_score,
                     count(DISTINCT CASE WHEN 'transaction_anomaly' IN a.risk_factors THEN a END) AS tx_anomaly,
                     count(DISTINCT CASE WHEN 'suspicious_cycle' IN a.risk_factors THEN a END) AS cycles,
                     count(DISTINCT CASE WHEN 'network_structure' IN a.risk_factors THEN a END) AS network,
                     count(DISTINCT CASE WHEN 'behavior_patterns' IN a.risk_factors THEN a END) AS behavior,
                     count(DISTINCT CASE WHEN 'complex_patterns' IN a.risk_factors THEN a END) AS complex_patterns,
                     count(DISTINCT CASE WHEN 'money_mule' IN a.risk_factors THEN a END) AS mules,
                     count(DISTINCT CASE WHEN 'high_confidence_pattern' IN a.risk_factors THEN a END) AS high_conf,
                     count(DISTINCT CASE WHEN 'similar_to_fraud' IN a.risk_factors THEN a END) AS similar,
                     count(DISTINCT CASE WHEN 'funnel_disperse' IN a.risk_factors THEN a END) AS funnel,
                     count(DISTINCT CASE WHEN 'round_transactions' IN a.risk_factors THEN a END) AS round_tx,
                     count(DISTINCT CASE WHEN 'increasing_chain' IN a.risk_factors THEN a END) AS inc_chain,
                     count(DISTINCT CASE WHEN 'high_velocity' IN a.risk_factors THEN a END) AS velocity
                RETURN 
                    flagged_accounts,
                    multi_factor,
                    1.0 * multi_factor / flagged_accounts AS multi_factor_ratio,
                    fraud_score, tx_anomaly, cycles, network, behavior, complex_patterns,
                    mules, high_conf, similar, funnel, round_tx, inc_chain, velocity
            """).single()
            
            if validation:
                flagged = validation.get("flagged_accounts", 0)
                multi = validation.get("multi_factor", 0)
                multi_ratio = validation.get("multi_factor_ratio", 0)
                fraud_score = validation.get("fraud_score", 0)
                tx_anomaly = validation.get("tx_anomaly", 0)
                cycles = validation.get("cycles", 0)
                network = validation.get("network", 0)
                behavior = validation.get("behavior", 0)
                complex_patterns = validation.get("complex_patterns", 0)
                mules = validation.get("mules", 0)
                high_conf = validation.get("high_conf", 0)
                similar = validation.get("similar", 0)
                funnel = validation.get("funnel", 0)
                round_tx = validation.get("round_tx", 0)
                inc_chain = validation.get("inc_chain", 0)
                velocity = validation.get("velocity", 0)
                
                print(f"\nKết quả phát hiện gian lận với thuật toán tối ưu:")
                print(f"  Tổng số tài khoản đáng ngờ: {flagged}")
                print(f"  - Có nhiều yếu tố: {multi} ({multi_ratio:.1%})")
                print(f"  - Điểm gian lận cao: {fraud_score}")
                print(f"  - Mô hình cấu trúc mạng: {network}")
                print(f"  - Mô hình hành vi: {behavior}")
                print(f"  - Mô hình mẫu phức tạp: {complex_patterns}")
                print(f"  - Giao dịch bất thường: {tx_anomaly}")
                print(f"  - Nằm trong chu trình: {cycles}")
                print(f"  - Tài khoản trung gian: {mules}")
                print(f"  - Mẫu độ tin cậy cao: {high_conf}")
                print(f"  - Tương tự tài khoản gian lận: {similar}")
                print(f"  - Mẫu phễu và phân tán: {funnel}")
                print(f"  - Giao dịch số tròn: {round_tx}")
                print(f"  - Chuỗi tăng dần: {inc_chain}")
                print(f"  - Tốc độ giao dịch cao: {velocity}")
            
            # Performance Metrics (if ground truth is available)
            metrics = session.run("""
                MATCH (a:Account)
                WITH
                    count(a) AS total_accounts,
                    count(CASE WHEN a.is_fraud = true THEN a END) AS actual_fraud,
                    count(CASE WHEN a.fraud_score > 0.5 THEN a END) AS detected_05,
                    count(CASE WHEN a.fraud_score > 0.6 THEN a END) AS detected_06,
                    count(CASE WHEN a.fraud_score > 0.7 THEN a END) AS detected_07,
                    count(CASE WHEN a.is_fraud = true AND a.fraud_score > 0.5 THEN a END) AS true_pos_05,
                    count(CASE WHEN a.is_fraud = true AND a.fraud_score > 0.6 THEN a END) AS true_pos_06,
                    count(CASE WHEN a.is_fraud = true AND a.fraud_score > 0.7 THEN a END) AS true_pos_07
                
                RETURN 
                    total_accounts,
                    actual_fraud,
                    detected_05, detected_06, detected_07,
                    true_pos_05, true_pos_06, true_pos_07,
                    
                    // Calculate precision, recall, F1 at different thresholds
                    CASE WHEN detected_05 > 0 THEN 1.0 * true_pos_05 / detected_05 ELSE 0 END AS precision_05,
                    CASE WHEN detected_06 > 0 THEN 1.0 * true_pos_06 / detected_06 ELSE 0 END AS precision_06,
                    CASE WHEN detected_07 > 0 THEN 1.0 * true_pos_07 / detected_07 ELSE 0 END AS precision_07,
                    
                    CASE WHEN actual_fraud > 0 THEN 1.0 * true_pos_05 / actual_fraud ELSE 0 END AS recall_05,
                    CASE WHEN actual_fraud > 0 THEN 1.0 * true_pos_06 / actual_fraud ELSE 0 END AS recall_06,
                    CASE WHEN actual_fraud > 0 THEN 1.0 * true_pos_07 / actual_fraud ELSE 0 END AS recall_07
            """).single()
            
            if metrics and metrics.get("actual_fraud", 0) > 0:
                total = metrics.get("total_accounts", 0)
                actual = metrics.get("actual_fraud", 0)
                detected_05 = metrics.get("detected_05", 0)
                detected_06 = metrics.get("detected_06", 0)
                detected_07 = metrics.get("detected_07", 0)
                true_pos_05 = metrics.get("true_pos_05", 0)
                true_pos_06 = metrics.get("true_pos_06", 0)
                true_pos_07 = metrics.get("true_pos_07", 0)
                precision_05 = metrics.get("precision_05", 0)
                precision_06 = metrics.get("precision_06", 0)
                precision_07 = metrics.get("precision_07", 0)
                recall_05 = metrics.get("recall_05", 0)
                recall_06 = metrics.get("recall_06", 0)
                recall_07 = metrics.get("recall_07", 0)
                
                # Calculate F1 scores
                f1_05 = 2 * (precision_05 * recall_05) / (precision_05 + recall_05) if (precision_05 + recall_05) > 0 else 0
                f1_06 = 2 * (precision_06 * recall_06) / (precision_06 + recall_06) if (precision_06 + recall_06) > 0 else 0
                f1_07 = 2 * (precision_07 * recall_07) / (precision_07 + recall_07) if (precision_07 + recall_07) > 0 else 0
                
                print(f"\nMetrics hiệu suất phát hiện gian lận:")
                print(f"  Tổng số tài khoản: {total:,}")
                print(f"  Tài khoản gian lận thực tế: {actual:,}")
                print(f"  Ngưỡng 0.5: {detected_05:,} phát hiện, {true_pos_05:,} đúng, " +
                      f"Precision: {precision_05:.1%}, Recall: {recall_05:.1%}, F1: {f1_05:.1%}")
                print(f"  Ngưỡng 0.6: {detected_06:,} phát hiện, {true_pos_06:,} đúng, " +
                      f"Precision: {precision_06:.1%}, Recall: {recall_06:.1%}, F1: {f1_06:.1%}")
                print(f"  Ngưỡng 0.7: {detected_07:,} phát hiện, {true_pos_07:,} đúng, " +
                      f"Precision: {precision_07:.1%}, Recall: {recall_07:.1%}, F1: {f1_07:.1%}")
            
            return True

    def analyze_fraud(self):
        """Run optimized fraud analysis with graph algorithms"""
        try:
            with self.db_manager.driver.session() as session:
                # Clear old analysis data
                print("🔍 Clearing old analysis...")
                session.run("""
                    MATCH (a:Account) 
                    REMOVE a.fraud_score, a.community, a.pagerank_score, 
                        a.degree_score, a.similarity_score, a.path_score, a.suspected_fraud,
                        a.base_score, a.tx_anomaly, a.high_tx_volume, a.only_sender,
                        a.potential_mule, a.mule_boost, a.temporal_boost, a.rapid_transfer,
                        a.model1_score, a.model2_score, a.model3_score, a.ensemble_score,
                        a.optimized_score, a.confidence_level, a.feature_importance,
                        a.high_confidence_pattern, a.round_tx_pattern, a.funnel_disperse_pattern,
                        a.increasing_chain, a.similar_to_fraud, a.high_velocity, a.velocity_ratio,
                        a.calibrated_score, a.risk_level
                """)
    
                # Clear old SIMILAR_TO relationships
                print("🔍 Clearing old relationships...")
                session.run("MATCH ()-[r:SIMILAR_TO]->() DELETE r")
                
                # Create index if needed
                session.run("CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.id)")
                
                # Remove old projected graph
                print("🔍 Removing old projected graph...")
                try:
                    result = session.run("""
                        CALL gds.graph.list()
                        YIELD graphName
                        WHERE graphName = 'fraud_graph'
                        RETURN count(*) > 0 AS exists
                    """).single()
    
                    if result and result.get('exists', False):
                        print("  Found existing projected graph, removing...")
                        session.run("CALL gds.graph.drop('fraud_graph', false)")
                except Exception as e:
                    print(f"  Error checking projected graph: {e}")
                
                # 1. Create optimized projected graph 
                print("🔍 Creating optimized projected graph...")
                session.run("""
                    CALL gds.graph.project(
                        'fraud_graph',
                        'Account',
                        {
                            SENT: {
                                type: 'SENT',
                                orientation: 'UNDIRECTED',
                                properties: {
                                    amount: {property: 'amount', defaultValue: 0.0}
                                }
                            }
                        }
                    )
                """)
                
                # 2. Degree Centrality with weights
                print("🔍 Calculating weighted Degree Centrality...")
                session.run("""
                    CALL gds.degree.write('fraud_graph', {
                        relationshipWeightProperty: 'amount',
                        writeProperty: 'degree_score'
                    })
                """)
                
                # 3. PageRank with optimized parameters
                print("🔍 Running optimized PageRank...")
                session.run("""
                    CALL gds.pageRank.write('fraud_graph', {
                        maxIterations: 30,
                        dampingFactor: 0.85,
                        relationshipWeightProperty: 'amount',
                        writeProperty: 'pagerank_score'
                    })
                """)
                
                # 4. Optimized Community Detection
                print("🔍 Detecting communities with optimized Louvain...")
                session.run("""
                    CALL gds.louvain.write('fraud_graph', {
                        relationshipWeightProperty: 'amount',
                        maxLevels: 10,
                        maxIterations: 20,
                        tolerance: 0.0001,
                        writeProperty: 'community'
                    })
                """)
                
                # 5. Node Similarity optimization
                print("🔍 Calculating optimized Node Similarity...")
                session.run("""
                    CALL gds.nodeSimilarity.write('fraud_graph', {
                        writeRelationshipType: 'SIMILAR_TO',
                        writeRelationshipProperty: 'similarity',
                        topK: 15,
                        similarityCutoff: 0.5
                    })
                """)
                
                # 6. Transaction analysis
                print("🔍 Calculating transaction flow metrics...")
                self.transaction_analyzer.process_transaction_stats()
    
                # 7. Account behavior analysis
                print("🔍 Marking abnormal behaviors...")
                self.account_analyzer.process_account_behaviors()
                self.account_analyzer.process_transaction_anomalies()
                
                # 8. Apply optimized fraud detection with pattern detector
                print("🔍 Running optimized fraud detection algorithms...")
                self.finalize_and_evaluate()
                
                # 9. Clean up graph
                print("🔍 Cleaning up projected graph...")
                self.db_manager.cleanup_projected_graph()
                print("✅ Optimized fraud analysis complete.")
                return True
                
        except Exception as e:
            print(f"Error during fraud analysis: {e}")
            import traceback
            traceback.print_exc()
            return False

    def cleanup_projected_graph(self):
        """Xóa projected graph với cơ chế timeout và bỏ qua việc kiểm tra tồn tại"""
        return self.db_manager.cleanup_projected_graph()
    
    def close(self):
        """Đóng kết nối đến Neo4j"""
        self.db_manager.close()
