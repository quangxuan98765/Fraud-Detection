from detector.database_manager import DatabaseManager
from detector.data_importer import DataImporter
from analysis.account_analyzer import AccountAnalyzer
from analysis.transaction_analyzer import TransactionAnalyzer
from analysis.pattern_detector import PatternDetector
from queries.detector_queries import FraudDetectorQueries
from config import FRAUD_SCORE_THRESHOLD

class FraudDetector:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.data_importer = DataImporter(self.db_manager)
        self.account_analyzer = AccountAnalyzer(self.db_manager.driver)
        self.transaction_analyzer = TransactionAnalyzer(self.db_manager.driver)
        self.pattern_detector = PatternDetector(self.db_manager.driver)
        self.queries = FraudDetectorQueries()
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
            session.run(self.queries.mark_high_risk_accounts(FRAUD_SCORE_THRESHOLD))
            
            # Validate detection effectiveness
            print("  Đang kiểm tra hiệu quả phát hiện với thuật toán tối ưu...")
            validation = session.run(self.queries.VALIDATION_QUERY).single()
            
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
            metrics = session.run(self.queries.PERFORMANCE_METRICS_QUERY).single()
            
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
                session.run(self.queries.CLEAR_ANALYSIS_DATA_QUERY)
    
                # Clear old SIMILAR_TO relationships
                print("🔍 Clearing old relationships...")
                session.run(self.queries.CLEAR_RELATIONSHIPS_QUERY)
                
                # Create index if needed
                session.run(self.queries.CREATE_INDEX_QUERY)
                
                # Remove old projected graph
                print("🔍 Removing old projected graph...")
                try:
                    result = session.run(self.queries.CHECK_PROJECTED_GRAPH_QUERY).single()
    
                    if result and result.get('exists', False):
                        print("  Found existing projected graph, removing...")
                        session.run(self.queries.DROP_PROJECTED_GRAPH_QUERY)
                except Exception as e:
                    print(f"  Error checking projected graph: {e}")
                
                # 1. Create optimized projected graph 
                print("🔍 Creating optimized projected graph...")
                session.run(self.queries.CREATE_PROJECTED_GRAPH_QUERY)
                
                # 2. Degree Centrality with weights
                print("🔍 Calculating weighted Degree Centrality...")
                session.run(self.queries.DEGREE_CENTRALITY_QUERY)
                
                # 3. PageRank with optimized parameters
                print("🔍 Running optimized PageRank...")
                session.run(self.queries.PAGERANK_QUERY)
                
                # 4. Optimized Community Detection
                print("🔍 Detecting communities with optimized Louvain...")
                session.run(self.queries.COMMUNITY_DETECTION_QUERY)
                
                # 5. Node Similarity optimization
                print("🔍 Calculating optimized Node Similarity...")
                session.run(self.queries.NODE_SIMILARITY_QUERY)
                
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