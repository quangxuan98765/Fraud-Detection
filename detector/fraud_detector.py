import time
from .database_manager import DatabaseManager
from .feature_extraction import FeatureExtractor
from .graph_algorithms import GraphAlgorithms
from .anomaly_detection import AnomalyDetector
from .evaluation import EvaluationManager
from .utils.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DEFAULT_PERCENTILE
from .queries.fraud_detector_queries import (
    # Queries for prepare_ground_truth
    CHECK_FRAUD_FIELD_QUERY,
    MAP_FRAUD_TO_GROUND_TRUTH_QUERY,
    CHECK_GROUND_TRUTH_RESULT_QUERY,
    
    # Queries for examine_data
    GROUND_TRUTH_DISTRIBUTION_QUERY,
    TYPE_CHECK_QUERY,
    SCORE_DISTRIBUTION_QUERY,
    
    # Queries for cleanup
    get_node_cleanup_query,
    RELATIONSHIP_CLEANUP_QUERY,
    DELETE_SIMILAR_RELATIONSHIPS_QUERY
)

class FraudDetector:
    def __init__(self, db_manager: DatabaseManager):
        """Khởi tạo fraud detector với các thành phần con."""
        self.db_manager = db_manager
              # Khởi tạo các thành phần con
        self.feature_extractor = FeatureExtractor(self.db_manager)
        self.graph_algorithms = GraphAlgorithms(self.db_manager)
        self.anomaly_detector = AnomalyDetector(self.db_manager, percentile_cutoff=DEFAULT_PERCENTILE)
        self.evaluation = EvaluationManager(self.db_manager)
        
        # Config
        self.percentile_cutoff = DEFAULT_PERCENTILE

    def prepare_ground_truth(self):
        """Map isFraud từ CSV sang ground_truth_fraud để hỗ trợ đánh giá."""
        print("🔄 Đang chuẩn bị dữ liệu ground truth...")
        
        # Kiểm tra xem isFraud có tồn tại trong SENT relationships không
        result = self.db_manager.run_query(CHECK_FRAUD_FIELD_QUERY)
        
        if result and result["has_is_fraud"] > 0:
            print(f"  • Tìm thấy {result['has_is_fraud']} giao dịch có trường isFraud")
            
            # Map từ isFraud sang ground_truth_fraud
            map_result = self.db_manager.run_query(MAP_FRAUD_TO_GROUND_TRUTH_QUERY)
            
            if map_result:
                print(f"  ✅ Đã map {map_result['mapped']} giao dịch từ isFraud sang ground_truth_fraud")
        else:
            print("  ⚠️ Không tìm thấy trường isFraud trong dữ liệu SENT relationships")
        
        # Kiểm tra kết quả
        final_result = self.db_manager.run_query(CHECK_GROUND_TRUTH_RESULT_QUERY)
        
        if final_result:
            total = final_result["total"]
            has_ground_truth = final_result["has_ground_truth"]
            fraud_cases = final_result["fraud_cases"]
            
            print(f"\n📊 Thông tin ground truth sau khi chuẩn bị:")
            print(f"  • Tổng số giao dịch: {total}")
            print(f"  • Số giao dịch có ground truth: {has_ground_truth} ({has_ground_truth/total*100:.2f}%)")
            print(f"  • Số giao dịch gian lận: {fraud_cases} ({fraud_cases/has_ground_truth*100:.2f}% trong số có nhãn)")

    def examine_data(self):
        """
        Kiểm tra và xác thực dữ liệu trước khi thực hiện phân tích.
        - Kiểm tra sự phân bố của ground truth data
        - Kiểm tra kiểu dữ liệu của các đặc trưng
        - Kiểm tra sự nhất quán và đầy đủ của dữ liệu
        """
        print("🔍 Đang kiểm tra dữ liệu...")
        
        # 1. Kiểm tra sự tồn tại và phân bố của ground_truth_fraud
        result = self.db_manager.run_query(GROUND_TRUTH_DISTRIBUTION_QUERY)
        
        if result:
            total = result["total"]
            has_ground_truth = result["has_ground_truth"]
            fraud_cases = result["fraud_cases"]
            coverage_ratio = result["coverage_ratio"]
            fraud_ratio = result["fraud_ratio"]
            
            print(f"\n📊 Phân tích ground truth data:")
            print(f"  • Tổng số giao dịch: {total}")
            print(f"  • Số giao dịch có ground truth: {has_ground_truth} ({coverage_ratio*100:.2f}%)")
            print(f"  • Số giao dịch gian lận: {fraud_cases} ({fraud_ratio*100:.2f}%)")
            
            # Cảnh báo nếu tỷ lệ phủ quá thấp
            if coverage_ratio < 0.5:
                print(f"  ⚠️ Cảnh báo: Chỉ {coverage_ratio*100:.2f}% giao dịch có ground truth data.")
            
            # Cảnh báo nếu tỷ lệ gian lận quá cao hoặc quá thấp
            if fraud_ratio < 0.001:
                print(f"  ⚠️ Cảnh báo: Tỷ lệ gian lận quá thấp ({fraud_ratio*100:.4f}%).")
            elif fraud_ratio > 0.2:
                print(f"  ⚠️ Cảnh báo: Tỷ lệ gian lận quá cao ({fraud_ratio*100:.2f}%).")
        else:
            print("  ❌ Không thể truy vấn thông tin ground truth data.")
        
        # 2. Kiểm tra kiểu dữ liệu của ground_truth_fraud
        try:
            # Run this query directly since it uses APOC
            with self.db_manager.driver.session() as session:
                type_check_result = session.run(TYPE_CHECK_QUERY).data()
                
            if type_check_result:
                print("\n📊 Kiểu dữ liệu của ground_truth_fraud:")
                for record in type_check_result:
                    print(f"  • {record['data_type']}: {record['count']} giao dịch")
                    
                    # Cảnh báo nếu có kiểu dữ liệu không phải boolean
                    if record['data_type'] != 'Boolean' and record['data_type'] != 'boolean':
                        print(f"  ⚠️ Cảnh báo: ground_truth_fraud có kiểu dữ liệu {record['data_type']} thay vì Boolean.")
        except Exception as e:
            print(f"  ❌ Không thể kiểm tra kiểu dữ liệu: {str(e)}")
            print("  🔄 Sẽ tiếp tục với giả định ground_truth_fraud là String hoặc Boolean.")
        
        # 3. Kiểm tra phân phối của anomaly score (nếu có)
        score_result = self.db_manager.run_query(SCORE_DISTRIBUTION_QUERY)
        
        if score_result and score_result["count"] > 0:
            print("\n📊 Phân phối của anomaly score:")
            print(f"  • Số giao dịch có anomaly score: {score_result['count']}")
            print(f"  • Giá trị nhỏ nhất: {score_result['min_score']:.6f}")
            print(f"  • Giá trị lớn nhất: {score_result['max_score']:.6f}")
            print(f"  • Giá trị trung bình: {score_result['avg_score']:.6f}")
            print(f"  • Độ lệch chuẩn: {score_result['std_score']:.6f}")
            print(f"  • Giá trị trung vị: {score_result['median_score']:.6f}")
            print(f"  • Phân vị 95%: {score_result['p95_score']:.6f}")
            print(f"  • Phân vị 99%: {score_result['p99_score']:.6f}")
            
            # Cảnh báo nếu phân phối không đều
            if score_result['std_score'] < 0.001:
                print("  ⚠️ Cảnh báo: Phân phối anomaly score quá tập trung (độ lệch chuẩn thấp).")
        
        print("✅ Đã hoàn thành kiểm tra dữ liệu.")
    
    def cleanup_properties_and_relationships(self):
        """Xóa tất cả các thuộc tính được thêm vào trong quá trình phân tích để tránh đầy database."""
        print("🔄 Đang dọn dẹp các thuộc tính phân tích...")
        
        # Danh sách các thuộc tính được thêm vào trong quá trình phân tích
        added_properties = [
            'degScore', 'prScore', 'communityId', 'communitySize', 'normCommunitySize',
            'simScore', 'btwScore', 'hubScore', 'authScore', 'coreScore', 'triCount',
            'cycleCount', 'tempBurst', 'tempBurst1h', 'tempBurst24h', 'anomaly_score', 'flagged'
        ]
        
        try:
            # Xóa thuộc tính trên tất cả các node
            self.db_manager.run_query(get_node_cleanup_query(added_properties))
            
            # Xóa thuộc tính trên relationships
            self.db_manager.run_query(RELATIONSHIP_CLEANUP_QUERY)
            print(f"✅ Đã xóa {len(added_properties)} thuộc tính phân tích khỏi database.")
        except Exception as e:
            print(f"❌ Lỗi khi dọn dẹp thuộc tính: {str(e)}")
            
        # Xóa các mối quan hệ SIMILAR (từ Node Similarity)
        try:
            self.db_manager.run_query(DELETE_SIMILAR_RELATIONSHIPS_QUERY)
            print("✅ Đã xóa các mối quan hệ SIMILAR.")
        except Exception as e:
            print(f"❌ Lỗi khi xóa quan hệ SIMILAR: {str(e)}")
    
    def run_pipeline(self, percentile_cutoff=None):
        """
        Chạy toàn bộ pipeline phát hiện bất thường.
        
        Args:
            percentile_cutoff: Ngưỡng phân vị để đánh dấu giao dịch bất thường (mặc định: 0.95)
        
        Returns:
            dict: Metrics đánh giá hiệu suất
        """

        if percentile_cutoff is not None:
            self.percentile_cutoff = percentile_cutoff
            self.anomaly_detector.percentile_cutoff = percentile_cutoff
            
        start_time = time.time()
        
        print("=" * 50)
        print("🚀 Bắt đầu chạy pipeline phát hiện bất thường không giám sát")
        print("=" * 50)
        
        # 1. Chuẩn bị dữ liệu ground truth
        self.prepare_ground_truth()

        # 2. Kiểm tra và sửa lỗi dữ liệu
        self.examine_data()
        
        # 3. Tạo graph projections
        self.db_manager.create_graph_projections()

        # 4. Trích xuất đặc trưng thời gian
        self.feature_extractor.extract_temporal_features()
        
        # 5. Chạy các thuật toán Graph Data Science
        self.graph_algorithms = GraphAlgorithms(
            self.db_manager, 
            self.db_manager.main_graph_name,  # Truyền tên graph từ database_manager sang
            self.db_manager.similarity_graph_name,
            self.db_manager.temporal_graph_name
        )
        self.graph_algorithms.run_algorithms()
        
        # 6. Normalize các đặc trưng
        self.feature_extractor.normalize_features()
        
        # 7. Tính toán anomaly score
        self.anomaly_detector.compute_anomaly_scores()
        
        # 8. Đánh dấu các giao dịch bất thường
        self.anomaly_detector.flag_anomalies(self.percentile_cutoff)
        
        # 9. Đánh giá hiệu suất
        metrics = self.evaluation.evaluate_performance()
        
        # 10. Phân tích tầm quan trọng của các đặc trưng
        feature_importances = self.evaluation.analyze_feature_importance(self.feature_extractor.weights)

        # 11. Xóa các graph projections
        self.db_manager.delete_graph_projections()

        # 12. Dọn dẹp các thuộc tính và mối quan hệ không cần thiết
        cleanup_result = self.db_manager.cleanup_properties()

        end_time = time.time()
        execution_time = end_time - start_time
        
        print("\n⏱️ Thời gian thực thi: {:.2f} giây".format(execution_time))
        print("=" * 50)
        print("✅ Hoàn thành pipeline phát hiện bất thường không giám sát")
        print("=" * 50)
        
        return metrics  
      
    def get_suspicious_accounts(self, threshold=None, min_flagged_tx=1):
        """
        Lấy các tài khoản đáng ngờ dựa trên điểm bất thường và số giao dịch bị đánh dấu.
        
        Args:
            threshold (float, optional): Ngưỡng anomaly_score để lọc tài khoản. 
                                        Nếu None, sẽ dùng r.flagged = true.
            min_flagged_tx (int): Số lượng giao dịch bị đánh dấu tối thiểu.
            
        Returns:
            list: Danh sách các tài khoản đáng ngờ dưới dạng dict.
        """
        print(f"🔍 Đang tìm các tài khoản đáng ngờ...")
        
        # Clear query logic - separate branches for threshold and flagged criteria        
        if threshold is None:
            # Use flagged relationships to identify suspicious accounts
            query = """
            MATCH (a:Account)-[r:SENT]->()
            WHERE r.flagged = true
            WITH a, COUNT(r) AS flagged_tx_count
            WHERE flagged_tx_count >= $min_flagged_tx
            OPTIONAL MATCH (a)-[r2:SENT]->()
            WHERE r2.combined_score IS NOT NULL
            WITH a, flagged_tx_count, AVG(r2.combined_score) AS avg_combined_score 
            RETURN a.id AS account, a.name, flagged_tx_count, 
                a.anomaly_score AS score, avg_combined_score AS combined_score
            ORDER BY avg_combined_score DESC, flagged_tx_count DESC
            LIMIT 50
            """
            params = {"min_flagged_tx": min_flagged_tx}
        else:
            # Use anomaly_score threshold to identify suspicious accounts
            query = """
            MATCH (a:Account)
            WHERE a.anomaly_score >= $threshold
            OPTIONAL MATCH (a)-[r:SENT]->() WHERE r.flagged = true
            WITH a, COUNT(r) AS flagged_tx_count
            RETURN a.id AS account, a.name, flagged_tx_count, 
                a.anomaly_score AS score
            ORDER BY a.anomaly_score DESC, flagged_tx_count DESC
            LIMIT 50
            """
            params = {"threshold": threshold}
        
        # Verify the query with a debug log
        print(f"Executing query with params: {params}")
        
        # Ensure correct result handling
        results = self.db_manager.run_query(query, params)
        
        # Convert to list if it's a single result
        if isinstance(results, dict):
            suspicious_accounts = [results]
        elif isinstance(results, list):
            suspicious_accounts = results
        else:
            suspicious_accounts = []
        
        # If no accounts found, try a direct approach
        if not suspicious_accounts:
            print("⚠️ Không tìm thấy tài khoản đáng ngờ, kiểm tra xem có giao dịch được đánh dấu không...")
            
            # Check if there are any flagged transactions
            check_query = """
            MATCH ()-[r:SENT]->()
            WHERE r.flagged = true
            RETURN COUNT(r) AS flagged_count
            """
            check_result = self.db_manager.run_query(check_query)
            
            if check_result and check_result.get("flagged_count", 0) > 0:
                print(f"✅ Có {check_result['flagged_count']} giao dịch được đánh dấu, nhưng không tài khoản nào thỏa điều kiện.")
            else:
                print("❌ Không có giao dịch nào được đánh dấu. Kiểm tra lại hàm flag_anomalies().")
            
            # Try getting accounts with highest anomaly scores
            alt_query = """
            MATCH (a:Account)
            WHERE a.anomaly_score IS NOT NULL
            RETURN a.id AS account, a.name, 0 AS flagged_tx_count, 
                a.anomaly_score AS score
            ORDER BY a.anomaly_score DESC
            LIMIT 10
            """
            
            alt_results = self.db_manager.run_query(alt_query)
            
            # Handle results appropriately
            if isinstance(alt_results, dict):
                suspicious_accounts = [alt_results]
            elif isinstance(alt_results, list):
                suspicious_accounts = alt_results
            else:
                suspicious_accounts = []
        
        # Display results
        if suspicious_accounts:
            print(f"✅ Tìm thấy {len(suspicious_accounts)} tài khoản đáng ngờ.")
            print("\n📊 Top tài khoản đáng ngờ:")
            print(f"{'ID Tài khoản':<20} {'Tên (nếu có)':<20} {'Giao dịch đánh dấu':<20} {'Điểm bất thường':<15}")
            print("-" * 75)
            
            for acc in suspicious_accounts[:10]:  # Hiển thị top 10
                name = acc.get('name', 'N/A')
                print(f"{acc['account']:<20} {name:<20} {acc['flagged_tx_count']:<20} {acc.get('score', 0):.6f}")
        else:
            print("⚠️ Không tìm thấy tài khoản đáng ngờ thỏa mãn điều kiện.")
        
        return suspicious_accounts
