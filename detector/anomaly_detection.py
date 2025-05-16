from .utils.config import DEFAULT_PERCENTILE, FEATURE_WEIGHTS
from .database_manager import DatabaseManager
from .queries.anomaly_detection_queries import (
    COMPUTE_ANOMALY_SCORE, 
    TRANSFER_SCORE_TO_RELATIONSHIP,
    get_flag_anomalies_query,
    DEFAULT_FLAG,
    EXPORT_ANOMALY_SCORES
)

class AnomalyDetector:
    def __init__(self, db_manager: DatabaseManager, weights=None, percentile_cutoff=None):
        self.db_manager = db_manager
        self.weights = weights or FEATURE_WEIGHTS
        self.percentile_cutoff = percentile_cutoff or DEFAULT_PERCENTILE
    
    def compute_anomaly_scores(self):
        """Tính điểm bất thường (anomaly score) dựa trên weighted sum."""
        print("🔄 Đang tính toán anomaly score...")
        
        # Tạo weighted sum của tất cả các đặc trưng đã normalize
        self.db_manager.run_query(COMPUTE_ANOMALY_SCORE)
        
        # Kiểm tra xem anomaly_score đã được tính cho Account chưa
        account_check = self.db_manager.run_query("""
            MATCH (a:Account) 
            WHERE a.anomaly_score IS NOT NULL 
            RETURN COUNT(a) AS account_count
        """)
        
        if account_check and account_check.get("account_count", 0) > 0:
            print(f"✅ Đã tính anomaly_score cho {account_check['account_count']} tài khoản")
        else:
            print("⚠️ Không tìm thấy tài khoản nào có anomaly_score!")
            return
        
        # Chuyển anomaly score từ Account sang Transaction
        relationship_result = self.db_manager.run_query("""
            MATCH (a:Account)-[r:SENT]->()
            WHERE a.anomaly_score IS NOT NULL
            SET r.anomaly_score = a.anomaly_score
            RETURN COUNT(r) AS updated_count
        """)
        
        if relationship_result and relationship_result.get("updated_count", 0) > 0:
            print(f"✅ Đã chuyển anomaly_score từ Account sang {relationship_result['updated_count']} giao dịch")
        else:
            print("⚠️ Không thể chuyển anomaly_score sang các giao dịch!")
   
    def flag_anomalies(self, percentile_cutoff=None):
        """Đánh dấu giao dịch bất thường dựa trên ngưỡng phân vị (percentile)."""
        if percentile_cutoff is not None:
            self.percentile_cutoff = percentile_cutoff
            
        print(f"🔄 Đang đánh dấu các giao dịch bất thường (ngưỡng phân vị: {self.percentile_cutoff*100}%)...")
        
        # CRITICAL FIX: First ensure anomaly scores exist and aren't null
        fix_null_scores = """
        MATCH (a:Account)
        WHERE a.anomaly_score IS NULL
        SET a.anomaly_score = 0.0
        """
        self.db_manager.run_query(fix_null_scores)
        
        # Fix any null anomaly scores on relationships
        fix_rel_scores = """
        MATCH (sender:Account)-[r:SENT]->()
        WHERE r.anomaly_score IS NULL
        SET r.anomaly_score = COALESCE(sender.anomaly_score, 0.0)
        RETURN COUNT(r) as fixed_count
        """
        fix_result = self.db_manager.run_query(fix_rel_scores)
        print(f"✅ Fixed {fix_result.get('fixed_count', 0)} relationships with null anomaly scores")
        
        # Get statistics about anomaly scores
        stats_query = """
        MATCH ()-[r:SENT]->()
        RETURN 
            COUNT(r) AS total,
            COUNT(r.anomaly_score) AS with_score,
            MIN(r.anomaly_score) AS min,
            MAX(r.anomaly_score) AS max,
            AVG(r.anomaly_score) AS avg
        """
        stats = self.db_manager.run_query(stats_query)
        print(f"✅ Stats: Total={stats.get('total', 0)}, With Score={stats.get('with_score', 0)}, " +
            f"Min={stats.get('min', 0)}, Max={stats.get('max', 0)}, Avg={stats.get('avg', 0)}")
        
        # Cải tiến: Sử dụng phương pháp kết hợp percentile và biến đổi ngưỡng tương đối
        # Chỉ đánh dấu giao dịch có điểm bất thường cao hơn NHIỀU so với điểm trung bình
        improved_flag_query = """
        MATCH ()-[r:SENT]->()
        WITH AVG(r.anomaly_score) AS avg_score, STDEV(r.anomaly_score) AS std_score, MAX(r.anomaly_score) AS max_score,
             collect(r) AS all_txs, percentileCont(r.anomaly_score, $percentile) AS perc_threshold
             
        // Tính toán ngưỡng thông minh - lấy giá trị cao nhất của các ngưỡng:
        // 1. Ngưỡng phân vị
        // 2. Ngưỡng thống kê (trung bình + 3*độ lệch chuẩn)
        // 3. Ngưỡng tương đối (80% giá trị lớn nhất)
        WITH all_txs, 
             perc_threshold AS percentile_threshold,
             avg_score + 3 * std_score AS statistical_threshold,
             max_score * 0.8 AS relative_threshold,
             CASE 
                WHEN perc_threshold > (avg_score + 3 * std_score) THEN perc_threshold
                WHEN (avg_score + 3 * std_score) > (max_score * 0.8) THEN avg_score + 3 * std_score
                ELSE max_score * 0.8
             END AS smart_threshold
        
        // Reset tất cả flagged về false
        UNWIND all_txs AS r
        SET r.flagged = false
        
        // Đặt flagged=true cho các giao dịch vượt ngưỡng
        WITH collect(r) AS reset_txs, smart_threshold
        MATCH ()-[r:SENT]->()
        WHERE r.anomaly_score >= smart_threshold
        SET r.flagged = true
        
        RETURN COUNT(r) AS flagged_count, smart_threshold AS threshold
        """
        
        flag_result = self.db_manager.run_query(improved_flag_query, {"percentile": self.percentile_cutoff})
        
        if flag_result:
            flagged = flag_result.get("flagged_count", 0)
            threshold = flag_result.get("threshold", 0)
            print(f"✅ Đã đánh dấu {flagged} giao dịch bất thường (ngưỡng điểm: {threshold:.6f})")
        else:
            print("⚠️ Không thể đánh dấu giao dịch bất thường")
            flagged = 0
        
        # Verify flagging worked
        verify_query = """
        MATCH ()-[r:SENT]->()
        WHERE r.flagged = true
        RETURN COUNT(r) AS count
        """
        verify_result = self.db_manager.run_query(verify_query)
        final_count = verify_result.get("count", 0) if verify_result else 0
        
        print(f"✅ Tổng số giao dịch được đánh dấu trong DB: {final_count}")
        
        return flagged