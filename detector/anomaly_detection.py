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
        
        # Export anomaly score từ graph về DataFrame để xuất ra file csv
        # Chuyển anomaly score từ Account sang Transaction
        self.db_manager.run_query(TRANSFER_SCORE_TO_RELATIONSHIP)

        # Lấy transaction_id, anomaly_score, isFraud từ graph để export
        df = self.db_manager.run_query(EXPORT_ANOMALY_SCORES)
        self.df = df
        
        print("✅ Đã tính toán xong anomaly score.")

    def flag_anomalies(self, percentile_cutoff=None):
        """Đánh dấu giao dịch bất thường dựa trên ngưỡng phân vị (percentile)."""
        if percentile_cutoff is not None:
            self.percentile_cutoff = percentile_cutoff
            
        print(f"🔄 Đang đánh dấu các giao dịch bất thường (ngưỡng phân vị: {self.percentile_cutoff*100}%)...")
        
        # Tính giá trị ngưỡng percentile
        result = self.db_manager.run_query(get_flag_anomalies_query(self.percentile_cutoff))
        
        if result:
            threshold = result["threshold"]
            flagged_count = result["flagged_count"]
            
            # Đánh dấu các giao dịch không vượt ngưỡng là không bất thường
            self.db_manager.run_query(DEFAULT_FLAG)
            
            print(f"✅ Đã đánh dấu {flagged_count} giao dịch bất thường (threshold: {threshold:.6f}).")
        else:
            print("⚠️ Không thể tính ngưỡng percentile cho anomaly score.")