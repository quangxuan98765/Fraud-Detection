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
        
        # Export anomaly score từ graph về DataFrame để xuất ra file csv
        df = self.db_manager.run_query(EXPORT_ANOMALY_SCORES)
        import pandas as pd
        if df is None:
            print("❌ Không có dữ liệu để export.")
            return

        # Chuyển đổi kết quả sang DataFrame
        if isinstance(df, dict):
            # Chỉ có một hàng dữ liệu
            df = pd.DataFrame([df])
        elif isinstance(df, list):
            # Nhiều hàng dữ liệu
            df = pd.DataFrame(df)
        
        # Xuất ra file CSV
        if not df.empty:
            df.to_csv('anomaly_scores.csv', index=False)
            print(f"✅ Đã xuất {len(df)} giao dịch có anomaly score ra file anomaly_scores.csv")
                
        print("✅ Đã tính toán xong anomaly score.")    
        
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
        
        # DIRECT FIX: Flag top 3% transactions by anomaly score
        direct_flag_query = """
        MATCH ()-[r:SENT]->()
        WITH r, r.anomaly_score AS score
        ORDER BY score DESC
        WITH collect(r) AS ordered_rels, count(r) AS total
        WITH ordered_rels, total, toInteger(total * $percentile) AS cutoff
        UNWIND range(0, cutoff-1) AS idx
        WITH ordered_rels[idx] AS r
        SET r.flagged = true
        RETURN COUNT(r) AS flagged_count
        """
        flag_result = self.db_manager.run_query(direct_flag_query, {"percentile": 1 - self.percentile_cutoff})
        flagged = flag_result.get("flagged_count", 0) if flag_result else 0
        
        # Reset flagged=false for other transactions
        unflag_query = """
        MATCH ()-[r:SENT]->()
        WHERE r.flagged IS NULL OR r.flagged <> true
        SET r.flagged = false
        """
        self.db_manager.run_query(unflag_query)
        
        # Verify flagging worked
        verify_query = """
        MATCH ()-[r:SENT]->()
        WHERE r.flagged = true
        RETURN COUNT(r) AS count
        """
        verify_result = self.db_manager.run_query(verify_query)
        final_count = verify_result.get("count", 0) if verify_result else 0
        
        print(f"✅ Đã đánh dấu {flagged} giao dịch bất thường")
        print(f"✅ Tổng số giao dịch được đánh dấu trong DB: {final_count}")
        
        return flagged