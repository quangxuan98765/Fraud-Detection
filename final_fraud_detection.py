#!/usr/bin/env python3
"""
Final Fraud Detection: Phương pháp kết hợp tối ưu cho phát hiện gian lận
- Cải thiện cả precision và recall bằng cách kết hợp nhiều kỹ thuật
- Phân loại giao dịch theo các mức độ tin cậy khác nhau
- Sử dụng kỹ thuật lọc và tối ưu hóa để giảm false positives
"""

import os
import sys
import time
import argparse
import json
import numpy as np

# Thêm thư mục gốc vào sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from detector.database_manager import DatabaseManager
from detector.fraud_detector import FraudDetector
from detector.utils.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class FinalFraudDetection:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.fraud_detector = FraudDetector(db_manager)
          # Cấu hình hệ thống tối ưu cho dataset với tỷ lệ fraud 1.38%        
        self.config = {
            "percentile_cutoff": 0.99,
            "thresholds": {
                "very_high_anomaly": 0.165 * 1.02,    # 99th percentile với điều chỉnh
                "high_anomaly": 0.155 * 1.01,         # 97.5th percentile với điều chỉnh
                "medium_anomaly": 0.149,              # 95th percentile giữ nguyên
                "low_anomaly": 0.147 * 0.98,          # 90th percentile giảm nhẹ cho recall tốt hơn
                "amount_high": 2095000,               # ~ 99th percentile amount từ dataset mới
                "amount_medium": 389600,              # ~ 90th percentile amount từ dataset mới
            },            
            "feature_weights": {
                "degScore": 0.38,                # Giữ nguyên - hiệu suất tốt
                "hubScore": 0.22,                # Tăng thêm - có hiệu quả cao trong phát hiện fraud
                "normCommunitySize": 0.18,       # Tăng thêm - có tương quan mạnh với fraud
                "amountVolatility": 0.06,        # Giữ nguyên
                "txVelocity": 0.06,              # Giữ nguyên
                "btwScore": 0.04,                # Giảm nhẹ
                "prScore": 0.03,                 # Giảm thêm
                "authScore": 0.03                # Giảm thêm
            },            
            "confidence_levels": {
                "very_high": 0.96,   # Giữ nguyên - đã tối ưu cho precision
                "high": 0.84,        # Giảm nhẹ để phù hợp với tỷ lệ gian lận cao hơn
                "medium": 0.72,      # Giảm nhẹ để phù hợp với tỷ lệ gian lận cao hơn
                "low": 0.56          # Giảm thêm để cải thiện recall
            }
        }
    
    def run_detection(self, skip_basic_detection=False, balance_mode="balanced"):
        """
        Chạy phát hiện gian lận tối ưu.
        
        Args:
            skip_basic_detection: Bỏ qua bước phát hiện bất thường cơ bản nếu True
            balance_mode: Chế độ cân bằng giữa precision và recall
                          - "precision": Ưu tiên precision cao (ít false positives)
                          - "recall": Ưu tiên recall cao (ít false negatives)
                          - "balanced": Cân bằng giữa precision và recall
        """
        print(f"🔍 FINAL FRAUD DETECTION - Chế độ: {balance_mode}")
        
        start_time = time.time()
        
        # 1. Chạy phát hiện bất thường cơ bản nếu cần
        if not skip_basic_detection:
            print("\n🔄 Bước 1: Chạy phát hiện bất thường cơ bản...")
            
            # Áp dụng trọng số đã tối ưu
            self.fraud_detector.feature_extractor.weights = self.config["feature_weights"]
              # Chạy pipeline với ngưỡng phân vị cấu hình
            self.fraud_detector.run_pipeline(
                percentile_cutoff=self.config["percentile_cutoff"]
            )
        else:
            print("\n⏩ Bỏ qua Bước 1: Sử dụng kết quả phát hiện bất thường hiện có...")
            # Kiểm tra nếu anomaly_score đã tồn tại
            check_query = """
            MATCH ()-[tx:SENT]->()
            WHERE tx.anomaly_score IS NOT NULL
            RETURN COUNT(tx) AS count
            """
            result = self.db_manager.run_query(check_query)
            if result and result.get("count", 0) > 0:
                print(f"  ✅ Đã tìm thấy {result.get('count', 0)} giao dịch có anomaly_score")
            else:
                print("  ⚠️ Không tìm thấy giao dịch nào có anomaly_score. Cần chạy bước 1.")
                return
        
        # 2. Lấy thông tin thống kê
        print("\n🔄 Bước 2: Phân tích thống kê giao dịch...")
        stats = self._calculate_statistics()
        
        # 3. Reset trạng thái đánh dấu
        self._reset_flags()
        
        # 4. Phát hiện gian lận theo cấp độ độ tin cậy
        print("\n🔄 Bước 3: Phát hiện gian lận theo độ tin cậy...")
        
        # 4.1. Phát hiện gian lận độ tin cậy rất cao
        self._detect_very_high_confidence_fraud(stats)
        
        # 4.2. Phát hiện gian lận độ tin cậy cao
        self._detect_high_confidence_fraud(stats)
        
        # 4.3. Phát hiện gian lận độ tin cậy trung bình
        self._detect_medium_confidence_fraud(stats)
        
        # 4.4. Phát hiện gian lận độ tin cậy thấp (chỉ áp dụng cho chế độ ưu tiên recall)
        if balance_mode == "recall":
            self._detect_low_confidence_fraud(stats)
        
        # 5. Phát hiện gian lận dựa trên mối quan hệ
        print("\n🔄 Bước 4: Phát hiện gian lận liên quan...")
        self._detect_related_fraud(stats)
          
        # 6. Lọc false positives nếu ưu tiên precision hoặc lọc các trường hợp rõ ràng cho chế độ recall
        if balance_mode == "precision":
            print("\n🔄 Bước 5: Lọc các false positives (precision-focused)...")
            self._filter_false_positives(stats)
        elif balance_mode == "recall":
            print("\n🔄 Bước 5: Lọc các false positives cơ bản (recall-focused)...")
            self._filter_basic_false_positives(stats)
            self._apply_statistical_refinement(stats)
        
        # 7. Đánh giá kết quả
        print("\n🔄 Bước 6: Đánh giá kết quả phát hiện...")
        metrics = self._evaluate_results(balance_mode)
        
        # 8. Phân tích chi tiết các giao dịch gian lận
        print("\n🔄 Bước 7: Phân tích chi tiết giao dịch gian lận...")
        self._analyze_fraud_details()
        
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"\n⏱️ Thời gian thực thi: {execution_time:.2f} giây")
        
        return metrics
    
    def _calculate_statistics(self):
        """Tính toán các thống kê cần thiết cho phát hiện."""
        
        stats_query = """
        MATCH ()-[tx:SENT]->()
        WITH 
            percentileCont(tx.anomaly_score, 0.99) AS very_high_threshold,
            percentileCont(tx.anomaly_score, 0.975) AS high_threshold,
            percentileCont(tx.anomaly_score, 0.95) AS medium_threshold,
            percentileCont(tx.anomaly_score, 0.90) AS low_threshold,
            percentileCont(tx.amount, 0.99) AS high_amount,
            percentileCont(tx.amount, 0.90) AS medium_amount,
            AVG(tx.amount) as avg_amount,
            STDEV(tx.amount) as std_amount,
            COUNT(*) as total_transactions,
            SUM(CASE WHEN tx.ground_truth_fraud = true THEN 1 ELSE 0 END) as fraud_count
        RETURN 
            very_high_threshold,
            high_threshold,
            medium_threshold, 
            low_threshold,
            high_amount,
            medium_amount,
            avg_amount,
            std_amount,
            total_transactions,
            fraud_count
        """
        
        result = self.db_manager.run_query(stats_query)
        
        if not result:
            print("⚠️ Không thể tính toán thống kê giao dịch")
            return {}
        
        # Cập nhật thông số cấu hình dựa trên dữ liệu
        self.config["thresholds"]["very_high_anomaly"] = result["very_high_threshold"]
        self.config["thresholds"]["high_anomaly"] = result["high_threshold"]
        self.config["thresholds"]["medium_anomaly"] = result["medium_threshold"]
        self.config["thresholds"]["low_anomaly"] = result["low_threshold"]
        self.config["thresholds"]["amount_high"] = result["high_amount"]
        self.config["thresholds"]["amount_medium"] = result["medium_amount"]
        
        # Hiển thị thông tin thống kê
        print(f"📊 Thông tin thống kê giao dịch:")
        print(f"  • Tổng số giao dịch: {result.get('total_transactions', 0)}")
        print(f"  • Tổng số giao dịch gian lận: {result.get('fraud_count', 0)}")
        print(f"  • Tỷ lệ gian lận: {result.get('fraud_count', 0) / result.get('total_transactions', 1) * 100:.4f}%")
        print(f"  • Ngưỡng anomaly score rất cao (99%): {result.get('very_high_threshold', 0):.6f}")
        print(f"  • Ngưỡng anomaly score cao (97.5%): {result.get('high_threshold', 0):.6f}")
        print(f"  • Ngưỡng anomaly score trung bình (95%): {result.get('medium_threshold', 0):.6f}")
        print(f"  • Ngưỡng anomaly score thấp (90%): {result.get('low_threshold', 0):.6f}")
        print(f"  • Giá trị giao dịch trung bình: {result.get('avg_amount', 0):.2f}")
        print(f"  • Ngưỡng giá trị cao (99%): {result.get('high_amount', 0):.2f}")
        print(f"  • Ngưỡng giá trị trung bình (90%): {result.get('medium_amount', 0):.2f}")
        
        return result
    
    def _reset_flags(self):
        """Reset trạng thái đánh dấu."""
        print("  - Đang reset trạng thái đánh dấu...")
        
        reset_query = """
        MATCH ()-[tx:SENT]->()
        SET tx.flagged = false, 
            tx.confidence = null, 
            tx.flag_reason = null
            
        WITH count(tx) AS reset_count
        
        // Reset account flags
        MATCH (acc:Account)
        SET acc.suspicious = null,
            acc.suspicious_count = null,
            acc.fraud_risk = null
            
        RETURN reset_count
        """
        reset_result = self.db_manager.run_query(reset_query)
        
        # Handle both list and dictionary results
        reset_count = 0
        if reset_result:
            if isinstance(reset_result, dict):
                reset_count = reset_result.get("reset_count", 0)
            elif isinstance(reset_result, list) and len(reset_result) > 0:
                reset_count = reset_result[0].get("reset_count", 0) if isinstance(reset_result[0], dict) else 0
                
        print(f"    ✅ Đã reset {reset_count} giao dịch và các tài khoản liên quan")
        
    def _apply_statistical_refinement(self, stats):
        """Áp dụng phân tích thống kê nâng cao để tối ưu kết quả chế độ recall."""
        print("  - Tiến hành phân tích hiệu quả phát hiện và lọc thêm...")
        
        # Lọc dựa trên anomaly score thấp và các đặc trưng khác không nổi bật
        basic_filter_query = """
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE 
            tx.flagged = true AND
            tx.detection_rule IS NOT NULL AND
            tx.anomaly_score < $medium_threshold * 0.85 AND
            tx.amount < $avg_amount * 1.5 AND
            (src.hubScore IS NULL OR src.hubScore < 0.5) AND
            (src.degScore IS NULL OR src.degScore < 0.5)
        
        SET tx.flagged = false,
            tx.filtered = true, 
            tx.filter_reason = "Statistical refinement - low scores"
            
        RETURN count(tx) as filtered_count
        """
        
        params = {
            "medium_threshold": self.config["thresholds"]["medium_anomaly"],
            "avg_amount": stats["avg_amount"]
        }
        
        result = self.db_manager.run_query(basic_filter_query, params)
        filtered_count = result.get("filtered_count", 0) if result else 0
        print(f"    ✅ Đã lọc bỏ {filtered_count} giao dịch dựa trên điểm số thấp")
        
        # Lọc dựa trên mẫu hoạt động bình thường
        normal_pattern_query = """
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE 
            tx.flagged = true AND
            // Giao dịch có mẫu hoạt động bình thường
            tx.amount <= $avg_amount * 0.8 AND
            tx.anomaly_score < $medium_threshold * 0.9 AND
            src.txCount > 5 // Tài khoản có nhiều giao dịch lịch sử
        
        SET tx.flagged = false,
            tx.filtered = true,
            tx.filter_reason = "Normal activity pattern"
            
        RETURN count(tx) as filtered_count
        """
        
        result = self.db_manager.run_query(normal_pattern_query, params)
        filtered_count = result.get("filtered_count", 0) if result else 0
        print(f"    ✅ Đã lọc bỏ {filtered_count} giao dịch có mẫu hoạt động bình thường")
        
        # Lọc dựa trên thời gian giao dịch        
        time_pattern_query = """
        MATCH (src:Account)-[tx:SENT]->(dest:Account)        WHERE 
            tx.flagged = true AND
            src.stdTimeBetweenTx IS NOT NULL AND
            src.stdTimeBetweenTx < 0.25 AND
            tx.anomaly_score < $medium_threshold
        
        SET tx.flagged = false,
            tx.filtered = true,
            tx.filter_reason = "Normal time pattern"
            
        RETURN count(tx) as filtered_count
        """
        
        result = self.db_manager.run_query(time_pattern_query, params)
        filtered_count = result.get("filtered_count", 0) if result else 0
        print(f"    ✅ Đã lọc bỏ {filtered_count} giao dịch có mẫu thời gian bình thường")
    
    def _detect_very_high_confidence_fraud(self, stats):
        """Phát hiện gian lận với độ tin cậy rất cao."""
        print("  - Phát hiện gian lận với độ tin cậy rất cao...")
        very_high_query = """
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE            // Điểm anomaly cực cao - đã chứng minh precision tốt nhất (29.33%)
            tx.anomaly_score >= $very_high_threshold * 1.08
            
            // HOẶC điểm anomaly cao (ngưỡng 99%) KẾT HỢP với cấu trúc đồ thị đáng ngờ cao
            OR (tx.anomaly_score >= $very_high_threshold AND 
                (
                    // Là hub node có kết nối cao
                    (src.hubScore IS NOT NULL AND src.hubScore >= 0.85) OR
                      // Nằm trong cộng đồng nhỏ đáng ngờ - cải thiện cho dataset mới
                    (src.normCommunitySize IS NOT NULL AND src.normCommunitySize <= 0.04)
                )
            )
            
            // HOẶC điểm anomaly cao (ngưỡng 99%) KẾT HỢP với giá trị giao dịch rất cao
            OR (tx.anomaly_score >= $very_high_threshold AND tx.amount >= $amount_high * 1.3)
              SET tx.flagged = true,
            tx.confidence = $very_high_confidence,
            tx.flag_reason = CASE
                WHEN tx.anomaly_score >= $very_high_threshold * 1.08 THEN "Điểm anomaly cực cao"
                WHEN tx.amount >= $amount_high * 1.2 THEN "Điểm anomaly cao + giá trị giao dịch rất cao"
                ELSE "Điểm anomaly cao + cấu trúc đồ thị rất đáng ngờ"
            END,
            tx.detection_rule = "very_high_confidence"
            
        RETURN count(tx) as flagged_count
        """
        
        params = {
            "very_high_threshold": self.config["thresholds"]["very_high_anomaly"],
            "high_threshold": self.config["thresholds"]["high_anomaly"],
            "amount_high": self.config["thresholds"]["amount_high"],
            "very_high_confidence": self.config["confidence_levels"]["very_high"]
        }
        
        result = self.db_manager.run_query(very_high_query, params)
        flagged_count = result.get("flagged_count", 0) if result else 0
        print(f"    ✅ Đã đánh dấu {flagged_count} giao dịch có độ tin cậy rất cao")
    
    def _detect_high_confidence_fraud(self, stats):
        """Phát hiện gian lận với độ tin cậy cao."""
        print("  - Phát hiện gian lận với độ tin cậy cao...")
        
        high_confidence_query = """
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE tx.flagged = false AND
        (
            // Điểm anomaly cao (ngưỡng 97.5%)
            tx.anomaly_score >= $high_threshold
            
            // HOẶC điểm anomaly trung bình (ngưỡng 95%) KẾT HỢP với cấu trúc đồ thị đáng ngờ
            OR (tx.anomaly_score >= $medium_threshold AND 
                (
                    // Chỉ số trung tâm cao
                    (src.hubScore IS NOT NULL AND src.hubScore >= 0.7) OR
                    (src.degScore IS NOT NULL AND src.degScore >= 0.7) OR
                    
                    // Nằm trong cộng đồng nhỏ bất thường
                    (src.normCommunitySize IS NOT NULL AND src.normCommunitySize <= 0.15) OR
                    
                    // Hoạt động bất thường về thời gian
                    (src.tempBurst IS NOT NULL AND src.tempBurst >= 0.7)
                )
            )
            
            // HOẶC điểm anomaly trung bình KẾT HỢP với giá trị giao dịch cao
            OR (tx.anomaly_score >= $medium_threshold AND tx.amount >= $amount_medium * 1.5)
        )
        
        SET tx.flagged = true,
            tx.confidence = $high_confidence,
            tx.flag_reason = CASE
                WHEN tx.anomaly_score >= $high_threshold THEN "Điểm anomaly cao"
                WHEN tx.amount >= $amount_medium * 1.5 THEN "Điểm anomaly trung bình + giá trị giao dịch cao"
                ELSE "Điểm anomaly trung bình + cấu trúc đồ thị đáng ngờ"
            END,
            // Thêm nhãn để phân tích
            tx.detection_rule = "high_confidence"
            
        RETURN count(tx) as flagged_count
        """
        
        params = {
            "high_threshold": self.config["thresholds"]["high_anomaly"],
            "medium_threshold": self.config["thresholds"]["medium_anomaly"],
            "amount_medium": self.config["thresholds"]["amount_medium"],
            "high_confidence": self.config["confidence_levels"]["high"]
        }
        
        result = self.db_manager.run_query(high_confidence_query, params)
        flagged_count = result.get("flagged_count", 0) if result else 0
        print(f"    ✅ Đã đánh dấu {flagged_count} giao dịch có độ tin cậy cao")
    
    def _detect_medium_confidence_fraud(self, stats):
        """Phát hiện gian lận với độ tin cậy trung bình."""
        print("  - Phát hiện gian lận với độ tin cậy trung bình...")
        
        medium_confidence_query = """
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE tx.flagged = false AND
        (
            // Điểm anomaly trung bình 
            tx.anomaly_score >= $medium_threshold
            
            // HOẶC điểm anomaly thấp KẾT HỢP với các mẫu đáng ngờ
            OR (tx.anomaly_score >= $low_threshold AND 
                (
                    // Giá trị giao dịch cao hơn trung bình
                    tx.amount >= $avg_amount * 2 OR
                    
                    // Chỉ số đồ thị đáng ngờ
                    (src.hubScore IS NOT NULL AND src.hubScore >= 0.6) OR
                    (src.degScore IS NOT NULL AND src.degScore >= 0.6) OR
                    
                    // Vận tốc giao dịch cao
                    (src.txVelocity IS NOT NULL AND src.txVelocity >= 0.7)
                )
            )
        )
        
        SET tx.flagged = true,
            tx.confidence = $medium_confidence,
            tx.flag_reason = CASE
                WHEN tx.anomaly_score >= $medium_threshold THEN "Điểm anomaly trung bình"
                WHEN tx.amount >= $avg_amount * 2 THEN "Điểm anomaly thấp + giá trị giao dịch cao"
                WHEN src.txVelocity IS NOT NULL AND src.txVelocity >= 0.7 THEN "Điểm anomaly thấp + vận tốc giao dịch cao"
                ELSE "Điểm anomaly thấp + chỉ số đồ thị đáng ngờ"
            END,
            // Thêm nhãn để phân tích
            tx.detection_rule = "medium_confidence"
            
        RETURN count(tx) as flagged_count
        """
        
        params = {
            "medium_threshold": self.config["thresholds"]["medium_anomaly"],
            "low_threshold": self.config["thresholds"]["low_anomaly"],
            "avg_amount": stats["avg_amount"],
            "medium_confidence": self.config["confidence_levels"]["medium"]
        }
        
        result = self.db_manager.run_query(medium_confidence_query, params)
        flagged_count = result.get("flagged_count", 0) if result else 0
        print(f"    ✅ Đã đánh dấu {flagged_count} giao dịch có độ tin cậy trung bình")
    
    def _detect_low_confidence_fraud(self, stats):
        """Phát hiện gian lận với độ tin cậy thấp - sửa đổi để giảm false positives."""
        print("  - Phát hiện gian lận với độ tin cậy thấp (recall-focused)...")
        
        low_confidence_query = """
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE tx.flagged = false AND
        (
            // Kết hợp nhiều yếu tố bất thường thay vì chỉ dựa vào điểm anomaly
            (tx.anomaly_score >= $medium_threshold * 0.95 AND 
                (
                    // Giá trị giao dịch cao
                    tx.amount >= $avg_amount * 3 OR
                    
                    // Kết hợp chỉ số đồ thị và thời gian bất thường
                    (
                        (src.hubScore IS NOT NULL AND src.hubScore >= 0.5) AND
                        (src.txVelocity IS NOT NULL AND src.txVelocity >= 0.5)
                    ) OR
                    
                    // Cấu trúc cộng đồng đáng ngờ
                    (
                        (src.normCommunitySize IS NOT NULL AND src.normCommunitySize <= 0.15) AND
                        tx.amount >= $avg_amount * 1.5
                    )
                )
            ) OR
            
            // Giao dịch có giá trị rất cao kết hợp với anomaly score không quá thấp
            (tx.amount >= $avg_amount * 8 AND tx.anomaly_score >= $low_threshold * 0.9) OR
            
            // Hoạt động giao dịch vô cùng bất thường
            (tx.anomaly_score >= $medium_threshold * 0.9 AND src.txVelocity IS NOT NULL AND src.txVelocity >= 0.8)
        )
        
        SET tx.flagged = true,
            tx.confidence = $low_confidence,
            tx.flag_reason = CASE
                WHEN tx.amount >= $avg_amount * 8 THEN "Giá trị giao dịch cực cao"
                WHEN src.txVelocity IS NOT NULL AND src.txVelocity >= 0.8 THEN "Vận tốc giao dịch cực cao"
                ELSE "Kết hợp nhiều yếu tố đáng ngờ - độ tin cậy thấp"
            END,
            tx.detection_rule = "low_confidence"
            
        RETURN count(tx) as flagged_count
        """
        
        params = {
            "low_threshold": self.config["thresholds"]["low_anomaly"],
            "medium_threshold": self.config["thresholds"]["medium_anomaly"],
            "avg_amount": stats["avg_amount"],
            "low_confidence": self.config["confidence_levels"]["low"]
        }
        
        result = self.db_manager.run_query(low_confidence_query, params)
        flagged_count = result.get("flagged_count", 0) if result else 0
        print(f"    ✅ Đã đánh dấu {flagged_count} giao dịch có độ tin cậy thấp")
    
    def _detect_related_fraud(self, stats):
        """Phát hiện gian lận dựa trên mối quan hệ với các giao dịch gian lận đã biết."""
        print("  - Phát hiện gian lận dựa trên mối quan hệ...")
        
        # 1. Đánh dấu tài khoản đáng ngờ dựa trên giao dịch gian lận
        suspicious_account_query = """
        // Đánh dấu tài khoản liên quan đến giao dịch gian lận
        MATCH (acc:Account)-[tx:SENT]->() 
        WHERE tx.flagged = true AND tx.confidence >= $high_confidence
        
        WITH acc, count(tx) AS fraud_count
        
        SET acc.suspicious = true,
            acc.suspicious_count = fraud_count,
            acc.fraud_risk = CASE
                WHEN fraud_count >= 3 THEN "high"
                WHEN fraud_count = 2 THEN "medium"
                ELSE "low"
            END
            
        RETURN count(acc) as suspicious_accounts
        """
        
        params = {
            "high_confidence": self.config["confidence_levels"]["high"]
        }
        
        result = self.db_manager.run_query(suspicious_account_query, params)
        suspicious_accounts = result.get("suspicious_accounts", 0) if result else 0
        print(f"    ✅ Đã đánh dấu {suspicious_accounts} tài khoản đáng ngờ")
        
        # 2. Tìm giao dịch liên quan đến tài khoản đáng ngờ
        related_tx_query = """
        // Tìm giao dịch từ hoặc đến tài khoản đáng ngờ
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE 
            tx.flagged = false AND
            (
                // Từ tài khoản đáng ngờ HIGH có giá trị cao
                (src.suspicious = true AND src.fraud_risk = "high" AND 
                tx.anomaly_score >= $low_threshold * 0.8 AND
                tx.amount >= $avg_amount) OR
                
                // Đến tài khoản đáng ngờ HIGH có giá trị cao
                (dest.suspicious = true AND dest.fraud_risk = "high" AND 
                tx.anomaly_score >= $low_threshold * 0.8 AND
                tx.amount >= $avg_amount) OR
                
                // Giao dịch giữa hai tài khoản đáng ngờ
                (src.suspicious = true AND dest.suspicious = true AND
                tx.anomaly_score >= $low_threshold * 0.7)
            )
        
        SET tx.flagged = true,
            tx.confidence = CASE
                WHEN src.fraud_risk = "high" OR dest.fraud_risk = "high" THEN 0.8
                ELSE 0.7
            END,
            tx.flag_reason = CASE
                WHEN src.suspicious = true AND dest.suspicious = true THEN "Giao dịch giữa hai tài khoản đáng ngờ"
                WHEN src.suspicious = true THEN "Giao dịch từ tài khoản đáng ngờ"
                ELSE "Giao dịch đến tài khoản đáng ngờ"
            END,
            tx.detection_rule = "related_fraud"
            
        RETURN count(tx) as flagged_count
        """
        
        params = {
            "low_threshold": self.config["thresholds"]["low_anomaly"],
            "avg_amount": stats["avg_amount"]
        }
        
        result = self.db_manager.run_query(related_tx_query, params)        
        flagged_count = result.get("flagged_count", 0) if result else 0
        print(f"    ✅ Đã đánh dấu {flagged_count} giao dịch liên quan đến tài khoản đáng ngờ")
    
    def _filter_false_positives(self, stats):
        """Lọc các false positives cho chế độ precision cao."""
        print("  - Lọc các giao dịch có thể là false positives...")
        
        filter_query = """
        // Hủy đánh dấu các giao dịch có khả năng cao là false positive
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE 
            tx.flagged = true AND
            (
                // Giao dịch có độ tin cậy thấp với các chỉ số bình thường
                (tx.confidence <= 0.72 AND
                    (
                        // Giao dịch có giá trị bình thường
                        (tx.amount <= $avg_amount * 1.2 AND tx.anomaly_score <= $medium_threshold) OR
                        
                        // Mẫu giao dịch bình thường
                        (src.txVelocity IS NOT NULL AND src.txVelocity <= 0.3 AND tx.anomaly_score <= $medium_threshold) OR
                        
                        // Cấu trúc đồ thị bình thường
                        (tx.detection_rule = "medium_confidence" AND
                         tx.anomaly_score <= $medium_threshold * 0.98 AND
                         (src.normCommunitySize IS NULL OR src.normCommunitySize >= 0.3))
                    )
                ) OR
                
                // Giao dịch đã được lọc trong quá trình phân tích
                (tx.confidence <= 0.8 AND
                    (
                        // Các chỉ số đồ thị không đủ cao
                        ((src.hubScore IS NULL OR src.hubScore < 0.5) AND
                         (src.degScore IS NULL OR src.degScore < 0.5) AND
                         tx.anomaly_score < $high_threshold * 0.95 AND
                         tx.amount < $amount_high * 0.5)
                    )
                )
            )
        
        SET tx.flagged = false,
            tx.filtered = true,
            tx.filter_reason = "Lọc bỏ false positive tiềm năng"
            
        RETURN count(tx) as filtered_count
        """
        params = {
            "avg_amount": stats["avg_amount"],
            "medium_threshold": self.config["thresholds"]["medium_anomaly"],
            "high_threshold": self.config["thresholds"]["high_anomaly"],
            "low_threshold": self.config["thresholds"]["low_anomaly"],
            "amount_high": self.config["thresholds"]["amount_high"]
        }
        
        result = self.db_manager.run_query(filter_query, params)
        filtered_count = result.get("filtered_count", 0) if result else 0
        print(f"    ✅ Đã lọc bỏ {filtered_count} false positives tiềm năng")
    
    def _filter_basic_false_positives(self, stats):
        """Lọc false positives mạnh mẽ cho chế độ recall."""
        print("  - Lọc false positives mạnh mẽ cho chế độ recall...")
        
        filter_query = """
        // Lọc các giao dịch có nhiều khả năng là false positive
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE 
            tx.flagged = true AND
            (
                // Các giao dịch nhỏ với anomaly score thấp
                (tx.amount <= $avg_amount * 0.5 AND tx.anomaly_score <= $medium_threshold) OR
                
                // Giao dịch với các tài khoản có hoạt động bình thường
                (
                    tx.confidence < 0.8 AND 
                    tx.anomaly_score < $medium_threshold AND
                    (src.txVelocity IS NULL OR src.txVelocity < 0.3) AND
                    (src.hubScore IS NULL OR src.hubScore < 0.3) AND
                    tx.amount < $avg_amount
                )
            )
        
        SET tx.flagged = false,
            tx.filtered = true,
            tx.filter_reason = "Enhanced recall mode filter"
            
        RETURN count(tx) as filtered_count
        """
        
        params = {
            "avg_amount": stats["avg_amount"],
            "medium_threshold": self.config["thresholds"]["medium_anomaly"]
        }
        
        result = self.db_manager.run_query(filter_query, params)
        filtered_count = result.get("filtered_count", 0) if result else 0
        print(f"    ✅ Đã lọc bỏ {filtered_count} false positives trong chế độ recall")
        
        # Lọc giao dịch dựa trên điểm tổng hợp
        secondary_filter_query = """
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE 
            tx.flagged = true AND
            (
                // Giao dịch không thực sự đáng ngờ dựa trên điểm tổng hợp
                (
                    // Điểm anomaly không cao
                    tx.anomaly_score < $medium_threshold AND
                    
                    // Và không có chỉ số đặc biệt nào khác nổi trội
                    (src.hubScore IS NULL OR src.hubScore < 0.4) AND
                    (src.degScore IS NULL OR src.degScore < 0.4) AND
                    (tx.amount < $avg_amount * 1.2)
                )
            )
        
        SET tx.flagged = false,
            tx.filtered = true,
            tx.filter_reason = "Combined features filter"
            
        RETURN count(tx) as filtered_count
        """
        
        result = self.db_manager.run_query(secondary_filter_query, params)
        filtered_count = result.get("filtered_count", 0) if result else 0
        print(f"    ✅ Đã lọc bỏ thêm {filtered_count} false positives thông qua đánh giá tổng hợp")
    
    def _parse_record(self, record):
        """Parse record from Neo4j result."""
        try:
            # Trường hợp record là một đối tượng Neo4j Record
            if hasattr(record, "keys") and callable(getattr(record, "keys")):
                # Chuyển đổi Record thành dictionary
                return {key: record[key] for key in record.keys()}
            
            # Trường hợp đã là dictionary
            if isinstance(record, dict):
                return record
            
            # Trường hợp record là một đối tượng có thể truy cập theo tên thuộc tính
            if hasattr(record, "get") and callable(getattr(record, "get")):
                # Nếu có phương thức get(), giả định nó hoạt động như dict
                return record
                
            # Trường hợp record có thuộc tính đặc biệt
            if hasattr(record, "_properties"):
                # Một số đối tượng Neo4j node/relationship có thuộc tính _properties
                return record._properties
                
            # Ghi log và trả về dictionary rỗng nếu không xử lý được
            print(f"  ⚠️ Không thể phân tích dữ liệu kiểu: {type(record)}")
            return {}
            
        except Exception as e:
            print(f"  ⚠️ Lỗi khi phân tích bản ghi: {str(e)}")
            return {}
    
    def _evaluate_results(self, mode="balanced"):
        """Đánh giá kết quả phát hiện."""
        
        evaluation_query = """
        MATCH ()-[tx:SENT]->()
        WITH
            SUM(CASE WHEN tx.flagged = true AND tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS true_positives,
            SUM(CASE WHEN tx.flagged = true AND tx.ground_truth_fraud = false THEN 1 ELSE 0 END) AS false_positives,
            SUM(CASE WHEN tx.flagged = false AND tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS false_negatives,
            SUM(CASE WHEN tx.flagged = false AND tx.ground_truth_fraud = false THEN 1 ELSE 0 END) AS true_negatives,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS total_fraud

        // Tính precision và recall
        WITH
            true_positives, false_positives, false_negatives, true_negatives, 
            total_transactions, total_fraud,
            CASE WHEN (true_positives + false_positives) > 0 
                THEN toFloat(true_positives) / (true_positives + false_positives) 
                ELSE 0 
            END AS precision,
            CASE WHEN (true_positives + false_negatives) > 0 
                THEN toFloat(true_positives) / (true_positives + false_negatives) 
                ELSE 0 
            END AS recall

        // Tính F1 score và accuracy
        WITH 
            true_positives, false_positives, false_negatives, true_negatives,
            total_transactions, total_fraud, precision, recall,
            CASE 
                WHEN (precision + recall) > 0 
                THEN 2 * precision * recall / (precision + recall) 
                ELSE 0 
            END AS f1_score,
            toFloat(true_positives + true_negatives) / total_transactions AS accuracy
            
        RETURN *
        """
        
        result = self.db_manager.run_query(evaluation_query)
        
        if not result:
            print("⚠️ Không thể đánh giá hiệu suất")
            return None
        
        # Hiển thị kết quả
        print("\n📊 Kết quả đánh giá hiệu suất:")
        print(f"  • Tổng số giao dịch: {result['total_transactions']}")
        print(f"  • Tổng số giao dịch gian lận thực tế: {result['total_fraud']}")
        print(f"  • Số giao dịch được đánh dấu: {result['true_positives'] + result['false_positives']}")
        print(f"  • True Positives: {result['true_positives']}")
        print(f"  • False Positives: {result['false_positives']}")
        print(f"  • False Negatives: {result['false_negatives']}")
        print(f"  • True Negatives: {result['true_negatives']}")
        print(f"  • Precision: {result['precision']:.4f}")
        print(f"  • Recall: {result['recall']:.4f}")
        print(f"  • F1 Score: {result['f1_score']:.4f}")
        print(f"  • Accuracy: {result['accuracy']:.4f}")    
        
        # Thông tin chi tiết phát hiện theo độ tin cậy        # Phân tích theo độ tin cậy - Simplified Version        # Phân tích theo độ tin cậy
        print("\n📊 Phân tích theo độ tin cậy:")
        
        confidence_query = """
        MATCH ()-[tx:SENT]->()
        WHERE tx.flagged = true
        WITH tx.confidence as confidence_level, 
             COUNT(tx) as flagged_count,
             SUM(CASE WHEN tx.ground_truth_fraud = true THEN 1 ELSE 0 END) as true_fraud
        RETURN 
            confidence_level,
            flagged_count,
            true_fraud,
            toFloat(true_fraud) / flagged_count as precision_rate
        ORDER BY confidence_level DESC
        """
        
        confidence_result = self.db_manager.run_query(confidence_query)
        if confidence_result:
            try:
                processed_items = []
                
                # Process results based on returned data structure
                if isinstance(confidence_result, list):
                    processed_items = confidence_result
                elif isinstance(confidence_result, dict):
                    processed_items = [confidence_result]
                    
                # Display results
                for item in processed_items:
                    confidence_level = item.get("confidence_level", 0)
                    flagged_count = item.get("flagged_count", 0)
                    true_fraud = item.get("true_fraud", 0)
                    precision_rate = item.get("precision_rate", 0)
                    
                    print(f"  • Độ tin cậy {confidence_level:.2f}: {flagged_count} giao dịch, {true_fraud} gian lận thực sự, precision {precision_rate:.4f}")
            except Exception as e:
                print(f"  ⚠️ Không thể phân tích kết quả độ tin cậy: {str(e)}")
        else:
            print("  ⚠️ Không tìm thấy dữ liệu phân tích độ tin cậy")
        
        # Lưu metrics
        metrics = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "model": f"final_fraud_detection_{mode}",
            "parameters": self.config,
            "metrics": {
                "true_positives": result['true_positives'],
                "false_positives": result['false_positives'],
                "false_negatives": result['false_negatives'],
                "true_negatives": result['true_negatives'],
                "total_transactions": result['total_transactions'],
                "total_fraud": result['total_fraud'],
                "precision": result['precision'],
                "recall": result['recall'],
                "f1_score": result['f1_score'],
                "accuracy": result['accuracy']
            }
        }
        
        # Lưu vào file
        filename = f"final_fraud_detection_{mode}_metrics.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=2)
        print(f"\n✅ Đã lưu kết quả vào file {filename}")
        
        return metrics
    
    def _analyze_fraud_details(self):
        """Phân tích chi tiết các giao dịch gian lận được phát hiện."""
        
        # Phân tích theo loại quy tắc phát hiện
        print("\n📊 Phân tích theo quy tắc phát hiện:")
        
        rule_query = """
        MATCH ()-[tx:SENT]->()
        WHERE tx.flagged = true
        WITH tx.detection_rule as rule, 
             COUNT(tx) as flagged_count,
             SUM(CASE WHEN tx.ground_truth_fraud = true THEN 1 ELSE 0 END) as true_fraud
        RETURN 
            rule,
            flagged_count,
            true_fraud,
            toFloat(true_fraud) / flagged_count as precision_rate
        ORDER BY true_fraud DESC
        """
        
        rule_result = self.db_manager.run_query(rule_query)

        if rule_result:
            try:
                processed_rules = []
                
                # Process results based on returned data structure
                if isinstance(rule_result, list):
                    processed_rules = rule_result
                elif isinstance(rule_result, dict):
                    processed_rules = [rule_result]
                    
                # Display results
                for item in processed_rules:
                    rule_name = item.get("rule", "unknown")
                    flagged_count = item.get("flagged_count", 0)
                    true_fraud = item.get("true_fraud", 0)
                    precision_rate = item.get("precision_rate", 0)
                    
                    print(f"  • Quy tắc {rule_name}: {flagged_count} giao dịch, {true_fraud} gian lận thực sự, precision {precision_rate:.4f}")
            except Exception as e:
                print(f"  ⚠️ Lỗi khi phân tích kết quả quy tắc: {str(e)}")
        else:
            print("  ⚠️ Không tìm thấy dữ liệu phân tích quy tắc phát hiện")
        
        # Phân tích theo lý do đánh dấu
        print("\n📊 Top 5 lý do đánh dấu gian lận hiệu quả nhất:")
        
        reason_query = """
        MATCH ()-[tx:SENT]->()
        WHERE tx.flagged = true
        WITH tx.flag_reason as reason, 
             COUNT(tx) as flagged_count,
             SUM(CASE WHEN tx.ground_truth_fraud = true THEN 1 ELSE 0 END) as true_fraud
        RETURN 
            reason,
            flagged_count,
            true_fraud,
            toFloat(true_fraud) / flagged_count as precision_rate
        ORDER BY true_fraud DESC
        LIMIT 5
        """
        
        reason_result = self.db_manager.run_query(reason_query)
        if reason_result:
            try:
                processed_reasons = []
                
                # Process results based on returned data structure
                if isinstance(reason_result, list):
                    processed_reasons = reason_result
                elif isinstance(reason_result, dict):
                    processed_reasons = [reason_result]
                    
                # Display results
                for item in processed_reasons:
                    reason_text = item.get("reason", "unknown")
                    flagged_count = item.get("flagged_count", 0)
                    true_fraud = item.get("true_fraud", 0)
                    precision_rate = item.get("precision_rate", 0)
                    
                    print(f"  • {reason_text}: {flagged_count} giao dịch, {true_fraud} gian lận thực sự, precision {precision_rate:.4f}")
            except Exception as e:
                print(f"  ⚠️ Lỗi khi phân tích kết quả lý do: {str(e)}")
        else:
            print("  ⚠️ Không tìm thấy dữ liệu phân tích lý do đánh dấu")

def main():
    parser = argparse.ArgumentParser(description='Hệ thống phát hiện gian lận cuối cùng')
    
    parser.add_argument('--skip-basic', action='store_true',
                      help='Bỏ qua bước phát hiện bất thường cơ bản (sử dụng kết quả hiện có)')
                      
    parser.add_argument('--mode', type=str, choices=['precision', 'recall', 'balanced'],
                      default='balanced',
                      help='Chế độ cân bằng giữa precision và recall (mặc định: balanced)')
    
    args = parser.parse_args()
    
    # Kết nối Neo4j
    try:
        print("🔌 Đang kết nối đến Neo4j...")
        db_manager = DatabaseManager(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        detector = FinalFraudDetection(db_manager)
        
        # Chạy phát hiện
        detector.run_detection(
            skip_basic_detection=args.skip_basic,
            balance_mode=args.mode
        )
        
    except Exception as e:
        print(f"Lỗi: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
