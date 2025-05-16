#!/usr/bin/env python3
"""
Tối ưu hóa phát hiện gian lận sử dụng thuật toán đồ thị nâng cao
và kết hợp nhiều phương pháp để tối đa hóa precision và recall.
"""
import argparse
import time
import sys
import os
from sklearn.metrics import precision_recall_fscore_support, accuracy_score
import json
import pandas as pd
import numpy as np

# Thêm thư mục gốc vào sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from detector.database_manager import DatabaseManager
from detector.fraud_detector import FraudDetector
from detector.advanced_graph_algorithms import AdvancedGraphAlgorithms
from detector.utils.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class EnhancedFraudDetection:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.fraud_detector = FraudDetector(db_manager)
        self.advanced_graph_algorithms = None
        
        # Cấu hình mặc định
        self.config = {
            "advanced_graph_algorithms": True,
            "hybrid_scoring": True,
            "ensemble_thresholds": True,
            "percentile_cutoff": 0.995,
            "score_weights": {
                "anomaly_score": 0.4,
                "advanced_fraud_score": 0.6
            },
            "thresholds": {
                "absolute_threshold": 0.35,
                "statistical_threshold_factor": 3.0,  # mean + 3*std
                "high_recall_percentile": 0.95
            }
        }
    
    def setup_advanced_algorithms(self, graph_name):
        """Thiết lập các thuật toán đồ thị nâng cao."""
        self.advanced_graph_algorithms = AdvancedGraphAlgorithms(self.db_manager, graph_name)
    
    def run_enhanced_detection(self, skip_basic_detection=False):
        """Chạy quy trình phát hiện gian lận nâng cao."""
        print("🔄 Đang chạy quy trình phát hiện gian lận nâng cao...")
        
        start_time = time.time()
        
        # 1. Chạy quy trình phát hiện cơ bản trước (có thể bỏ qua)
        if not skip_basic_detection:
            print("\n🔄 Bước 1: Chạy quy trình phát hiện bất thường cơ bản...")
            metrics = self.fraud_detector.run_pipeline(percentile_cutoff=self.config["percentile_cutoff"])
        else:
            print("\n⏩ Bỏ qua Bước 1: Sử dụng kết quả phát hiện bất thường cơ bản đã có...")
            # Kiểm tra xem đã có anomaly_score chưa
            check_query = """
            MATCH ()-[tx:SENT]->()
            WHERE tx.anomaly_score IS NOT NULL
            RETURN COUNT(tx) AS count
            """
            result = self.db_manager.run_query(check_query)
            if result and result.get("count", 0) > 0:
                print(f"  ✅ Đã tìm thấy {result.get('count', 0)} giao dịch có anomaly_score")
            else:
                print("  ⚠️ Không tìm thấy giao dịch nào có anomaly_score. Có thể cần chạy bước 1.")
        
        # 2. Chạy các thuật toán đồ thị nâng cao nếu được bật
        if self.config["advanced_graph_algorithms"] and self.advanced_graph_algorithms:
            # Kiểm tra xem đã có advancedAnomalyScore chưa
            check_advanced_query = """
            MATCH ()-[tx:SENT]->()
            WHERE tx.advancedAnomalyScore IS NOT NULL
            RETURN COUNT(tx) AS count
            """
            advanced_result = self.db_manager.run_query(check_advanced_query)
            advanced_count = advanced_result.get("count", 0) if advanced_result else 0
            
            if advanced_count > 0:
                print(f"\n⏩ Bỏ qua Bước 2: Đã tìm thấy {advanced_count} giao dịch có advancedAnomalyScore")
            else:
                print("\n🔄 Bước 2: Chạy các thuật toán đồ thị nâng cao...")
                self.advanced_graph_algorithms.run_advanced_algorithms()
        
        # 3. Kết hợp điểm từ hai phương pháp
        if self.config["hybrid_scoring"]:
            print("\n🔄 Bước 3: Kết hợp điểm từ nhiều phương pháp...")
            self._combine_scores()
            
        # 4. Áp dụng phương pháp ensemble threshold
        if self.config["ensemble_thresholds"]:
            print("\n🔄 Bước 4: Áp dụng ngưỡng tích hợp (ensemble)...")
            self._apply_ensemble_thresholds()
        
        # 5. Đánh giá hiệu suất cuối cùng
        print("\n🔄 Bước 5: Đánh giá hiệu suất cuối cùng...")
        final_metrics = self._evaluate_final_performance()
        
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"\n⏱️ Thời gian thực thi: {execution_time:.2f} giây")
        
        return final_metrics
    
    def _combine_scores(self):
        """Kết hợp điểm từ nhiều phương pháp một cách thông minh hơn."""
        combined_score_query = """
        MATCH (a:Account)-[tx:SENT]->(b:Account)
        WITH a, b, tx,
            // Điểm từ phương pháp cơ bản
            COALESCE(tx.anomaly_score, 0) AS basic_score,
            
            // Điểm từ thuật toán đồ thị nâng cao
            COALESCE(tx.advancedAnomalyScore, 0) AS advanced_score,
            
            // Yếu tố bổ sung từ node
            COALESCE(a.patternScore, 0) AS pattern_score,
            COALESCE(a.cycleScore, 0) AS cycle_score,
            COALESCE(a.temporalScore, 0) AS temporal_score,
            COALESCE(a.structuringScore, 0) AS structuring_score
            
        // Tăng cường điểm bằng cách sử dụng các mẫu phát hiện được
        WITH a, b, tx, basic_score, advanced_score,
            pattern_score, cycle_score, temporal_score, structuring_score,
            
            // Bổ sung: Tăng cường điểm nếu phát hiện chu trình
            CASE 
                WHEN cycle_score > 0.8 THEN 0.15
                WHEN cycle_score > 0.6 THEN 0.1
                WHEN cycle_score > 0.4 THEN 0.05
                ELSE 0
            END AS cycle_bonus,
            
            // Bổ sung: Tăng cường điểm nếu có mẫu cấu trúc đáng ngờ
            CASE 
                WHEN pattern_score > 0.8 THEN 0.15
                WHEN pattern_score > 0.6 THEN 0.1
                WHEN pattern_score > 0.4 THEN 0.05
                ELSE 0
            END AS pattern_bonus,
            
            // Bổ sung: Tăng cường điểm nếu có mẫu thời gian đáng ngờ
            CASE 
                WHEN temporal_score > 0.7 THEN 0.1
                WHEN temporal_score > 0.5 THEN 0.05
                ELSE 0
            END AS temporal_bonus,
            
            // Bổ sung: Tăng cường điểm nếu có dấu hiệu chia nhỏ giao dịch
            CASE 
                WHEN structuring_score > 0.7 THEN 0.1
                WHEN structuring_score > 0.5 THEN 0.05
                ELSE 0
            END AS structuring_bonus
            
        // Kết hợp điểm với trọng số và bổ sung
        WITH a, tx, b,
            (basic_score * $anomaly_weight) + 
            (advanced_score * $advanced_weight) +
            cycle_bonus + pattern_bonus + temporal_bonus + structuring_bonus AS hybrid_score,
            cycle_bonus, pattern_bonus, temporal_bonus, structuring_bonus
            
        // Gán điểm kết hợp và lưu các bonus cho phân tích
        SET tx.hybrid_score = hybrid_score,
            tx.cycle_bonus = cycle_bonus,
            tx.pattern_bonus = pattern_bonus,
            tx.temporal_bonus = temporal_bonus,
            tx.structuring_bonus = structuring_bonus
            
        // Trả về thống kê
        RETURN 
            COUNT(tx) AS processed_count,
            MIN(hybrid_score) AS min_score,
            MAX(hybrid_score) AS max_score,
            AVG(hybrid_score) AS avg_score,
            STDEV(hybrid_score) AS std_score,
            AVG(cycle_bonus + pattern_bonus + temporal_bonus + structuring_bonus) AS avg_bonus
        """
        
        result = self.db_manager.run_query(combined_score_query, {
            "anomaly_weight": self.config["score_weights"]["anomaly_score"],
            "advanced_weight": self.config["score_weights"]["advanced_fraud_score"]
        })
        
        if result:
            print(f"✅ Đã kết hợp điểm cho {result.get('processed_count', 0)} giao dịch")
            print(f"📊 Thống kê điểm kết hợp: Min={result.get('min_score', 0):.4f}, " +
                f"Max={result.get('max_score', 0):.4f}, " +
                f"Avg={result.get('avg_score', 0):.4f}, " +
                f"Std={result.get('std_score', 0):.4f}")
            print(f"📊 Trung bình bonus pattern: {result.get('avg_bonus', 0):.4f}")
      
    def _apply_ensemble_thresholds(self):
        """Áp dụng phương pháp tích hợp nhiều ngưỡng được cải tiến để đánh dấu giao dịch bất thường."""
        # Tính toán các ngưỡng thống kê với cách tiếp cận chính xác hơn
        stats_query = """
        MATCH ()-[tx:SENT]->()
        WITH 
            percentileCont(tx.hybrid_score, $high_percentile) AS percentile_threshold,
            AVG(tx.hybrid_score) + $std_factor * STDEV(tx.hybrid_score) AS statistical_threshold,
            $absolute_threshold AS absolute_threshold,
            percentileCont(tx.hybrid_score, $low_percentile) AS recall_threshold,
            AVG(tx.hybrid_score) AS mean_score,
            STDEV(tx.hybrid_score) AS std_score,
            percentileCont(tx.hybrid_score, 0.5) AS median_score
        RETURN 
            percentile_threshold,
            statistical_threshold,
            absolute_threshold,
            recall_threshold,
            mean_score,
            std_score,
            median_score
        """
        
        stats = self.db_manager.run_query(stats_query, {
            "high_percentile": self.config["percentile_cutoff"],
            "low_percentile": self.config["thresholds"]["high_recall_percentile"],
            "std_factor": self.config["thresholds"]["statistical_threshold_factor"],
            "absolute_threshold": self.config["thresholds"]["absolute_threshold"]
        })
        
        if not stats:
            print("⚠️ Không thể tính toán ngưỡng thống kê")
            return
        
        # Lấy các ngưỡng
        percentile_threshold = stats.get("percentile_threshold", 0)
        statistical_threshold = stats.get("statistical_threshold", 0)
        absolute_threshold = stats.get("absolute_threshold", 0)
        recall_threshold = stats.get("recall_threshold", 0)
        mean_score = stats.get("mean_score", 0)
        std_score = stats.get("std_score", 0)
        median_score = stats.get("median_score", 0)
        
        # Tính thêm ngưỡng dựa trên Z-score
        z_threshold = mean_score + 2.5 * std_score
        
        print(f"📊 Ngưỡng phân vị cao ({self.config['percentile_cutoff']*100:.1f}%): {percentile_threshold:.6f}")
        print(f"📊 Ngưỡng phân vị recall cao ({self.config['thresholds']['high_recall_percentile']*100:.1f}%): {recall_threshold:.6f}")
        print(f"📊 Ngưỡng thống kê (mean + {self.config['thresholds']['statistical_threshold_factor']}*std): {statistical_threshold:.6f}")
        print(f"📊 Ngưỡng tuyệt đối: {absolute_threshold:.6f}")
        print(f"📊 Z-score threshold (mean + 2.5*std): {z_threshold:.6f}")
        print(f"📊 Thống kê bổ sung: Mean={mean_score:.6f}, Median={median_score:.6f}, Std={std_score:.6f}")
        
        # Step 1: Reset flagged property trước khi bắt đầu
        print("  - Đang reset trạng thái flagged...")
        reset_query = """
        MATCH ()-[tx:SENT]->()
        SET tx.flagged = false, tx.processed = false, tx.flagReason = null
        RETURN count(tx) as reset_count
        """
        reset_result = self.db_manager.run_query(reset_query)
        total_reset = reset_result.get("reset_count", 0) if reset_result else 0
        print(f"    ✅ Đã reset tổng cộng {total_reset} giao dịch.")
        
        # Step 2: Apply ensemble thresholds với thuật toán nâng cao
        print("  - Đang đánh dấu giao dịch bất thường...")
        batch_size = 5000
        
        # Tìm kiếm các "super anomalies" (giao dịch vượt ngưỡng rất mạnh)
        super_anomaly_query = """
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE tx.hybrid_score IS NOT NULL AND tx.processed = false
        AND (
            // Điều kiện 1: Score vượt ngưỡng phân vị cao
            tx.hybrid_score >= $p_thresh
            // Điều kiện 2: Mẫu hành vi bất thường
            OR (src.cycleScore >= 0.8 AND tx.hybrid_score >= $r_thresh)
            // Điều kiện 3: Tài khoản đích đáng ngờ cao
            OR (dest.patternScore >= 0.8 AND tx.hybrid_score >= $r_thresh)
            // Điều kiện 4: Cấu trúc đồ thị bất thường
            OR (src.structuringScore >= 0.85 AND tx.hybrid_score >= $z_thresh)
        )
        WITH tx, src, dest, 
            CASE
                WHEN tx.hybrid_score >= $p_thresh THEN "Vượt ngưỡng phân vị cao"
                WHEN src.cycleScore >= 0.8 AND tx.hybrid_score >= $r_thresh THEN "Nguồn trong chu trình đáng ngờ"
                WHEN dest.patternScore >= 0.8 AND tx.hybrid_score >= $r_thresh THEN "Đích có mẫu đáng ngờ"
                WHEN src.structuringScore >= 0.85 AND tx.hybrid_score >= $z_thresh THEN "Nguồn có dấu hiệu structuring"
                ELSE "Kết hợp nhiều yếu tố"
            END AS reason
        SET tx.flagged = true, 
            tx.processed = true,
            tx.flagReason = reason,
            tx.confidence = tx.hybrid_score / $p_thresh
        RETURN count(tx) as flagged_count
        """
        
        super_result = self.db_manager.run_query(super_anomaly_query, {
            "p_thresh": percentile_threshold,
            "r_thresh": recall_threshold,
            "z_thresh": z_threshold
        })
        
        super_flagged = super_result.get("flagged_count", 0) if super_result else 0
        print(f"    ✅ Đã đánh dấu {super_flagged} giao dịch bất thường mức độ cao")
        
        # Đánh dấu các giao dịch bình thường với thuật toán cân bằng
        flag_query = """
        MATCH (src:Account)-[tx:SENT]->(dest:Account)
        WHERE tx.hybrid_score IS NOT NULL AND tx.processed = false
        WITH tx, src, dest LIMIT $batch_size
        
        // Phân tích sâu hơn dựa trên nhiều yếu tố đồng thời
        WITH tx, src, dest,
            // Hybrid Score Weight - Trọng số cho điểm hybrid score
            CASE 
                WHEN tx.hybrid_score >= $s_thresh THEN 3
                WHEN tx.hybrid_score >= $a_thresh THEN 2
                WHEN tx.hybrid_score >= $median THEN 1
                ELSE 0
            END +
            // Account Pattern Score - Mẫu lịch sử của tài khoản
            CASE 
                WHEN src.patternScore >= 0.7 THEN 2
                WHEN src.patternScore >= 0.5 THEN 1 
                ELSE 0
            END +
            // Cycle Detection - Phát hiện chu trình
            CASE 
                WHEN src.cycleScore >= 0.7 THEN 2
                WHEN src.cycleScore >= 0.5 THEN 1
                ELSE 0
            END +
            // Temporal Patterns - Mẫu thời gian
            CASE
                WHEN src.temporalScore >= 0.7 THEN 2
                WHEN src.temporalScore >= 0.5 THEN 1
                ELSE 0
            END +
            // Community Analysis - Phân tích cộng đồng
            CASE
                WHEN src.communitySuspiciousScore >= 0.7 THEN 1
                ELSE 0
            END +
            // Network Position - Vị trí trong mạng lưới
            CASE
                WHEN src.hubScore >= 0.7 OR dest.hubScore >= 0.7 THEN 1
                ELSE 0
            END +
            // Money Flow Analysis - Phân tích dòng tiền
            CASE
                WHEN src.moneyFlowScore >= 0.7 THEN 1
                ELSE 0
            END AS criteria_met,
            
            // Tính reason cho flag
            COLLECT(
                CASE
                    WHEN tx.hybrid_score >= $s_thresh THEN "hybrid_score"
                    WHEN src.patternScore >= 0.7 THEN "pattern_score"
                    WHEN src.cycleScore >= 0.7 THEN "cycle_score"
                    WHEN src.temporalScore >= 0.7 THEN "temporal_score"
                    WHEN src.communitySuspiciousScore >= 0.7 THEN "community_score"
                    WHEN src.moneyFlowScore >= 0.7 THEN "money_flow"
                    ELSE null
                END
            ) AS reasons
        
        // Đánh dấu là bất thường theo tiêu chí cải tiến
        SET tx.flagged = CASE 
                WHEN criteria_met >= 4 THEN true // Nhiều yếu tố bất thường
                WHEN criteria_met >= 3 AND tx.hybrid_score >= $a_thresh THEN true // Kết hợp score và mẫu
                ELSE false 
            END,
            tx.processed = true,
            tx.flagReason = CASE
                WHEN criteria_met >= 4 THEN "Combined anomalies: " + REDUCE(s="", x IN [r IN reasons WHERE r IS NOT NULL] | s + x + ",")
                WHEN criteria_met >= 3 AND tx.hybrid_score >= $a_thresh THEN "Multiple signals with high score"
                ELSE null
            END,
            tx.confidence = CASE
                WHEN criteria_met >= 5 THEN 0.95
                WHEN criteria_met >= 4 THEN 0.85
                WHEN criteria_met >= 3 AND tx.hybrid_score >= $a_thresh THEN 0.75
                ELSE 0.0
            END
                
        // Báo cáo số lượng giao dịch được đánh dấu
        RETURN 
            COUNT(tx) AS processed_count,
            SUM(CASE WHEN tx.flagged = true THEN 1 ELSE 0 END) AS flagged_in_batch
        """
        
        total_flagged = super_flagged
        total_processed = super_flagged
        batch_count = 0
        
        while True:
            batch_count += 1
            flag_result = self.db_manager.run_query(flag_query, {
                "batch_size": batch_size,
                "p_thresh": percentile_threshold,
                "s_thresh": statistical_threshold,
                "a_thresh": absolute_threshold,
                "r_thresh": recall_threshold,
                "median": median_score
            })
            
            if not flag_result:
                break
                    
            processed_count = flag_result.get("processed_count", 0)
            flagged_in_batch = flag_result.get("flagged_in_batch", 0)
            
            total_processed += processed_count
            total_flagged += flagged_in_batch
            
            # Dừng khi không còn giao dịch nào để xử lý
            if processed_count == 0:
                break
                    
            print(f"    ✓ Đã xử lý {total_processed}/{total_reset} giao dịch, đánh dấu {total_flagged} giao dịch bất thường", end="\r")
        
        # Áp dụng lần thứ hai để phát hiện các giao dịch liên quan đến gian lận đã xác định
        related_fraud_query = """
        // Tìm các giao dịch liên quan đến giao dịch đã đánh dấu trong cùng một chuỗi
        MATCH (a1:Account)-[tx1:SENT]->(b1:Account)
        WHERE tx1.flagged = true AND tx1.confidence >= 0.85
        
        MATCH (a2:Account)-[tx2:SENT]->(b2:Account)
        WHERE tx2.processed = true AND tx2.flagged = false
        AND (a1 = a2 OR b1 = a2 OR a1 = b2 OR b1 = b2)
        AND abs(tx1.step - tx2.step) <= 3
        AND tx2.hybrid_score >= $median
        
        // Chỉ chọn những giao dịch có điểm cao
        WITH a2, b2, tx2, tx1, 
            (tx2.hybrid_score / $r_thresh) * 0.7 AS related_score
        WHERE related_score >= 0.5
        
        // Đánh dấu giao dịch liên quan
        SET tx2.flagged = true,
            tx2.flagReason = "Related to high-confidence fraudulent transaction",
            tx2.confidence = related_score
        
        RETURN count(tx2) AS related_flagged
        """
        
        related_result = self.db_manager.run_query(related_fraud_query, {
            "median": median_score,
            "r_thresh": recall_threshold
        })
        
        related_flagged = related_result.get("related_flagged", 0) if related_result else 0
        total_flagged += related_flagged
        print(f"    ✅ Đã đánh dấu thêm {related_flagged} giao dịch liên quan đến gian lận xác định")
        
        # Xóa thuộc tính processed tạm thời
        cleanup_query = """
        MATCH ()-[tx:SENT]->()
        WHERE tx.processed = true
        REMOVE tx.processed
        """
        self.db_manager.run_query(cleanup_query)
        
        print(f"\n    ✅ Đã đánh dấu tổng cộng {total_flagged} giao dịch bất thường trong {total_processed} giao dịch")
    
    def _evaluate_final_performance(self):
        """Đánh giá hiệu suất cuối cùng của phương pháp phát hiện gian lận."""
        evaluation_query = """
        MATCH ()-[tx:SENT]->()
        WITH
            SUM(CASE WHEN tx.flagged = true AND tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS true_positives,
            SUM(CASE WHEN tx.flagged = true AND tx.ground_truth_fraud = false THEN 1 ELSE 0 END) AS false_positives,
            SUM(CASE WHEN tx.flagged = false AND tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS false_negatives,
            SUM(CASE WHEN tx.flagged = false AND tx.ground_truth_fraud = false THEN 1 ELSE 0 END) AS true_negatives,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS total_fraud

        // Calculate precision and recall
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

        // Then use precision and recall to calculate F1 score
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
        print("\n📊 Kết quả đánh giá hiệu suất cuối cùng:")
        print(f"  • Tổng số giao dịch: {result['total_transactions']}")
        print(f"  • Tổng số giao dịch gian lận thực tế: {result['total_fraud']}")
        print(f"  • Số giao dịch bất thường được đánh dấu: {result['true_positives'] + result['false_positives']}")
        print(f"  • True Positives: {result['true_positives']}")
        print(f"  • False Positives: {result['false_positives']}")
        print(f"  • False Negatives: {result['false_negatives']}")
        print(f"  • True Negatives: {result['true_negatives']}")
        print(f"  • Precision: {result['precision']:.4f}")
        print(f"  • Recall: {result['recall']:.4f}")
        print(f"  • F1 Score: {result['f1_score']:.4f}")
        print(f"  • Accuracy: {result['accuracy']:.4f}")
        
        # Lưu metrics
        metrics = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "model": "enhanced_fraud_detection",
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
        
        # Lưu metrics ra file
        with open('enhanced_fraud_detection_metrics.json', 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=2)
        
        print("\n✅ Đã lưu kết quả đánh giá vào file enhanced_fraud_detection_metrics.json")
        
        return metrics

def main():
    parser = argparse.ArgumentParser(description='Phát hiện gian lận nâng cao sử dụng đồ thị')
    
    parser.add_argument('--percentile', type=float, default=0.995,
                       help='Ngưỡng phân vị (0.0-1.0) để đánh dấu giao dịch bất thường (mặc định: 0.995)')
    
    parser.add_argument('--basic-weight', type=float, default=0.4,
                       help='Trọng số cho phương pháp cơ bản (mặc định: 0.4)')
    
    parser.add_argument('--advanced-weight', type=float, default=0.6,
                       help='Trọng số cho phương pháp nâng cao (mặc định: 0.6)')
    
    parser.add_argument('--threshold', type=float, default=0.35,
                       help='Ngưỡng tuyệt đối để đánh dấu giao dịch bất thường (mặc định: 0.35)')
    
    parser.add_argument('--graph-name', type=str, default='transactions-graph',
                       help='Tên của graph projection (mặc định: transactions-graph)')
    
    parser.add_argument('--disable-advanced', action='store_true',
                       help='Tắt các thuật toán đồ thị nâng cao')
    
    parser.add_argument('--disable-ensemble', action='store_true',
                       help='Tắt phương pháp ensemble threshold')
    
    parser.add_argument('--skip-basic', action='store_true',
                       help='Bỏ qua bước phát hiện bất thường cơ bản (sử dụng kết quả đã có)')
    
    args = parser.parse_args()
    
    # Neo4j connection
    try:
        db_manager = DatabaseManager(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)
        
        # Khởi tạo phát hiện gian lận nâng cao
        enhanced_detection = EnhancedFraudDetection(db_manager)
        
        # Cấu hình
        enhanced_detection.config["percentile_cutoff"] = args.percentile
        enhanced_detection.config["score_weights"]["anomaly_score"] = args.basic_weight
        enhanced_detection.config["score_weights"]["advanced_fraud_score"] = args.advanced_weight
        enhanced_detection.config["thresholds"]["absolute_threshold"] = args.threshold
        enhanced_detection.config["advanced_graph_algorithms"] = not args.disable_advanced
        enhanced_detection.config["ensemble_thresholds"] = not args.disable_ensemble
        
        # Thiết lập thuật toán đồ thị nâng cao
        if not args.disable_advanced:
            enhanced_detection.setup_advanced_algorithms(args.graph_name)
        
        # Hiển thị cấu hình
        print(f"""
        =========================================================
        🔍 Enhanced Fraud Detection System
        =========================================================
        Thông số được sử dụng:
        • Ngưỡng phân vị: {args.percentile*100:.2f}%
        • Trọng số cơ bản/nâng cao: {args.basic_weight:.2f}/{args.advanced_weight:.2f}
        • Ngưỡng tuyệt đối: {args.threshold:.2f}
        • Tên graph: {args.graph_name}
        • Sử dụng thuật toán nâng cao: {'Không' if args.disable_advanced else 'Có'}
        • Sử dụng ensemble threshold: {'Không' if args.disable_ensemble else 'Có'}
        • Bỏ qua bước phát hiện cơ bản: {'Có' if args.skip_basic else 'Không'}
        =========================================================
        """)
        
        # Chạy phát hiện gian lận nâng cao
        metrics = enhanced_detection.run_enhanced_detection(skip_basic_detection=args.skip_basic)
        
    except Exception as e:
        print(f"❌ Lỗi: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
