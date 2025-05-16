#!/usr/bin/env python3
"""
Phát hiện mẫu gian lận nâng cao tập trung vào việc cân bằng precision và recall.
Script này thực hiện những phát hiện phức tạp hơn để phát hiện gian lận và sử dụng điểm số hybrid.
"""
import argparse
import time
import sys
import os
import json
import numpy as np
from sklearn.metrics import precision_recall_fscore_support, accuracy_score

# Thêm thư mục gốc vào sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from detector.database_manager import DatabaseManager
from detector.utils.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class AdvancedPatternDetection:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
    def detect_all_patterns(self):
        """Chạy tất cả các phát hiện mẫu gian lận nâng cao."""
        print("🔄 Đang phát hiện các mẫu gian lận nâng cao...")
        
        # 1. Phát hiện mẫu giao dịch "Bùng nổ" (Burst Transactions)
        self.detect_burst_transactions()
        
        # 2. Phát hiện mẫu "Mở tài khoản mới + giao dịch lớn"
        self.detect_new_account_large_transactions()
        
        # 3. Phát hiện mẫu "Tài khoản trung gian" (Pass-through Accounts)
        self.detect_passthrough_accounts()
        
        # 4. Phát hiện mẫu "Phân tách và tái hợp" (Split & Merge)
        self.detect_split_merge_pattern()
        
        # 5. Phát hiện mẫu "Vòng lặp giữa tổ chức" (Institutional Cycling)
        self.detect_institutional_cycling()
        
        # 6. Kết hợp tất cả các điểm
        self.calculate_combined_pattern_score()
        
        print("✅ Hoàn thành phát hiện các mẫu gian lận nâng cao.")
    
    def detect_burst_transactions(self):
        """Phát hiện mẫu giao dịch bùng nổ trong thời gian ngắn."""
        print("  - Đang phát hiện mẫu giao dịch bùng nổ...")
        
        burst_query = """
        // Phát hiện tài khoản có nhiều giao dịch trong thời gian ngắn
        MATCH (a:Account)-[tx:SENT]->()
        WITH a, tx.step AS step
        ORDER BY a, step
        WITH a, collect(step) AS steps
        WHERE size(steps) >= 3
        
        // Phân tích khoảng thời gian giữa các giao dịch
        WITH a, steps,
             [i IN range(0, size(steps)-2) | steps[i+1] - steps[i]] AS intervals
        
        // Tìm các khoảng thời gian ngắn bất thường
        WITH a, intervals,
             [interval IN intervals WHERE interval <= 2] AS short_intervals
        WHERE size(short_intervals) >= 2
        
        // Gán điểm burst dựa trên số lượng khoảng thời gian ngắn
        SET a.burstScore = CASE
            WHEN size(short_intervals) >= 5 THEN 0.9
            WHEN size(short_intervals) >= 3 THEN 0.7
            ELSE 0.5
        END
        """
        
        self.db_manager.run_query(burst_query)
        
        # Set default value for accounts with no burst transactions
        default_query = """
        MATCH (a:Account)
        WHERE a.burstScore IS NULL
        SET a.burstScore = 0.0
        """
        self.db_manager.run_query(default_query)
    
    def detect_new_account_large_transactions(self):
        """Phát hiện tài khoản mới có giao dịch lớn bất thường."""
        print("  - Đang phát hiện mẫu tài khoản mới + giao dịch lớn...")
        
        new_account_query = """
        // Tính điểm cho tài khoản mới có giao dịch lớn
        MATCH (a:Account)-[tx:SENT]->()
        WHERE tx.step <= 5  // Tài khoản mới (giao dịch trong 5 bước đầu)
        WITH a, sum(tx.amount) AS early_total, count(tx) AS early_count
        
        // Tìm tổng số giao dịch của tài khoản
        MATCH (a)-[all_tx:SENT]->()
        WITH a, early_total, early_count, count(all_tx) AS total_count
        
        // Đánh giá mức độ đáng ngờ
        WITH a, 
             early_total,
             early_count,
             total_count,
             CASE WHEN total_count > 0 THEN early_count / toFloat(total_count) ELSE 0 END AS early_ratio
        
        // Gán điểm dựa trên tổng số tiền sớm và tỷ lệ giao dịch sớm
        SET a.newAccountScore = CASE
            WHEN early_total > 50000 AND early_ratio >= 0.5 THEN 0.95  // Giao dịch lớn và chủ yếu là ban đầu
            WHEN early_total > 10000 AND early_ratio >= 0.7 THEN 0.85  // Giao dịch khá lớn và hầu hết là ban đầu
            WHEN early_total > 5000 AND early_ratio >= 0.5 THEN 0.75   // Giao dịch trung bình và một nửa là ban đầu
            WHEN early_total > 1000 THEN 0.5                          // Giao dịch nhỏ hơn
            ELSE 0.0
        END
        """
        
        self.db_manager.run_query(new_account_query)
        
        # Set default value for accounts with no early transactions
        default_query = """
        MATCH (a:Account)
        WHERE a.newAccountScore IS NULL
        SET a.newAccountScore = 0.0
        """
        self.db_manager.run_query(default_query)
    
    def detect_passthrough_accounts(self):
        """Phát hiện tài khoản trung gian chỉ chuyển tiền đi ngay sau khi nhận."""
        print("  - Đang phát hiện mẫu tài khoản trung gian...")
        
        passthrough_query = """
        // Tìm các cặp giao dịch nhận-gửi trong thời gian ngắn
        MATCH (sender:Account)-[tx1:SENT]->()-[:RECEIVED]->(a:Account)-[tx2:SENT]->()-[:RECEIVED]->(receiver:Account)
        WHERE id(sender) <> id(receiver)  // Tránh giao dịch vòng tròn trực tiếp
          AND abs(tx2.step - tx1.step) <= 2  // Chuyển tiền đi ngay sau khi nhận
          AND abs(tx2.amount - tx1.amount) / tx1.amount < 0.1  // Số tiền gần như không đổi
        
        // Đếm số lần làm trung gian
        WITH a, count(*) AS passthrough_count
        
        // Gán điểm dựa trên số lần làm trung gian
        SET a.passthroughScore = CASE
            WHEN passthrough_count >= 5 THEN 0.95  // Nhiều lần làm trung gian
            WHEN passthrough_count >= 3 THEN 0.85  // Khá nhiều lần làm trung gian
            WHEN passthrough_count >= 2 THEN 0.7   // Ít nhất 2 lần làm trung gian
            ELSE 0.5                              // Ít nhất 1 lần làm trung gian
        END
        """
        
        self.db_manager.run_query(passthrough_query)
        
        # Set default value for accounts with no passthrough behavior
        default_query = """
        MATCH (a:Account)
        WHERE a.passthroughScore IS NULL
        SET a.passthroughScore = 0.0
        """
        self.db_manager.run_query(default_query)
    
    def detect_split_merge_pattern(self):
        """Phát hiện mẫu phân tách và tái hợp (split & merge)."""
        print("  - Đang phát hiện mẫu phân tách và tái hợp (split & merge)...")
        
        split_merge_query = """
        // Phát hiện mẫu phân tách: 1 tài khoản gửi cho nhiều tài khoản khác gần như cùng lúc
        MATCH (source:Account)-[tx1:SENT]->()-[:RECEIVED]->(mid:Account)
        WHERE tx1.step >= 0
        WITH source, mid, tx1.step AS split_step
        ORDER BY source, split_step
        WITH source, split_step, collect(mid) AS recipients
        WHERE size(recipients) >= 3  // Phân tách thành ít nhất 3 giao dịch
        
        // Lưu vết tài khoản nguồn
        WITH source, split_step, recipients
        SET source.splitScore = CASE 
            WHEN size(recipients) >= 5 THEN 0.9
            WHEN size(recipients) >= 3 THEN 0.7
            ELSE 0.5
        END
        
        // Tiếp tục tìm mẫu tái hợp
        WITH recipients AS split_accounts, split_step
        
        // Tìm giao dịch tái hợp từ các tài khoản đã được phân tách
        MATCH (merge_account:Account)<-[:RECEIVED]-()<-[tx2:SENT]-(mid:Account)
        WHERE mid IN split_accounts
        AND tx2.step > split_step
        AND tx2.step - split_step <= 5  // Tái hợp xảy ra không quá 5 bước sau phân tách
        
        // Đếm số lượng tài khoản trung gian gửi đến tài khoản tái hợp
        WITH merge_account, count(DISTINCT mid) AS merge_count, size(split_accounts) AS split_count
        
        // Tính tỷ lệ tái hợp
        WITH merge_account, merge_count, split_count,
            merge_count / toFloat(split_count) AS merge_ratio
        
        // Gán điểm tái hợp
        SET merge_account.mergeScore = CASE
            WHEN merge_count >= 3 AND merge_ratio >= 0.8 THEN 0.95  // Hầu hết các giao dịch được tái hợp
            WHEN merge_count >= 2 AND merge_ratio >= 0.5 THEN 0.85  // Nhiều giao dịch được tái hợp
            ELSE 0.6                                               // Ít nhất một phần được tái hợp
        END
        """
        
        self.db_manager.run_query(split_merge_query)
        
        # Set default values for accounts with no split behavior - FIX: Split into two separate queries
        split_default_query = """
        MATCH (a:Account)
        WHERE a.splitScore IS NULL
        SET a.splitScore = 0.0
        """
        self.db_manager.run_query(split_default_query)
        
        # Set default values for accounts with no merge behavior - FIX: Separate query
        merge_default_query = """
        MATCH (a:Account)
        WHERE a.mergeScore IS NULL
        SET a.mergeScore = 0.0
        """
        self.db_manager.run_query(merge_default_query)
    
    def detect_institutional_cycling(self):
        """Phát hiện mẫu vòng lặp giữa các tổ chức."""
        print("  - Đang phát hiện mẫu vòng lặp giữa tổ chức...")
        
        cycling_query = """
        // Tìm các chu trình có độ dài 4-5
        MATCH path = (a:Account)-[:SENT]->()-[:RECEIVED]->(b:Account)-[:SENT]->()-[:RECEIVED]->(c:Account)-[:SENT]->()-[:RECEIVED]->(d:Account)-[:SENT]->()-[:RECEIVED]->(a)
        WHERE id(a) <> id(b) AND id(b) <> id(c) AND id(c) <> id(d) AND id(a) <> id(c) AND id(b) <> id(d) AND id(a) <> id(d)
        
        // Lấy các giao dịch trong chu trình
        WITH a, b, c, d, [r IN relationships(path) WHERE type(r) = 'SENT'] AS sent_txs
        
        // Phân tích số tiền và thời gian
        WITH a, b, c, d,
             [tx IN sent_txs | tx.amount] AS amounts,
             [tx IN sent_txs | tx.step] AS steps
        
        // Tính toán chênh lệch giữa số tiền lớn nhất và nhỏ nhất
        WITH a, b, c, d,
             CASE WHEN size(amounts) > 0 THEN reduce(max_val = 0.0, x IN amounts | CASE WHEN x > max_val THEN x ELSE max_val END) ELSE 0.0 END AS max_amount,
             CASE WHEN size(amounts) > 0 THEN reduce(min_val = 999999.0, x IN amounts | CASE WHEN x < min_val THEN x ELSE min_val END) ELSE 0.0 END AS min_amount,
             CASE WHEN size(steps) > 0 THEN reduce(max_val = 0, x IN steps | CASE WHEN x > max_val THEN x ELSE max_val END) ELSE 0 END AS max_step,
             CASE WHEN size(steps) > 0 THEN reduce(min_val = 999999, x IN steps | CASE WHEN x < min_val THEN x ELSE min_val END) ELSE 0 END AS min_step
        
        // Tính toán tỷ lệ chênh lệch và khoảng thời gian
        WITH a, b, c, d,
             CASE WHEN min_amount = 0 THEN 999999.0 ELSE (max_amount - min_amount) / min_amount END AS amount_ratio,
             max_step - min_step AS cycle_time
        
        // Gán điểm chu trình tổ chức
        WITH a, b, c, d, amount_ratio, cycle_time,
             CASE
                 WHEN cycle_time <= 5 AND amount_ratio <= 0.05 THEN 0.95  // Chu trình rất nhanh và số tiền gần như không đổi
                 WHEN cycle_time <= 10 AND amount_ratio <= 0.1 THEN 0.9   // Chu trình nhanh và số tiền thay đổi rất ít
                 WHEN cycle_time <= 15 AND amount_ratio <= 0.2 THEN 0.8   // Chu trình khá nhanh và số tiền thay đổi ít
                 ELSE 0.7                                                // Chu trình khác
             END AS cycling_score
        
        // Gán điểm cho tất cả các tài khoản trong chu trình
        SET a.institutionalCyclingScore = cycling_score,
            b.institutionalCyclingScore = cycling_score,
            c.institutionalCyclingScore = cycling_score,
            d.institutionalCyclingScore = cycling_score
        """
        
        self.db_manager.run_query(cycling_query)
        
        # Set default value for accounts with no institutional cycling
        default_query = """
        MATCH (a:Account)
        WHERE a.institutionalCyclingScore IS NULL
        SET a.institutionalCyclingScore = 0.0
        """
        self.db_manager.run_query(default_query)
    
    def calculate_combined_pattern_score(self):
        """Kết hợp tất cả các điểm mẫu thành một điểm tổng hợp."""
        print("  - Đang kết hợp các điểm mẫu...")
        
        combined_query = """
        MATCH (a:Account)
        WITH a,
             COALESCE(a.burstScore, 0.0) AS burst,
             COALESCE(a.newAccountScore, 0.0) AS new_account,
             COALESCE(a.passthroughScore, 0.0) AS passthrough,
             COALESCE(a.splitScore, 0.0) AS split,
             COALESCE(a.mergeScore, 0.0) AS merge,
             COALESCE(a.institutionalCyclingScore, 0.0) AS cycling
        
        // Kết hợp các điểm với trọng số
        SET a.advancedPatternScore = 
            burst * 0.15 +
            new_account * 0.2 +
            passthrough * 0.2 +
            split * 0.15 +
            merge * 0.15 +
            cycling * 0.15
        """
        
        self.db_manager.run_query(combined_query)
        
        # Chuẩn hóa điểm cuối cùng
        normalize_query = """
        MATCH (a:Account)
        WITH MIN(a.advancedPatternScore) AS min_score, MAX(a.advancedPatternScore) AS max_score
        MATCH (a:Account)
        SET a.advancedPatternScore = CASE 
            WHEN max_score = min_score THEN 0
            ELSE (a.advancedPatternScore - min_score) / (max_score - min_score)
        END
        """
        self.db_manager.run_query(normalize_query)
        
        # Truyền điểm từ Account đến Transaction
        propagate_query = """
        MATCH (a:Account)-[tx:SENT]->()
        SET tx.advancedPatternScore = a.advancedPatternScore
        """
        self.db_manager.run_query(propagate_query)
        
        # Kết hợp điểm pattern với điểm anomaly_score để tạo điểm tổng hợp tốt hơn
        final_combine_query = """
        MATCH ()-[tx:SENT]->()
        WHERE tx.advancedPatternScore IS NOT NULL
        SET tx.enhancedHybridScore = CASE
            WHEN tx.anomaly_score IS NOT NULL AND tx.advancedAnomalyScore IS NOT NULL
            THEN tx.anomaly_score * 0.3 + tx.advancedAnomalyScore * 0.4 + tx.advancedPatternScore * 0.3
            WHEN tx.anomaly_score IS NOT NULL
            THEN tx.anomaly_score * 0.6 + tx.advancedPatternScore * 0.4
            WHEN tx.advancedAnomalyScore IS NOT NULL
            THEN tx.advancedAnomalyScore * 0.7 + tx.advancedPatternScore * 0.3
            ELSE tx.advancedPatternScore
        END
        """
        self.db_manager.run_query(final_combine_query)
        
        print("✅ Đã tính toán và gán các điểm mẫu cho tất cả tài khoản và giao dịch.")

def main():
    parser = argparse.ArgumentParser(description='Phát hiện mẫu gian lận nâng cao')
    
    parser.add_argument('--evaluate', action='store_true',
                       help='Đánh giá hiệu suất sau khi phát hiện mẫu')
    
    args = parser.parse_args()
    
    # Neo4j connection
    try:
        db_manager = DatabaseManager(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)
        
        # Khởi tạo và chạy phát hiện mẫu nâng cao
        pattern_detector = AdvancedPatternDetection(db_manager)
        
        print(f"""
        =========================================================
        🔍 Advanced Pattern Detection
        =========================================================
        • Phát hiện mẫu giao dịch bùng nổ
        • Phát hiện mẫu tài khoản mới + giao dịch lớn
        • Phát hiện mẫu tài khoản trung gian
        • Phát hiện mẫu phân tách và tái hợp (split & merge)
        • Phát hiện mẫu vòng lặp giữa tổ chức
        =========================================================
        """)
        
        # Phát hiện các mẫu nâng cao
        pattern_detector.detect_all_patterns()
        
        # Đánh giá hiệu suất nếu được yêu cầu
        if args.evaluate:
            print("\n🔄 Đang đánh giá hiệu suất với điểm mẫu nâng cao...")
            
            # Đánh dấu giao dịch bất thường dựa trên enhancedHybridScore
            flag_query = """
            MATCH ()-[tx:SENT]->()
            WHERE tx.enhancedHybridScore IS NOT NULL
            WITH percentileCont(tx.enhancedHybridScore, 0.95) AS threshold
            
            MATCH ()-[tx:SENT]->()
            WHERE tx.enhancedHybridScore IS NOT NULL
            SET tx.patternFlagged = tx.enhancedHybridScore >= threshold
            """
            db_manager.run_query(flag_query)
            
            # Đánh giá hiệu suất
            evaluation_query = """
            MATCH ()-[tx:SENT]->()
            WHERE tx.patternFlagged IS NOT NULL
            WITH
                SUM(CASE WHEN tx.patternFlagged = true AND tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS true_positives,
                SUM(CASE WHEN tx.patternFlagged = true AND tx.ground_truth_fraud = false THEN 1 ELSE 0 END) AS false_positives,
                SUM(CASE WHEN tx.patternFlagged = false AND tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS false_negatives,
                SUM(CASE WHEN tx.patternFlagged = false AND tx.ground_truth_fraud = false THEN 1 ELSE 0 END) AS true_negatives,
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
            
            result = db_manager.run_query(evaluation_query)
            
            if result:
                print("\n📊 Kết quả đánh giá hiệu suất mẫu nâng cao:")
                print(f"  • Precision: {result['precision']:.4f}")
                print(f"  • Recall: {result['recall']:.4f}")
                print(f"  • F1 Score: {result['f1_score']:.4f}")
                print(f"  • Accuracy: {result['accuracy']:.4f}")
                
                # Lưu metrics
                metrics = {
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "model": "advanced_pattern_detection",
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
                with open('advanced_pattern_detection_metrics.json', 'w', encoding='utf-8') as f:
                    json.dump(metrics, f, indent=2)
                
                print("\n✅ Đã lưu kết quả đánh giá vào file advanced_pattern_detection_metrics.json")
        
    except Exception as e:
        print(f"❌ Lỗi: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
