#!/usr/bin/env python3
"""
Công cụ dòng lệnh để tinh chỉnh các tham số phát hiện gian lận
"""
import sys
import os
import json
import time
import argparse

# Thêm thư mục gốc vào sys.path để có thể import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from detector.database_manager import DatabaseManager
from detector.fraud_detector import FraudDetector
from detector.utils.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

def main():
    parser = argparse.ArgumentParser(description='Công cụ tinh chỉnh tham số phát hiện gian lận')
    
    parser.add_argument('--percentile', type=float, default=0.99,
                        help='Ngưỡng phân vị (0.0-1.0) để đánh dấu giao dịch bất thường (mặc định: 0.99)')
    
    parser.add_argument('--weights', type=str, default=None,
                        help='Đường dẫn đến file JSON chứa trọng số tùy chỉnh cho các đặc trưng')
    
    parser.add_argument('--min-flagged', type=int, default=1,
                        help='Số giao dịch bị đánh dấu tối thiểu để xếp một tài khoản vào diện đáng ngờ (mặc định: 1)')
    
    parser.add_argument('--threshold', type=float, default=None,
                        help='Ngưỡng anomaly_score để lọc tài khoản (mặc định: sử dụng flagged relationships)')
    
    parser.add_argument('--evaluate-only', action='store_true',
                        help='Chỉ đánh giá kết quả dựa trên anomaly_score và flagged đã được tính toán từ trước')
    
    parser.add_argument('--cleanup', action='store_true',
                        help='Dọn dẹp các thuộc tính phân tích khỏi cơ sở dữ liệu sau khi hoàn tất')
    
    args = parser.parse_args()
    
    print(f"""
    =================================================
    🔍 Fraud Detection System - Parameter Tuning Tool
    =================================================
    Thông số được sử dụng:
    • Ngưỡng phân vị: {args.percentile*100:.2f}%
    • Min flagged transactions: {args.min_flagged}
    • Threshold: {args.threshold if args.threshold is not None else 'Sử dụng flagged relationships'}
    • Evaluate only: {'Có' if args.evaluate_only else 'Không'}
    • Cleanup: {'Có' if args.cleanup else 'Không'}
    =================================================
    """)
    
    # Initialize database connection and fraud detector
    try:
        db_manager = DatabaseManager(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)
        detector = FraudDetector(db_manager)
        
        # Load custom weights if provided
        if args.weights:
            try:
                with open(args.weights, 'r') as f:
                    weights = json.load(f)
                print(f"✅ Đã tải trọng số tùy chỉnh từ {args.weights}")
                detector.anomaly_detector.weights = weights
            except Exception as e:
                print(f"❌ Lỗi khi tải file trọng số: {str(e)}")
                return
        
        # Run pipeline or evaluation based on arguments
        start_time = time.time()
        
        if args.evaluate_only:
            print("\n📊 Đang đánh giá hiệu suất với dữ liệu hiện có...")
            detector.anomaly_detector.flag_anomalies(args.percentile)
            metrics = detector.evaluation.evaluate_performance()
            feature_importances = detector.evaluation.analyze_feature_importance()
        else:
            print("\n🔄 Đang chạy toàn bộ pipeline phát hiện bất thường...")
            metrics = detector.run_pipeline(percentile_cutoff=args.percentile)
        
        print("\n🔍 Đang tìm các tài khoản đáng ngờ...")
        accounts = detector.get_suspicious_accounts(threshold=args.threshold, min_flagged_tx=args.min_flagged)
        
        print(f"\n✅ Tìm thấy {len(accounts)} tài khoản đáng ngờ")
        
        if args.cleanup:
            print("\n🔄 Dọn dẹp các thuộc tính phân tích...")
            detector.cleanup_properties_and_relationships()
        
        end_time = time.time()
        print(f"\n⏱️ Thời gian thực thi: {end_time - start_time:.2f} giây")
        
    except Exception as e:
        print(f"❌ Lỗi: {str(e)}")
        import traceback
        traceback.print_exc()
        return

if __name__ == "__main__":
    main()
