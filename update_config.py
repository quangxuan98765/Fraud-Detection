#!/usr/bin/env python3
"""
Công cụ cập nhật cấu hình trọng số cho hệ thống phát hiện gian lận
Dựa trên phân tích tương quan và thử nghiệm thực tế
"""
import json
import os
import argparse
import sys

# Thêm thư mục gốc vào sys.path để có thể import các module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def update_config_file(config_path, new_weights, new_percentile):
    """Cập nhật file cấu hình Python."""
    if not os.path.exists(config_path):
        print(f"❌ Không tìm thấy file cấu hình tại {config_path}")
        return False
    
    with open(config_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Tạo chuỗi trọng số mới
    weights_str = "# Feature weights\nFEATURE_WEIGHTS = {\n"
    for feature, weight in new_weights.items():
        weights_str += f"    '{feature}': {weight:.2f},\n"
    weights_str += "}\n"
    
    # Tìm và thay thế phần trọng số trong file
    import re
    weights_pattern = r"# Feature weights\s*FEATURE_WEIGHTS\s*=\s*\{[^}]*\}"
    updated_content = re.sub(weights_pattern, weights_str.rstrip(), content, flags=re.DOTALL)
    
    # Tìm và thay thế percentile
    percentile_pattern = r"DEFAULT_PERCENTILE\s*=\s*[0-9.]*"
    updated_content = re.sub(percentile_pattern, f"DEFAULT_PERCENTILE = {new_percentile}", updated_content)
    
    # Ghi lại file
    with open(config_path, 'w', encoding='utf-8') as f:
        f.write(updated_content)
    
    print(f"✅ Đã cập nhật file cấu hình tại {config_path}")
    return True

def update_query_file(query_path, new_weights):
    """Cập nhật file truy vấn Cypher tính anomaly score."""
    if not os.path.exists(query_path):
        print(f"❌ Không tìm thấy file truy vấn tại {query_path}")
        return False
    
    with open(query_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Tìm phần tính toán anomaly score
    import re
    
    # Tạo phần weighted sum mới
    weighted_sum = """WITH a, 
    (degScore * {degScore}) + 
    (prScore * {prScore}) + 
    (simScore * {simScore}) + 
    (btwScore * {btwScore}) + 
    (hubScore * {hubScore}) + 
    (authScore * {authScore}) + 
    (coreScore * {coreScore}) + 
    (triCount * {triCount}) + 
    (cycleCount * {cycleCount}) + 
    (tempBurst * {tempBurst}) + 
    (txVelocity * {txVelocity}) +
    (amountVolatility * {amountVolatility}) +
    (maxAmountRatio * {maxAmountRatio}) +
    (stdTimeBetweenTx * {stdTimeBetweenTx}) +
    ({normCommunitySize} * (1 - coalesce(normCommunitySize, 0))) AS score""".format(**new_weights)
    
    # Tìm và thay thế phần tính toán
    weight_pattern = r"WITH a,\s*\((degScore.*\(1 - coalesce\(normCommunitySize, 0\)\)\) AS score"
    updated_content = re.sub(weight_pattern, weighted_sum, content, flags=re.DOTALL)
    
    # Ghi lại file
    with open(query_path, 'w', encoding='utf-8') as f:
        f.write(updated_content)
    
    print(f"✅ Đã cập nhật file truy vấn tại {query_path}")
    return True

def main():
    parser = argparse.ArgumentParser(description='Cập nhật cấu hình trọng số cho hệ thống phát hiện gian lận')
    
    parser.add_argument('--config', type=str, default='detector/utils/config.py',
                       help='Đường dẫn đến file cấu hình (mặc định: detector/utils/config.py)')
    
    parser.add_argument('--query', type=str, default='detector/queries/anomaly_detection_queries.py',
                       help='Đường dẫn đến file truy vấn (mặc định: detector/queries/anomaly_detection_queries.py)')
    
    parser.add_argument('--weights', type=str, default=None,
                       help='Đường dẫn đến file JSON chứa trọng số tùy chỉnh')
    
    parser.add_argument('--percentile', type=float, default=0.995,
                       help='Ngưỡng phân vị mới (mặc định: 0.995)')
    
    parser.add_argument('--optimize-for', choices=['precision', 'recall', 'balanced'],
                       default='precision', help='Tối ưu hóa cho precision, recall hoặc cân bằng (mặc định: precision)')
    
    args = parser.parse_args()
    
    # Trọng số tối ưu cho precision (giảm false positives)
    precision_weights = {
        'degScore': 0.60,
        'prScore': 0.02,
        'simScore': 0.01,
        'btwScore': 0.02,
        'hubScore': 0.08,
        'authScore': 0.01,
        'coreScore': 0.01,
        'triCount': 0.01,
        'cycleCount': 0.01,
        'tempBurst': 0.05,
        'txVelocity': 0.01,
        'amountVolatility': 0.02,
        'maxAmountRatio': 0.12,
        'stdTimeBetweenTx': 0.01,
        'normCommunitySize': 0.02
    }
    
    # Trọng số tối ưu cho recall (giảm false negatives)
    recall_weights = {
        'degScore': 0.30,
        'prScore': 0.05,
        'simScore': 0.05,
        'btwScore': 0.05,
        'hubScore': 0.10,
        'authScore': 0.05,
        'coreScore': 0.05,
        'triCount': 0.05,
        'cycleCount': 0.05,
        'tempBurst': 0.08,
        'txVelocity': 0.05,
        'amountVolatility': 0.05,
        'maxAmountRatio': 0.05,
        'stdTimeBetweenTx': 0.02,
        'normCommunitySize': 0.05
    }
    
    # Trọng số cân bằng (balanced)
    balanced_weights = {
        'degScore': 0.35,
        'prScore': 0.03,
        'simScore': 0.02,
        'btwScore': 0.03,
        'hubScore': 0.10,
        'authScore': 0.02,
        'coreScore': 0.02,
        'triCount': 0.02,
        'cycleCount': 0.02,
        'tempBurst': 0.10,
        'txVelocity': 0.03,
        'amountVolatility': 0.03,
        'maxAmountRatio': 0.20,
        'stdTimeBetweenTx': 0.01,
        'normCommunitySize': 0.02
    }
    
    # Chọn trọng số phù hợp
    if args.optimize_for == 'precision':
        weights = precision_weights
        print("🎯 Tối ưu hóa cho precision (giảm false positives)")
    elif args.optimize_for == 'recall':
        weights = recall_weights
        print("🎯 Tối ưu hóa cho recall (giảm false negatives)")
    else:
        weights = balanced_weights
        print("🎯 Tối ưu hóa cân bằng giữa precision và recall")
    
    # Nếu có file trọng số tùy chỉnh, sử dụng file đó
    if args.weights:
        try:
            with open(args.weights, 'r') as f:
                custom_weights = json.load(f)
            weights = custom_weights
            print(f"✅ Đã tải trọng số tùy chỉnh từ {args.weights}")
        except Exception as e:
            print(f"❌ Lỗi khi tải file trọng số: {str(e)}")
            return
    
    # Cập nhật file cấu hình
    update_config_file(args.config, weights, args.percentile)
    
    # Cập nhật file truy vấn
    update_query_file(args.query, weights)
    
    print(f"""
    =========================================================
    ✅ Cấu hình mới đã được áp dụng
    =========================================================
    • Ngưỡng phân vị: {args.percentile*100:.2f}%
    • Tối ưu hóa cho: {args.optimize_for}
    • File cấu hình: {args.config}
    • File truy vấn: {args.query}
    
    Các trọng số mới:""")
    
    for feature, weight in sorted(weights.items(), key=lambda x: x[1], reverse=True):
        if weight > 0.05:
            print(f"    • {feature}: {weight:.2f} ⭐")
        else:
            print(f"    • {feature}: {weight:.2f}")
    
    print("""
    =========================================================
    💡 Để áp dụng cấu hình mới, chạy:
    python unsupervised_anomaly_detection.py
    
    Hoặc để cải thiện thêm nữa, chạy:
    python precision_improvement.py --percentile {} --optimize-weights
    
    Hoặc sử dụng bộ lọc nâng cao:
    python advanced_fraud_filter.py
    =========================================================
    """.format(args.percentile))

if __name__ == "__main__":
    main()
