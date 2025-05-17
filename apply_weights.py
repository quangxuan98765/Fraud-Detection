#!/usr/bin/env python3
"""
Script mới để cập nhật trọng số trong file config.py
"""

import os
import sys
import json
import argparse
import re

def update_config(config_path, weights_path, percentile=None):
    """Cập nhật file config.py với trọng số từ file JSON"""
    
    # Đọc trọng số từ file JSON
    with open(weights_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if 'feature_importance' in data:
        weights = data['feature_importance']
    elif 'optimized_weights' in data:
        weights = data['optimized_weights']
    else:
        print("❌ Không tìm thấy trọng số trong file JSON")
        return False
    
    # Đọc file config
    with open(config_path, 'r', encoding='utf-8') as f:
        config_content = f.read()
    
    # Tìm khối FEATURE_WEIGHTS
    weights_block_pattern = r'FEATURE_WEIGHTS\s*=\s*{([^}]*)}'
    match = re.search(weights_block_pattern, config_content, re.DOTALL)
    
    if not match:
        print("❌ Không tìm thấy FEATURE_WEIGHTS trong config")
        return False
    
    old_weights_block = match.group(0)
    
    # Tạo khối trọng số mới
    new_weights_block = "FEATURE_WEIGHTS = {\n"
    for feature, weight in weights.items():
        # Chuyển đổi weight từ string sang float nếu cần
        if isinstance(weight, str):
            try:
                weight = float(weight)
            except ValueError:
                print(f"⚠️ Không thể chuyển đổi trọng số '{weight}' sang số")
                weight = 0.0
        
        new_weights_block += f"    '{feature}': {weight:.2f},\n"
    new_weights_block += "}"
    
    # Thay thế khối trọng số cũ bằng mới
    updated_content = config_content.replace(old_weights_block, new_weights_block)
    
    # Cập nhật percentile nếu được chỉ định
    if percentile is not None:
        percentile_pattern = r'DEFAULT_PERCENTILE\s*=\s*[0-9.]*'
        match = re.search(percentile_pattern, updated_content)
        if match:
            old_percentile = match.group(0)
            new_percentile = f'DEFAULT_PERCENTILE = {percentile}'
            updated_content = updated_content.replace(old_percentile, new_percentile)
            print(f"✅ Đã cập nhật percentile: {old_percentile} -> {new_percentile}")
    
    # Hiển thị trọng số cũ và mới
    print("\n📊 Trọng số cũ:")
    print(old_weights_block)
    print("\n📊 Trọng số mới:")
    print(new_weights_block)
    
    # Lưu file config đã cập nhật
    with open(config_path, 'w', encoding='utf-8') as f:
        f.write(updated_content)
    
    print(f"\n✅ Đã cập nhật {config_path} thành công")
    return True

def main():
    parser = argparse.ArgumentParser(description='Cập nhật trọng số trong config.py')
    
    parser.add_argument('--config', type=str, default='detector/utils/config.py',
                      help='Đường dẫn đến file config.py')
    
    parser.add_argument('--weights', type=str, required=True,
                      help='Đường dẫn đến file JSON chứa trọng số')
    
    parser.add_argument('--percentile', type=float, default=None,
                      help='Ngưỡng phân vị mới (ví dụ: 0.99)')
    
    args = parser.parse_args()
    
    # Kiểm tra file tồn tại
    if not os.path.exists(args.config):
        print(f"❌ Không tìm thấy file config: {args.config}")
        return
    
    if not os.path.exists(args.weights):
        print(f"❌ Không tìm thấy file trọng số: {args.weights}")
        return
    
    update_config(args.config, args.weights, args.percentile)

if __name__ == "__main__":
    main()
