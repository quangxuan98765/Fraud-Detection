import os
import time
from fraud_detector import FraudDetector

def main():
    try:
        # Bước 1: Kiểm tra file CSV
        input_csv = "filtered_paysim.csv"
        
        if not os.path.exists(input_csv):
            print(f"Không tìm thấy file {input_csv}!")
            return
            
        # Bước 2: Khởi tạo hệ thống
        detector = FraudDetector()
        
        # Bước 3: Import dữ liệu nếu cần
        if detector.check_data():
            print("Dữ liệu đã tồn tại trong database, bỏ qua bước import...")
        else:
            print("Đang import dữ liệu vào Neo4j...")
            start = time.time()
            import_success = detector.import_data(input_csv)
            
            if not import_success:
                print("Import dữ liệu không thành công, hãy kiểm tra kích thước dataset!")
                return
            
            print(f"Thời gian import: {time.time() - start:.2f}s")
        
        # Bước 4: Phân tích
        detector.analyze_fraud()
        
    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        if 'detector' in locals():
            detector.close()

if __name__ == "__main__":
    main()