import time
import json
import numpy as np
from .database_manager import DatabaseManager
from .utils.config import DEFAULT_PERCENTILE, FEATURE_WEIGHTS
from .queries.evaluation_queries import (
    PERFORMANCE_EVALUATION_QUERY,
    SCORE_DISTRIBUTION_QUERY,
    get_feature_importance_query
)

class EvaluationManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.weights = FEATURE_WEIGHTS
        self.percentile_cutoff = DEFAULT_PERCENTILE
    
    def evaluate_performance(self):
        """Đánh giá hiệu suất phát hiện bất thường dựa trên ground truth."""
        print("🔄 Đang đánh giá hiệu suất phát hiện bất thường...")
        
        result = self.db_manager.run_query(PERFORMANCE_EVALUATION_QUERY)
        
        # Tính các metric khác
        accuracy = (result["true_positives"] + result["true_negatives"]) / result["total_transactions"]
        
        # Prepare detailed metrics report
        metrics = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "model": "unsupervised_anomaly_detection",
            "parameters": {
                "weights": self.weights,
                "percentile_cutoff": self.percentile_cutoff
            },
            "metrics": {
                "true_positives": result["true_positives"],
                "false_positives": result["false_positives"],
                "false_negatives": result["false_negatives"],
                "true_negatives": result["true_negatives"],
                "total_transactions": result["total_transactions"],
                "total_fraud": result["total_fraud"],
                "precision": result["precision"],
                "recall": result["recall"],
                "f1_score": result["f1_score"],
                "accuracy": accuracy
            }
        }
        
        # Lưu metrics ra file
        with open('unsupervised_anomaly_detection_metrics.json', 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=2)
        
        # Hiển thị metrics
        print("\n📊 Kết quả đánh giá hiệu suất:")
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
        print(f"  • Accuracy: {accuracy:.4f}")
        print(f"\n✅ Đã lưu kết quả đánh giá vào file unsupervised_anomaly_detection_metrics.json")
        
        return metrics
    
    def analyze_feature_importance(self, weights=None):
        """Phân tích tầm quan trọng của các đặc trưng sử dụng Python thay vì APOC."""
        print("🔄 Đang phân tích tầm quan trọng của các đặc trưng...")
        
        # Use provided weights or default weights
        weights_to_use = weights or self.weights
        
        # Function to calculate correlation without APOC
        def calculate_correlation(list1, list2):
            if not list1 or not list2 or len(list1) != len(list2):
                return 0
            try:
                # Convert boolean values to integers for correlation calculation
                list1_numeric = [1 if x else 0 for x in list1]
                # Ensure numeric values for list2
                list2_numeric = [float(x) if x is not None else 0 for x in list2]
                
                # If all values are identical, correlation is not defined
                if np.std(list1_numeric) == 0 or np.std(list2_numeric) == 0:
                    return 0
                    
                return np.corrcoef(list1_numeric, list2_numeric)[0, 1]
            except Exception as e:
                print(f"Error calculating correlation: {e}")
                return 0
        
        # Tính tương quan giữa các đặc trưng và ground truth fraud
        features = list(weights_to_use.keys())
        correlations = {}
        
        for feature in features:
            # Sử dụng query từ file queries thay vì hardcode trực tiếp
            query = get_feature_importance_query(feature)
            
            try:
                # Get all records
                with self.db_manager.driver.session() as session:
                    result = session.run(query).data()
                
                if result:
                    # Extract lists for correlation
                    fraud_values = [record['fraud'] for record in result]
                    feature_values = [record['feature_value'] for record in result]
                    
                    # Calculate correlation
                    correlation = calculate_correlation(fraud_values, feature_values)
                    correlations[feature] = correlation
                    print(f"  ✅ Phân tích {feature}: {len(result)} giao dịch, tương quan = {correlation:.4f}")
                else:
                    print(f"  ⚠️ Không có dữ liệu cho {feature}")
                    correlations[feature] = 0
            except Exception as e:
                print(f"  ⚠️ Không thể tính tương quan cho {feature}: {str(e)}")
                correlations[feature] = 0
        
        # Sắp xếp các đặc trưng theo độ quan trọng (giá trị tuyệt đối của tương quan)
        sorted_features = sorted(correlations.items(), key=lambda x: abs(x[1]), reverse=True)
        
        print("\n📊 Tầm quan trọng của các đặc trưng:")
        for feature, correlation in sorted_features:
            print(f"  • {feature}: {correlation:.4f}")
        
        return sorted_features
    
    def visualize_results(self, output_file=None):
        """Hiển thị kết quả phát hiện bất thường dưới dạng đồ thị và biểu đồ."""
        print("🔄 Đang trực quan hóa kết quả...")
        
        try:
            # Get all records for visualization
            with self.db_manager.driver.session() as session:
                records = session.run(SCORE_DISTRIBUTION_QUERY).data()
                
            if records:
                # Extract data for plotting
                scores = [record['score'] for record in records]
                flagged = [record['flagged'] for record in records]
                is_fraud = [record['is_fraud'] for record in records]
                
                # Create a simple visualization or export to file for external visualization
                if output_file:
                    # Export to CSV for external visualization
                    with open(output_file, 'w') as f:
                        f.write("score,flagged,is_fraud\n")
                        for s, fl, fr in zip(scores, flagged, is_fraud):
                            f.write(f"{s},{fl},{fr}\n")
                    print(f"✅ Đã xuất dữ liệu trực quan hóa ra file {output_file}")
                
                # Basic statistics for console output
                flagged_count = sum(1 for f in flagged if f)
                fraud_count = sum(1 for f in is_fraud if f)
                correct_flags = sum(1 for fl, fr in zip(flagged, is_fraud) if fl and fr)
                
                print("\n📊 Thống kê trực quan:")
                print(f"  • Tổng số giao dịch: {len(records)}")
                print(f"  • Số giao dịch được đánh dấu bất thường: {flagged_count}")
                print(f"  • Số giao dịch gian lận thực tế: {fraud_count}")
                print(f"  • Số giao dịch gian lận đã phát hiện đúng: {correct_flags}")
                
                return {
                    "total": len(records),
                    "flagged": flagged_count,
                    "fraud": fraud_count,
                    "correct_flags": correct_flags
                }
            else:
                print("⚠️ Không có dữ liệu để trực quan hóa.")
                return None
                
        except Exception as e:
            print(f"❌ Lỗi khi trực quan hóa kết quả: {str(e)}")
            return None