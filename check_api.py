import requests
import json

def check_api_metrics():
    # Endpoint URL
    url = "http://localhost:8080/api/debug-metrics"
    
    try:
        # Gửi GET request
        response = requests.get(url)
        
        # Kiểm tra status code
        if response.status_code == 200:
            # Parse JSON response
            data = response.json()
            
            # In kết quả
            print("API Debug Metrics:")
            print(json.dumps(data, indent=2))
            
            # Kiểm tra các metrics quan trọng
            metrics = data.get("debug_metrics", {})
            
            print("\nKiểm tra các metrics chính:")
            print(f"- Tổng số tài khoản: {metrics.get('account_count', 0)}")
            print(f"- Tổng số giao dịch: {metrics.get('transaction_count', 0)}")
            print(f"- Giao dịch thực sự gian lận: {metrics.get('real_fraud_transactions', 0)}")
            print(f"- Tài khoản có fraud_score >= 0.7: {metrics.get('fraud_accounts_07', 0)}")
            print(f"- Giao dịch từ tài khoản có fraud_score >= 0.7: {metrics.get('fraud_transactions_07', 0)}")
            
            # Metrics từ các mô hình
            print("\nMetrics từ các mô hình:")
            print(f"- Model 1: {metrics.get('model1_transactions', 0)}")
            print(f"- Model 2: {metrics.get('model2_transactions', 0)}")
            print(f"- Model 3: {metrics.get('model3_transactions', 0)}")
            print(f"- High confidence: {metrics.get('high_confidence_transactions', 0)}")
            print(f"- Funnel: {metrics.get('funnel_disperse_transactions', 0)}")
            print(f"- Round: {metrics.get('round_tx_transactions', 0)}")
            print(f"- Chain: {metrics.get('chain_transactions', 0)}")
            print(f"- Similarity: {metrics.get('similarity_transactions', 0)}")
            print(f"- Velocity: {metrics.get('velocity_transactions', 0)}")
            
        else:
            print(f"Error: API returned status code {response.status_code}")
            print(response.text)
    
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to API. Make sure the Flask server is running.")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    check_api_metrics()
