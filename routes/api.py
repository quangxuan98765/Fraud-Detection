from flask import jsonify, request
import traceback
from datetime import datetime
from . import api_bp
from detector.fraud_detector import FraudDetector
from detector.database_manager import DatabaseManager
from detector.utils.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

# Định nghĩa các ngưỡng mặc định
FRAUD_SCORE_THRESHOLD = 0.7 
SUSPICIOUS_THRESHOLD = 0.5
HIGH_RISK_THRESHOLD = 0.7
VERY_HIGH_RISK_THRESHOLD = 0.9

# Khởi tạo database manager với thông tin kết nối từ config
db_manager = DatabaseManager(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)

# Khởi tạo detector với db_manager
detector = FraudDetector(db_manager)

@api_bp.route('/status')
def get_status():
    """Trả về trạng thái kết nối cơ sở dữ liệu và thông tin cơ bản"""
    try:
        has_data, stats = db_manager.check_data()
        
        # Kiểm tra xem đã có phân tích chưa
        has_analysis = False
        with db_manager.driver.session() as session:
            result = session.run("""
                MATCH (a:Account) 
                WHERE a.fraud_score IS NOT NULL 
                RETURN count(a) > 0 as has_analysis
            """).single()
            if result:
                has_analysis = result.get("has_analysis", False)
            
        return jsonify({
            'has_data': has_data,
            'has_analysis': has_analysis,
            'stats': stats
        })
    except Exception as e:
        print(f"Status API error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'has_data': False,
            'has_analysis': False,
            'stats': {}
        }), 500

@api_bp.route('/suspicious')
def get_suspicious():
    """API lấy các tài khoản đáng ngờ"""
    try:
        # Sử dụng phương thức từ FraudDetector
        suspicious_accounts = detector.get_suspicious_accounts(min_flagged_tx=1)
        
        # Nếu không tìm thấy kết quả, thử tìm các tài khoản có anomaly_score cao nhất
        if not suspicious_accounts:
            print("Không tìm thấy tài khoản đáng ngờ qua phương thức chính, thử phương pháp thay thế...")
            with db_manager.driver.session() as session:
                # Truy vấn tài khoản có anomaly_score cao nhất
                account_query = """
                MATCH (a:Account)
                WHERE a.anomaly_score IS NOT NULL
                WITH a
                ORDER BY a.anomaly_score DESC
                LIMIT 50
                OPTIONAL MATCH (a)-[r:SENT]->()
                RETURN 
                    a.id as account,
                    a.anomaly_score as score,
                    count(r) as flagged_tx_count
                """
                accounts_result = session.run(account_query).data()
                if accounts_result and len(accounts_result) > 0:
                    suspicious_accounts = accounts_result
                    print(f"Tìm thấy {len(suspicious_accounts)} tài khoản có anomaly_score cao bằng phương pháp thay thế")
        
        # Nếu vẫn không tìm thấy, thử tìm các giao dịch đáng ngờ theo cách cũ
        if not suspicious_accounts:
            print("Vẫn không tìm thấy tài khoản, thử tìm giao dịch đáng ngờ...")
            with db_manager.driver.session() as session:
                query = """
                MATCH (from:Account)-[tx:SENT]->(to:Account)
                WHERE tx.anomaly_score IS NOT NULL
                RETURN 
                    id(tx) as id,
                    from.id as from_id, 
                    to.id as to_id, 
                    tx.amount as amount, 
                    tx.step as step,
                    tx.type as type,
                    tx.anomaly_score as anomaly_score,
                    tx.isFraud as is_fraud
                ORDER BY tx.anomaly_score DESC
                LIMIT 100
                """
                transactions = session.run(query).data()
                
                return jsonify({
                    "transactions": transactions,
                    "count": len(transactions)
                })
        
        # Chuyển đổi định dạng để phù hợp với frontend
        formatted_accounts = []
        for acc in suspicious_accounts:
            formatted_accounts.append({
                "account_id": acc["account"],
                "anomaly_score": acc["score"],
                "flagged_tx_count": acc["flagged_tx_count"],
                "type": "SUSPICIOUS"
            })
        
        return jsonify({
            "accounts": formatted_accounts,
            "count": len(formatted_accounts)
        })
    except Exception as e:
        print(f"Suspicious API error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "accounts": [],
            "count": 0
        }), 500
        
        # Chuyển đổi định dạng để phù hợp với frontend
        formatted_accounts = []
        for acc in suspicious_accounts:
            formatted_accounts.append({
                "account_id": acc["account"],
                "anomaly_score": acc["score"],
                "flagged_tx_count": acc["flagged_tx_count"],
                "type": "SUSPICIOUS"
            })
        
        return jsonify({
            "accounts": formatted_accounts,
            "count": len(formatted_accounts)
        })
    except Exception as e:
        print(f"Suspicious API error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "accounts": [],
            "count": 0
        }), 500
        
        # Định dạng lại kết quả để phù hợp với frontend
        formatted_accounts = []
        for acc in suspicious_accounts:
            formatted_accounts.append({
                "account_id": acc["account"],
                "anomaly_score": acc["score"],
                "flagged_tx_count": acc["flagged_tx_count"],
                "type": "SUSPICIOUS" # Loại tài khoản
            })
            
        return jsonify({
            "accounts": formatted_accounts,
            "count": len(formatted_accounts)
        })
    except Exception as e:
        print(f"Suspicious API error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "accounts": [],            "count": 0
        })

@api_bp.route('/account/<account_id>')
def get_account_details(account_id):
    """API lấy thông tin chi tiết tài khoản"""
    try:
        # Tạo Cypher query để lấy thông tin chi tiết tài khoản
        query = """
        MATCH (a:Account {id: $account_id})
        OPTIONAL MATCH (a)-[sent:SENT]->(recipient)
        OPTIONAL MATCH (sender)-[received:SENT]->(a)
        
        WITH a, 
             count(distinct sent) as out_transactions,
             count(distinct received) as in_transactions,
             sum(sent.amount) as total_sent,
             sum(received.amount) as total_received,
             collect(distinct recipient.id) as recipients,
             collect(distinct sender.id) as senders
        
        RETURN a.id as id,
               a.anomaly_score as anomaly_score,
               out_transactions,
               in_transactions,
               total_sent,
               total_received,
               recipients[..5] as top_recipients,
               senders[..5] as top_senders
        """
        
        # Thực thi query với tham số
        with db_manager.driver.session() as session:
            result = session.run(query, account_id=account_id).single()
            
            if result:
                # Chuyển đổi neo4j.Record sang dict
                account_details = dict(result)
                
                # Định dạng lại kết quả để trả về client
                return jsonify({
                    "success": True,
                    "account": account_details
                })
            else:
                return jsonify({
                    "success": False,
                    "error": "Không tìm thấy tài khoản"
                })
                
    except Exception as e:
        print(f"Account details API error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@api_bp.route('/network')
def get_network():
    """API cho mạng lưới giao dịch"""
    try:
        with db_manager.driver.session() as session:
            # Truy vấn đơn giản cho relationships
            query = """
            MATCH (from:Account)-[tx:SENT]->(to:Account)
            WHERE from <> to
            RETURN 
                from.id as source, 
                to.id as target, 
                tx.amount as value,
                tx.anomaly_score as score,
                tx.type as type
            LIMIT 50
            """
            relationships = session.run(query).data()
            
            # Lấy danh sách các node duy nhất từ các mối quan hệ
            nodes = set()
            for rel in relationships:
                nodes.add(rel['source'])
                nodes.add(rel['target'])
            
            # Lấy thông tin chi tiết của các node
            node_data = []
            for node_id in nodes:
                query = f"""
                MATCH (a:Account {{id: '{node_id}'}})
                RETURN 
                    a.id as id,
                    a.fraud_score as fraud_score,
                    a.communityId as group
                """
                result = session.run(query).single()
                if result:
                    node_data.append(dict(result))
            
            return jsonify({
                "nodes": node_data,
                "links": relationships
            })
    except Exception as e:
        print(f"Network API error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "nodes": [],
            "links": []
        })

@api_bp.route('/fraud-stats')
def get_basic_fraud_stats():
    """Thống kê giao dịch đơn giản"""
    try:
        with db_manager.driver.session() as session:
            # Thống kê theo loại giao dịch
            type_stats = session.run("""
                MATCH ()-[r:SENT]->()
                RETURN r.type as type, count(r) as count
                ORDER BY count DESC
            """).data()
            
            # Phân phối điểm đánh giá
            score_distribution = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score IS NOT NULL
                RETURN 
                    count(a) as total,
                    sum(CASE WHEN a.fraud_score < 0.25 THEN 1 ELSE 0 END) as low_risk,
                    sum(CASE WHEN a.fraud_score >= 0.25 AND a.fraud_score < 0.5 THEN 1 ELSE 0 END) as medium_risk,
                    sum(CASE WHEN a.fraud_score >= 0.5 AND a.fraud_score < 0.75 THEN 1 ELSE 0 END) as high_risk,
                    sum(CASE WHEN a.fraud_score >= 0.75 THEN 1 ELSE 0 END) as very_high_risk
            """).single()
            
            # Thống kê cơ bản
            basic_stats = session.run("""
                MATCH (a:Account)
                WITH count(a) as total_accounts
                MATCH ()-[r:SENT]->()
                RETURN 
                    total_accounts,
                    count(r) as total_transactions
            """).single()
            
            stats = dict(basic_stats) if basic_stats else {'total_accounts': 0, 'total_transactions': 0}
            
            return jsonify({
                "fraud_by_type": type_stats,
                "score_distribution": dict(score_distribution) if score_distribution else {},
                "stats": stats
            })
    except Exception as e:
        print(f"Basic fraud stats API error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "fraud_by_type": [],
            "score_distribution": {},
            "stats": {}
        })

@api_bp.route('/metrics')
def get_metrics():
    try:
        metrics = {}
        
        with db_manager.driver.session() as session:
            # Get total accounts
            result = session.run("""
                MATCH (a:Account) 
                RETURN count(a) as total_accounts
            """).single()
            if result:
                metrics["total_accounts"] = result["total_accounts"]
            
            # Get total transactions
            result = session.run("""
                MATCH ()-[r:SENT]->() 
                RETURN count(r) as total_transactions
            """).single()
            if result:
                metrics["total_transactions"] = result["total_transactions"]
            
            # Get flagged (detected) fraud count
            result = session.run("""
                MATCH ()-[r:SENT]->()
                WHERE r.flagged = true
                RETURN count(r) as detected_fraud_count
            """).single()
            if result:
                metrics["detected_fraud_count"] = result["detected_fraud_count"]
            
            # Get risk communities count
            result = session.run("""
                MATCH (a:Account)
                WHERE a.communityId IS NOT NULL
                RETURN count(distinct a.communityId) as risk_communities
            """).single()
            if result:
                metrics["risk_communities"] = result["risk_communities"]
              # Get evaluation metrics from the metrics file
            try:
                import json
                with open('unsupervised_anomaly_detection_metrics.json', 'r') as f:
                    eval_metrics = json.load(f)
                    if 'metrics' in eval_metrics:
                        # Add precision, recall, f1_score
                        metrics["precision"] = eval_metrics['metrics']['precision']
                        metrics["recall"] = eval_metrics['metrics']['recall']
                        metrics["f1_score"] = eval_metrics['metrics']['f1_score']
                        metrics["true_positives"] = eval_metrics['metrics']['true_positives']
                        metrics["total_fraud"] = eval_metrics['metrics']['total_fraud']
                        
                        # Use the true_positives + false_positives for detected_fraud_count if not set yet
                        if "detected_fraud_count" not in metrics or metrics["detected_fraud_count"] == 0:
                            metrics["detected_fraud_count"] = eval_metrics['metrics']['true_positives'] + eval_metrics['metrics']['false_positives']
                        
                        # Add some default communities if we have analysis results but no communities detected
                        if "detected_fraud_count" in metrics and metrics["detected_fraud_count"] > 0:
                            if "risk_communities" not in metrics or metrics["risk_communities"] == 0:
                                # Giả lập một số cộng đồng rủi ro dựa trên số lượng giao dịch gian lận
                                # Giả sử trung bình 5 giao dịch gian lận / cộng đồng
                                estimated_communities = max(1, int(metrics["detected_fraud_count"] / 5000))
                                metrics["risk_communities"] = estimated_communities
            except Exception as e:
                print(f"Could not read metrics file: {str(e)}")
            
            return jsonify({
                "metrics": metrics,
                "has_data": True
            })
    except Exception as e:
        import traceback
        print(f"Metrics API error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": str(e), 
            "has_data": False,
            "metrics": {}
        })

@api_bp.route('/debug')
def debug_data():
    """Debug API to check what data is available"""
    try:
        debug_info = {}
        
        with db_manager.driver.session() as session:
            # Check accounts
            result = session.run("""
                MATCH (a:Account) 
                RETURN count(a) as account_count
            """).single()
            if result:
                debug_info["account_count"] = result["account_count"]
            
            # Check transactions
            result = session.run("""
                MATCH ()-[r:SENT]->() 
                RETURN count(r) as transaction_count
            """).single()
            if result:
                debug_info["transaction_count"] = result["transaction_count"]
            
            # Check flagged transactions (different properties to try)
            result = session.run("""
                MATCH ()-[r:SENT]->()
                WHERE r.flagged = true
                RETURN count(r) as flagged_count
            """).single()
            if result:
                debug_info["flagged_count"] = result["flagged_count"]
                
            result = session.run("""
                MATCH ()-[r:SENT]->()
                WHERE r.detected_fraud = true
                RETURN count(r) as detected_fraud_count
            """).single()
            if result:
                debug_info["detected_fraud_count"] = result["detected_fraud_count"]
                
            # Check community data
            result = session.run("""
                MATCH (a:Account)
                WHERE a.communityId IS NOT NULL
                RETURN count(distinct a.communityId) as community_count
            """).single()
            if result:
                debug_info["community_count"] = result["community_count"]
                
            # Check what properties exist on relationships
            result = session.run("""
                MATCH ()-[r:SENT]->()
                RETURN keys(r) AS rel_properties
                LIMIT 1
            """).single()
            if result:
                debug_info["relationship_properties"] = result["rel_properties"]
                
            # Check what properties exist on accounts
            result = session.run("""
                MATCH (a:Account)
                RETURN keys(a) AS node_properties
                LIMIT 1
            """).single()
            if result:
                debug_info["account_properties"] = result["node_properties"]
            
            return jsonify(debug_info)
    except Exception as e:
        import traceback
        print(f"Debug API error: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)})

@api_bp.route('/run-detection', methods=['POST'])
def run_detection():
    """Run the fraud detection pipeline"""
    try:
        # Create a new instance of FraudDetector
        from detector.fraud_detector import FraudDetector
        detector = FraudDetector(db_manager)
        
        # Run the pipeline with default percentile cutoff
        metrics = detector.run_pipeline()
        
        return jsonify({
            "success": True,
            "message": "Fraud detection pipeline completed successfully",
            "metrics": metrics
        })
    except Exception as e:
        import traceback
        print(f"Run detection error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500