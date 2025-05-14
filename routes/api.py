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
    """API lấy các giao dịch đáng ngờ"""
    try:
        with db_manager.driver.session() as session:
            # Truy vấn đơn giản không filter phức tạp
            query = """
            MATCH (from:Account)-[tx:SENT]->(to:Account)
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
    except Exception as e:
        print(f"Suspicious API error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "transactions": [],
            "count": 0
        })

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
    """API lấy metrics"""
    try:
        metrics = {}
        
        with db_manager.driver.session() as session:
            # Lấy metrics từ node Metrics (như trong file JSON mẫu)
            result = session.run("""
                MATCH (m:Metrics)
                RETURN 
                    m.precision as precision, 
                    m.recall as recall, 
                    m.f1_score as f1_score,
                    m.accuracy as accuracy,
                    m.true_positives as true_positives,
                    m.false_positives as false_positives,
                    m.false_negatives as false_negatives,
                    m.true_negatives as true_negatives,
                    m.total_fraud as total_fraud,
                    m.total_transactions as total_transactions
                ORDER BY m.timestamp DESC LIMIT 1
            """).single()
            
            if result:
                metrics.update(dict(result))
            
            # Số tài khoản
            result = session.run("""
                MATCH (a:Account) 
                RETURN count(a) as total_accounts
            """).single()
            if result:
                metrics["total_accounts"] = result["total_accounts"]
            
            return jsonify({
                "metrics": metrics,
                "has_data": True
            })
    except Exception as e:
        print(f"Metrics API error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": str(e), 
            "has_data": False,
            "metrics": {}
        })