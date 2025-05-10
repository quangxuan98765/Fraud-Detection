from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
import os
import time
from fraud_detector import FraudDetector
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'your_secret_key'

# Cấu hình upload
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB limit

# Đảm bảo thư mục upload tồn tại
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

detector = FraudDetector()  # Create a single instance

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    try:
        # Kiểm tra trạng thái database mỗi khi load trang
        has_data, stats = detector.check_data()
        return render_template('index.html', has_data=has_data, stats=stats)
    except Exception as e:
        # Xử lý lỗi kết nối database
        flash(f"Lỗi kết nối database: {str(e)}", "error")
        return render_template('index.html', has_data=False, stats={}, error=str(e))

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('Không tìm thấy file', 'error')
        return redirect(request.url)
    
    file = request.files['file']
    
    if file.filename == '':
        flash('Chưa chọn file', 'error')
        return redirect(request.url)
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Import dữ liệu
        start_time = time.time()
        
        try:
            success = detector.import_data(filepath)
            if success:
                import_time = time.time() - start_time
                flash(f'Import thành công trong {import_time:.2f} giây', 'success')
            else:
                flash('Import không thành công', 'error')
        except Exception as e:
            flash(f'Lỗi: {str(e)}', 'error')
            
        return redirect(url_for('index'))
    
    flash('File không hợp lệ, chỉ chấp nhận file CSV', 'error')
    return redirect(request.url)

@app.route('/analyze', methods=['POST'])
def analyze():
    try:
        # Thực hiện phân tích
        start_time = time.time()
        detector.analyze_fraud()
        end_time = time.time()
        print(f"Hàm analyze_fraud() chạy trong {end_time - start_time:.2f} giây")
        flash('Phân tích hoàn tất', 'success')
    except Exception as e:
        flash(f'Lỗi khi phân tích: {str(e)}', 'error')
    
    return redirect(url_for('results'))

@app.route('/results')
def results():
    return render_template('results.html')

# Thêm route để xóa dữ liệu
@app.route('/clear_database', methods=['POST'])
def clear_database():
    try:
        success = detector.clear_database()
        if success:
            flash('Đã xóa thành công tất cả dữ liệu', 'success')
        else:
            flash('Có lỗi khi xóa dữ liệu', 'error')
    except Exception as e:
        flash(f'Lỗi: {str(e)}', 'error')
    
    return redirect(url_for('index'))

# Cập nhật route để lấy thông tin database
@app.route('/api/status')
def get_status():
    try:
        has_data, stats = detector.check_data()
        # Thêm thông tin về phân tích
        with detector.driver.session() as session:
            has_analysis = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score IS NOT NULL
                RETURN count(a) > 0 AS has_analysis
            """).single()
            
        return jsonify({
            'has_data': has_data,
            'has_analysis': has_analysis and has_analysis.get('has_analysis', False) if has_analysis else False,
            'stats': stats
        })
    except Exception as e:
        print(f"Status API error: {str(e)}")
        return jsonify({
            'error': str(e),
            'has_data': False,
            'has_analysis': False,
            'stats': {}
        }), 500

@app.route('/api/fraud-stats')
def get_fraud_stats():
    try:
        with detector.driver.session() as session:            # Thống kê giao dịch đơn giản hóa
            fraud_by_type = session.run("""
                // Lấy tất cả giao dịch liên quan đến tài khoản đáng ngờ
                MATCH (sender:Account)-[r:SENT]->(receiver:Account)
                WHERE sender.fraud_score > 0.5 OR receiver.fraud_score > 0.5
                
                WITH CASE
                    WHEN r.type IS NULL THEN 'Khác'
                    ELSE r.type 
                END AS type,
                COUNT(r) as count,
                SUM(r.amount) as total_amount
                
                RETURN type, count, total_amount
                ORDER BY count DESC
                LIMIT 5
            """).data()
            
            # Log kết quả để debug
            print("Fraud by type data:", fraud_by_type)
            
            # Nếu không có kết quả, thử lấy tất cả các giao dịch
            if not fraud_by_type:
                fraud_by_type = session.run("""
                    MATCH (sender:Account)-[r:SENT]->(receiver:Account)
                    
                    // Nhóm theo loại giao dịch
                    WITH r.type AS type, 
                         COUNT(r) AS count,
                         SUM(r.amount) AS total_amount,
                         AVG(COALESCE(sender.fraud_score, 0) + COALESCE(receiver.fraud_score, 0)) / 2 AS avg_score
                    
                    RETURN type, count, total_amount, avg_score
                    ORDER BY count DESC
                    LIMIT 5
                """).data()
              # Thống kê cộng đồng có nguy cơ cao (đơn giản hóa và tối ưu)
            communities = session.run("""
                // Nhóm theo cộng đồng và lấy các chỉ số thống kê quan trọng
                MATCH (a:Account)
                WHERE a.community IS NOT NULL AND a.fraud_score IS NOT NULL
                
                WITH a.community AS community, 
                     COUNT(a) AS count, 
                     AVG(a.fraud_score) AS avg_score,
                     COUNT(CASE WHEN a.fraud_score > 0.6 THEN 1 END) AS high_risk_count
                
                // Chỉ lấy các cộng đồng có ý nghĩa
                WHERE count >= 3 AND high_risk_count > 0
                
                RETURN 
                    community, 
                    count, 
                    avg_score,
                    high_risk_count,
                    1.0 * high_risk_count / count AS risk_ratio
                ORDER BY risk_ratio DESC, count DESC
                LIMIT 8
            """).data()
            
            # Log kết quả để debug
            print("Communities data:", communities)
              # Thống kê phân bố điểm gian lận (chi tiết hơn)
            score_distribution = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score IS NOT NULL
                RETURN 
                    COUNT(CASE WHEN a.fraud_score > 0.85 THEN 1 END) AS very_high,
                    COUNT(CASE WHEN a.fraud_score > 0.75 AND a.fraud_score <= 0.85 THEN 1 END) AS high,
                    COUNT(CASE WHEN a.fraud_score > 0.65 AND a.fraud_score <= 0.75 THEN 1 END) AS medium,
                    COUNT(CASE WHEN a.fraud_score > 0.45 AND a.fraud_score <= 0.65 THEN 1 END) AS low,
                    COUNT(CASE WHEN a.fraud_score > 0.25 AND a.fraud_score <= 0.45 THEN 1 END) AS very_low,
                    COUNT(CASE WHEN a.fraud_score <= 0.25 THEN 1 END) AS negligible,
                    AVG(a.fraud_score) AS avg_score
            """).single()
              # Thống kê về chu trình giao dịch - đơn giản hóa truy vấn
            transaction_cycles = session.run("""
                // Tìm các chu trình giao dịch ngắn (2-4 bước)
                MATCH path = (a:Account)-[:SENT*2..4]->(a)
                
                // Tính toán thông tin về chu trình
                WITH SIZE(nodes(path)) AS cycle_length,
                     COUNT(DISTINCT a) AS unique_accounts,
                     COUNT(path) AS cycle_count
                
                // Trả về kết quả được sắp xếp
                RETURN 
                    cycle_length,
                    cycle_count,
                    unique_accounts
                ORDER BY cycle_length
            """).data()
            
            return jsonify({
                "fraud_by_type": fraud_by_type,
                "communities": communities,
                "score_distribution": score_distribution,
                "transaction_cycles": transaction_cycles
            })
    except Exception as e:
        print(f"Fraud-stats API error: {str(e)}")
        return jsonify({
            "error": str(e),
            "fraud_by_type": [],
            "communities": [],
            "score_distribution": {"very_high": 0, "high": 0, "medium": 0, "low": 0, "very_low": 0, "negligible": 0},
            "transaction_cycles": []
        })

@app.route('/api/metrics')
def get_metrics():
    try:
        with detector.driver.session() as session:
            # Truy vấn tối ưu để lấy thống kê
            basic_metrics = session.run("""
                MATCH (a:Account)
                WITH count(a) AS total_accounts
                
                MATCH ()-[r:SENT]->() 
                WITH total_accounts, count(r) AS total_transactions
                
                // Đếm tài khoản gian lận với các cách tiếp cận khác nhau
                MATCH (f:Account)
                WHERE f.fraud_score > 0.75
                WITH total_accounts, total_transactions, count(f) AS detected_fraud_accounts
                
                // Đếm giao dịch được phát hiện là gian lận
                MATCH (s1:Account)-[r1:SENT]->(t1:Account)
                WHERE s1.fraud_score > 0.85 OR t1.fraud_score > 0.85
                WITH total_accounts, total_transactions, detected_fraud_accounts, count(r1) AS detected_fraud_transactions
                
                // Đếm các giao dịch có nhãn is_fraud=1 và được phát hiện bởi thuật toán (true positives)
                MATCH (s2:Account)-[r2:SENT {is_fraud: 1}]->(t2:Account)
                WHERE s2.fraud_score > 0.85 AND t2.fraud_score > 0.85
                WITH total_accounts, total_transactions, detected_fraud_accounts, 
                    detected_fraud_transactions, count(r2) AS true_positives
                
                // Đếm tổng số giao dịch được đánh nhãn gian lận
                MATCH ()-[r3:SENT {is_fraud: 1}]->()
                
                RETURN total_accounts, 
                    total_transactions,
                    detected_fraud_accounts,
                    detected_fraud_transactions,
                    true_positives,
                    count(r3) AS ground_truth_frauds
            """).single()
            
            # Phân phối điểm gian lận - giúp hiểu rõ hơn về phân bố
            fraud_levels = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score IS NOT NULL
                RETURN 
                    count(CASE WHEN a.fraud_score > 0.8 THEN 1 END) AS very_high_risk,
                    count(CASE WHEN a.fraud_score > 0.6 AND a.fraud_score <= 0.8 THEN 1 END) AS high_risk,
                    count(CASE WHEN a.fraud_score > 0.4 AND a.fraud_score <= 0.6 THEN 1 END) AS medium_risk,
                    count(CASE WHEN a.fraud_score > 0.2 AND a.fraud_score <= 0.4 THEN 1 END) AS low_risk,
                    count(CASE WHEN a.fraud_score <= 0.2 THEN 1 END) AS very_low_risk
            """).single()
            
            # Thống kê về cộng đồng
            communities = session.run("""
                MATCH (a:Account)
                WHERE a.community IS NOT NULL 
                WITH a.community AS community, count(a) AS size, avg(a.fraud_score) AS avg_score
                RETURN count(DISTINCT community) AS count,
                       count(CASE WHEN avg_score > 0.6 THEN 1 END) AS high_risk_communities
            """).single()
            
            # Thống kê các chu trình giao dịch - điểm đáng ngờ đặc biệt
            cycles = session.run("""
                MATCH path = (a:Account)-[:SENT*2..4]->(a)
                RETURN count(DISTINCT a) AS accounts_in_cycles
            """).single()
            
            # Tạo dữ liệu phản hồi
            metrics_data = {
                "accounts": basic_metrics.get("total_accounts", 0) if basic_metrics else 0,
                "transactions": basic_metrics.get("total_transactions", 0) if basic_metrics else 0,
                "fraud": basic_metrics.get("detected_fraud_accounts", 0) if basic_metrics else 0,
                "ground_truth_frauds": basic_metrics.get("ground_truth_frauds", 0) if basic_metrics else 0,
                "detected_fraud_transactions": basic_metrics.get("detected_fraud_transactions", 0) if basic_metrics else 0,
                "true_positives": basic_metrics.get("true_positives", 0) if basic_metrics else 0,
                "very_high_risk": fraud_levels.get("very_high_risk", 0) if fraud_levels else 0,
                "high_risk": fraud_levels.get("high_risk", 0) if fraud_levels else 0,
                "medium_risk": fraud_levels.get("medium_risk", 0) if fraud_levels else 0,
                "low_risk": fraud_levels.get("low_risk", 0) if fraud_levels else 0,
                "very_low_risk": fraud_levels.get("very_low_risk", 0) if fraud_levels else 0,
                "communities": communities.get("count", 0) if communities else 0,
                "high_risk_communities": communities.get("high_risk_communities", 0) if communities else 0,
                "accounts_in_cycles": cycles.get("accounts_in_cycles", 0) if cycles else 0
            }

            # Kiểm tra nếu metrics tồn tại thì tính toán thêm
            if basic_metrics:
                true_positives = basic_metrics.get("true_positives", 0)
                detected = basic_metrics.get("detected_fraud_transactions", 0)
                actual = basic_metrics.get("ground_truth_frauds", 0)

                # Precision = TP / (TP + FP) 
                precision = true_positives / detected if detected > 0 else 0
                
                # Recall = TP / (TP + FN)
                recall = true_positives / actual if actual > 0 else 0
                
                # F1 Score = 2 * (Precision * Recall) / (Precision + Recall)
                f1_score = 2 * (precision * recall) / (precision + recall) if precision + recall > 0 else 0

                metrics_data.update({
                    "precision": precision,
                    "recall": recall,
                    "f1_score": f1_score
                })
            
            return jsonify({
                "metrics": metrics_data,
                "has_data": True
            })
    except Exception as e:
        print(f"Metrics API error: {str(e)}")
        return jsonify({
            "error": str(e), 
            "has_data": False,
            "metrics": {
                "accounts": 0,
                "transactions": 0,
                "fraud": 0,
                "actual_fraud": 0,
                "high_risk": 0,
                "medium_risk": 0,
                "low_risk": 0,
                "communities": 0,
                "high_risk_communities": 0,
                "accounts_in_cycles": 0
            }
        })

@app.route('/api/suspicious')
def get_suspicious_accounts():
    try:
        with detector.driver.session() as session:
            # Truy vấn cải tiến để lấy các tài khoản đáng ngờ có thêm thông tin chi tiết
            suspicious = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score > 0.5  // Suspicious score threshold
                
                // Lấy thêm thông tin về số lượng giao dịch và tổng giá trị
                OPTIONAL MATCH (a)-[out:SENT]->()
                WITH a, count(out) AS out_count, sum(out.amount) AS out_amount
                
                OPTIONAL MATCH ()-[in:SENT]->(a)
                WITH a, out_count, out_amount, count(in) AS in_count, sum(in.amount) AS in_amount
                
                // Tìm các chu trình giao dịch liên quan đến tài khoản này
                OPTIONAL MATCH path = (a)-[:SENT*2..3]->(a)
                WITH a, out_count, out_amount, in_count, in_amount, 
                     count(path) > 0 AS has_cycle
                
                RETURN 
                    a.id AS id, 
                    a.fraud_score AS score,
                    a.community AS community,
                    COALESCE(a.pagerank_score, 0) AS pagerank,
                    COALESCE(a.degree_score, 0) AS degree,
                    COALESCE(a.similarity_score, 0) AS similarity,
                    COALESCE(a.tx_imbalance, 0) AS imbalance,
                    COALESCE(a.base_score, 0) AS base_score,
                    COALESCE(a.relation_boost, 0) AS relation_boost,
                    COALESCE(a.cycle_boost, 0) AS cycle_boost,
                    CASE WHEN has_cycle THEN 1 ELSE 0 END AS in_cycle,
                    out_count AS sent_count,
                    in_count AS received_count,
                    out_amount AS sent_amount,
                    in_amount AS received_amount,
                    abs(out_amount - in_amount) AS imbalance_amount
                ORDER BY score DESC
                LIMIT 15
            """).data()
              # Log what we found
            print(f"Found {len(suspicious)} suspicious accounts")
            
            # Thêm debug để xem thực sự có gì trong dữ liệu
            if suspicious and len(suspicious) > 0:
                sample_account = suspicious[0]
                print(f"Mẫu tài khoản đáng ngờ: {sample_account}")
                print(f"Các thuộc tính: {list(sample_account.keys())}")
            
            return jsonify({"suspicious": suspicious})
    except Exception as e:
        print(f"Suspicious accounts API error: {str(e)}")
        return jsonify({"error": str(e), "suspicious": []})

@app.route('/api/network')
def get_network():
    try:
        with detector.driver.session() as session:
            # First check if there are any relationships
            rel_count = session.run("""
                MATCH ()-[r]->()
                RETURN count(r) as count
            """).single()
            
            print(f"Total relationships in database: {rel_count['count']}")            # Sử dụng truy vấn mới để lấy mạng lưới gian lận
            top_nodes_result = session.run("""
                // Lấy các tài khoản có điểm gian lận cao làm trung tâm
                MATCH (a:Account)
                WHERE a.fraud_score > 0.4
                WITH a ORDER BY a.fraud_score DESC LIMIT 10
                
                // Lấy các kết nối 1-hop
                OPTIONAL MATCH (a)-[r:SENT]-(other:Account)
                WHERE a <> other
                WITH a, other, r
                WHERE other IS NOT NULL
                
                // Thu thập tất cả các node và relationships
                WITH COLLECT(DISTINCT a) + COLLECT(DISTINCT other) AS allNodes,
                     COLLECT(DISTINCT r) AS allRels
                
                // Trả về kết quả
                WITH 
                    [n IN allNodes WHERE n.fraud_score > 0.4] AS central_nodes,
                    [n IN allNodes WHERE n.fraud_score <= 0.4 OR n.fraud_score IS NULL] AS connected_nodes,
                    allRels AS relationships
                
                RETURN central_nodes, connected_nodes, relationships
            """).single()

            # Debug output to verify what's being returned
            if top_nodes_result:
                central_count = len(top_nodes_result["central_nodes"]) if "central_nodes" in top_nodes_result else 0
                connected_count = len(top_nodes_result["connected_nodes"]) if "connected_nodes" in top_nodes_result else 0
                rel_count = len(top_nodes_result["relationships"]) if "relationships" in top_nodes_result else 0
                print(f"Network query returned: {central_count} central, {connected_count} connected, {rel_count} relationships")
                
                # Inspect actual data
                if central_count > 0:
                    print(f"Sample node properties: {dict(top_nodes_result['central_nodes'][0])}")
            
            # Initialize network data
            network = {"nodes": [], "links": []}
            
            # Track nodes we've added to avoid duplicates
            node_map = {}
            
            # Process central nodes
            central_count = 0
            if top_nodes_result and "central_nodes" in top_nodes_result:
                central_nodes = top_nodes_result["central_nodes"]
                central_count = len(central_nodes)
                
                for node in central_nodes:
                    if node is None:
                        continue
                    
                    # Get node ID
                    node_id = node.get("id", None)
                    if node_id is None:
                        continue
                    
                    # Add to network
                    if node_id not in node_map:
                        node_map[node_id] = True
                        network["nodes"].append({
                            "id": node_id,
                            "score": node.get("fraud_score", 0),
                            "community": node.get("community", 0),
                            "group": 1  # Central node
                        })
            
        # Process connected nodes
            connected_count = 0
            if top_nodes_result and "connected_nodes" in top_nodes_result:
                connected_nodes = top_nodes_result["connected_nodes"]
                connected_count = len([n for n in connected_nodes if n is not None]) 
                
                for node in connected_nodes:
                    if node is None:
                        continue
                    
                    # Get node ID
                    node_id = node.get("id", None)
                    if node_id is None:
                        continue
                    
                    # Add to network if not already added
                    if node_id not in node_map:
                        node_map[node_id] = True
                        network["nodes"].append({
                            "id": node_id,
                            "score": node.get("fraud_score", 0),
                            "community": node.get("community", 0),
                            "group": 2  # Connected node
                        })
              # Process relationships
            rel_count = 0
            if top_nodes_result and "relationships" in top_nodes_result:
                relationships = top_nodes_result["relationships"]
                rel_count = len([r for r in relationships if r is not None])
                
                for rel in relationships:
                    if rel is None:
                        continue
                    
                    # Bảo đảm start_node và end_node tồn tại
                    if not hasattr(rel, 'start_node') or not hasattr(rel, 'end_node'):
                        print(f"Bỏ qua relationship không hợp lệ: {rel}")
                        continue
                    
                    # Get source and target nodes
                    start_id = rel.start_node.get("id", None)
                    end_id = rel.end_node.get("id", None)
                    
                    if start_id is None or end_id is None:
                        continue
                    
                    # Get scores for fraud detection
                    start_score = rel.start_node.get("fraud_score", 0)
                    end_score = rel.end_node.get("fraud_score", 0)
                    
                    # Mark relationship as suspicious if either endpoint has high score
                    is_suspicious = 1 if (start_score > 0.7 or end_score > 0.7) else 0
                    
                    # Add relationship to network
                    network["links"].append({
                        "source": start_id,
                        "target": end_id,
                        "value": rel.get("amount", 1),
                        "is_fraud": is_suspicious,
                        "type": rel.get("type", "unknown")
                    })
            
            # Log what we found
            print(f"Network: {central_count} central nodes, {connected_count} connected nodes, {rel_count} relationships")
            print(f"Final network: {len(network['nodes'])} nodes, {len(network['links'])} links")
            
            return jsonify({"network": network})
    except Exception as e:
        print(f"Network API error: {str(e)}")
        return jsonify({"error": str(e), "network": {"nodes": [], "links": []}})

if __name__ == '__main__':
    app.run(debug=True, port=8080)
