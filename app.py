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
        with detector.driver.session() as session:
            # Thống kê giao dịch có liên quan đến tài khoản gian lận (dựa trên fraud_score)
            fraud_by_type = session.run("""
                MATCH (sender:Account)-[r:SENT]->(receiver:Account)
                WHERE sender.fraud_score > 0.7 OR receiver.fraud_score > 0.7
                RETURN r.type AS type, count(r) AS count
                ORDER BY count DESC
            """).data()
            
            # Thống kê cộng đồng có nguy cơ cao
            communities = session.run("""
                MATCH (a:Account)
                WHERE a.community IS NOT NULL AND a.fraud_score > 0.6
                WITH a.community AS community, count(a) AS count, avg(a.fraud_score) AS avg_score
                WHERE count > 5
                RETURN community, count, avg_score
                ORDER BY avg_score DESC, count DESC
                LIMIT 10
            """).data()
            
            # Thống kê phân bố điểm gian lận
            score_distribution = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score IS NOT NULL
                RETURN 
                    count(CASE WHEN a.fraud_score > 0.9 THEN 1 END) AS very_high,
                    count(CASE WHEN a.fraud_score > 0.7 AND a.fraud_score <= 0.9 THEN 1 END) AS high,
                    count(CASE WHEN a.fraud_score > 0.5 AND a.fraud_score <= 0.7 THEN 1 END) AS medium,
                    count(CASE WHEN a.fraud_score > 0.3 AND a.fraud_score <= 0.5 THEN 1 END) AS low,
                    count(CASE WHEN a.fraud_score <= 0.3 THEN 1 END) AS very_low
            """).single()
            
            return jsonify({
                "fraud_by_type": fraud_by_type,
                "communities": communities,
                "score_distribution": score_distribution
            })
    except Exception as e:
        print(f"Fraud-stats API error: {str(e)}")
        return jsonify({
            "error": str(e),
            "fraud_by_type": [],
            "communities": [],
            "score_distribution": {"very_high": 0, "high": 0, "medium": 0, "low": 0, "very_low": 0}
        })

@app.route('/api/metrics')
def get_metrics():
    try:
        with detector.driver.session() as session:
            # Query database statistics
            basic_metrics = session.run("""
                MATCH (a:Account)
                WITH count(a) AS total_accounts
                
                MATCH ()-[r:SENT]->() 
                WITH total_accounts, count(r) AS total_transactions
                
                // Count fraudulent accounts with variable thresholds
                OPTIONAL MATCH (f:Account)
                WHERE 
                // Higher threshold for high PageRank accounts
                (f.pagerank_score > 0.75 AND f.fraud_score > 0.50) OR
                // Higher threshold for accounts with similar transaction patterns
                (f.similarity_score > 0.70 AND f.fraud_score > 0.45) OR
                // Regular threshold
                (f.fraud_score > 0.60)
                
                WITH total_accounts, total_transactions, collect(f) AS fraud_accounts
                
                // Find large transactions separately
                OPTIONAL MATCH (a:Account)-[r:SENT]->(b:Account)
                WHERE r.amount > 40000 AND a.fraud_score > 0.5
                
                WITH total_accounts, total_transactions, 
                    fraud_accounts, collect(DISTINCT a) AS high_tx_accounts, 
                    collect(DISTINCT b) AS receiving_accounts
                
                // Combine all suspicious accounts
                WITH total_accounts, total_transactions,
                    fraud_accounts + high_tx_accounts + receiving_accounts AS combined_accounts
                
                // Unwind and limit to reduce false positives
                UNWIND combined_accounts AS susp
                WITH total_accounts, total_transactions, susp
                WHERE susp IS NOT NULL AND susp.fraud_score > 0.517
                ORDER BY susp.fraud_score DESC
                
                WITH total_accounts, total_transactions, collect(susp) AS limited_suspicious
                
                // Count actual fraudulent transactions (for evaluation)
                MATCH ()-[r:SENT {is_fraud: 1}]->()
                RETURN total_accounts, total_transactions, 
                    size(limited_suspicious) AS detected_fraud_accounts,
                    count(r) AS actual_fraud_transactions
            """).single()
            
            # Get fraud level distribution
            fraud_levels = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score IS NOT NULL
                RETURN 
                    count(CASE WHEN a.fraud_score > 0.7 THEN 1 END) AS high_risk,
                    count(CASE WHEN a.fraud_score > 0.5 AND a.fraud_score <= 0.7 THEN 1 END) AS medium_risk,
                    count(CASE WHEN a.fraud_score > 0.3 AND a.fraud_score <= 0.5 THEN 1 END) AS low_risk
            """).single()
            
            # Count communities
            communities = session.run("""
                MATCH (a:Account)
                WHERE a.community IS NOT NULL 
                WITH a.community AS community
                RETURN count(DISTINCT community) AS count
            """).single()
            
            # Create response data
            metrics_data = {
                "accounts": basic_metrics.get("total_accounts", 0) if basic_metrics else 0,
                "transactions": basic_metrics.get("total_transactions", 0) if basic_metrics else 0,
                "fraud": basic_metrics.get("detected_fraud_accounts", 0) if basic_metrics else 0,
                "actual_fraud": basic_metrics.get("actual_fraud_transactions", 0) if basic_metrics else 0,
                "high_risk": fraud_levels.get("high_risk", 0) if fraud_levels else 0,
                "medium_risk": fraud_levels.get("medium_risk", 0) if fraud_levels else 0,
                "low_risk": fraud_levels.get("low_risk", 0) if fraud_levels else 0,
                "communities": communities.get("count", 0) if communities else 0
            }
            
            # Log what we're returning
            print(f"Returning metrics: {metrics_data}")
            
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
                "communities": 0
            }
        })

@app.route('/api/suspicious')
def get_suspicious_accounts():
    try:
        with detector.driver.session() as session:
            # Get accounts with high fraud scores
            suspicious = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score > 0.5  // Suspicious score threshold
                RETURN 
                    a.id AS id, 
                    a.fraud_score AS score,
                    a.community AS community,
                    a.pagerank_score AS pagerank,
                    a.degree_score AS degree,
                    a.similarity_score AS similarity,
                    a.tx_imbalance AS imbalance,
                    a.avg_tx_amount AS avg_amount,
                    CASE WHEN a.only_sender = true THEN 1 ELSE 0 END AS only_sender,
                    CASE WHEN a.high_tx_volume = true THEN 1 ELSE 0 END AS high_volume
                ORDER BY score DESC
                LIMIT 15
            """).data()
            
            # Log what we found
            print(f"Found {len(suspicious)} suspicious accounts")
            
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
            
            print(f"Total relationships in database: {rel_count['count']}")
            
            # Use a simpler query to get network data
            top_nodes_result = session.run("""
                // Get ANY accounts with fraud scores and relationships
                MATCH (a:Account)-[r]-(b:Account)
                WHERE a.fraud_score > 0.7
                WITH a, collect(r) AS rels, collect(b) AS connected
                ORDER BY a.fraud_score DESC
                LIMIT 5
                
                // Return the data directly
                RETURN collect(a) AS central_nodes,
                    REDUCE(acc = [], n IN connected | acc + 
                        CASE WHEN n IN collect(a) THEN [] ELSE [n] END) AS connected_nodes,
                    REDUCE(acc = [], r IN rels | acc + [r]) AS relationships
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
                connected_count = len(connected_nodes)
                
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
                rel_count = len(relationships)
                
                for rel in relationships:
                    if rel is None:
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