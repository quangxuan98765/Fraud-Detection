from flask import jsonify
from . import api_bp
from detector.fraud_detector import FraudDetector
from config import FRAUD_SCORE_THRESHOLD, SUSPICIOUS_THRESHOLD

detector = FraudDetector()

@api_bp.route('/status')
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

@api_bp.route('/fraud-stats')
def get_fraud_stats():
    try:
        with detector.driver.session() as session:
            fraud_by_type = session.run("""
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
            communities = session.run("""
                MATCH (a:Account)
                WHERE a.community IS NOT NULL AND a.fraud_score IS NOT NULL
                
                WITH a.community AS community, 
                     COUNT(a) AS count, 
                     AVG(a.fraud_score) AS avg_score,
                     COUNT(CASE WHEN a.fraud_score > $fraud_threshold THEN 1 END) AS high_risk_count
                
                WHERE count >= 3 AND high_risk_count > 0
                
                RETURN 
                    community, 
                    count, 
                    avg_score,
                    high_risk_count,
                    1.0 * high_risk_count / count AS risk_ratio
                ORDER BY risk_ratio DESC, count DESC
                LIMIT 10
            """, fraud_threshold=FRAUD_SCORE_THRESHOLD).data()
            
            # Lấy thêm thống kê phân phối cộng đồng
            community_distribution = session.run("""
                MATCH (a:Account)
                WHERE a.community IS NOT NULL
                
                WITH a.community AS community, COUNT(a) AS size
                
                RETURN 
                    CASE 
                        WHEN size = 1 THEN 'Single'
                        WHEN size >= 2 AND size <= 5 THEN 'Small'
                        WHEN size > 5 AND size <= 20 THEN 'Medium'
                        WHEN size > 20 AND size <= 100 THEN 'Large'
                        ELSE 'Very Large'
                    END AS size_category,
                    COUNT(DISTINCT community) AS community_count
                ORDER BY 
                    CASE size_category
                        WHEN 'Single' THEN 1
                        WHEN 'Small' THEN 2
                        WHEN 'Medium' THEN 3
                        WHEN 'Large' THEN 4
                        ELSE 5
                    END
            """).data()
            
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
            
            transaction_cycles = session.run("""
                MATCH path = (a:Account)-[:SENT*2..4]->(a)
                
                WITH SIZE(nodes(path)) AS cycle_length,
                     COUNT(DISTINCT a) AS unique_accounts,
                     COUNT(path) AS cycle_count
                
                RETURN 
                    cycle_length,
                    cycle_count,
                    unique_accounts
                ORDER BY cycle_length
            """).data()
            
            return jsonify({
                "fraud_by_type": fraud_by_type,
                "communities": communities,
                "community_distribution": community_distribution,
                "score_distribution": score_distribution,
                "transaction_cycles": transaction_cycles
            })
    except Exception as e:
        print(f"Fraud-stats API error: {str(e)}")
        return jsonify({
            "error": str(e),
            "fraud_by_type": [],
            "communities": [],
            "community_distribution": [],
            "score_distribution": None,
            "transaction_cycles": []
        })

@api_bp.route('/metrics')
def get_metrics():
    try:
        with detector.driver.session() as session:
            basic_metrics = session.run("""
                MATCH (a:Account)
                OPTIONAL MATCH (a)-[tx:SENT]->()
                WITH a, tx

                RETURN count(DISTINCT a) AS total_accounts,
                       count(DISTINCT tx) AS total_transactions,
                       count(DISTINCT CASE WHEN a.fraud_score > $fraud_threshold THEN a END) AS detected_fraud_accounts,
                       count(DISTINCT CASE WHEN tx IS NOT NULL AND a.fraud_score > $fraud_threshold THEN tx END) AS detected_fraud_transactions
            """, fraud_threshold=FRAUD_SCORE_THRESHOLD).single()

            fraud_levels = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score IS NOT NULL
                RETURN 
                    count(CASE WHEN a.fraud_score > 0.75 THEN 1 END) AS very_high_risk,
                    count(CASE WHEN a.fraud_score > $fraud_threshold AND a.fraud_score <= 0.75 THEN 1 END) AS high_risk,
                    count(CASE WHEN a.fraud_score > $suspicious_threshold AND a.fraud_score <= $fraud_threshold THEN 1 END) AS medium_risk,
                    count(CASE WHEN a.fraud_score > 0.3 AND a.fraud_score <= $suspicious_threshold THEN 1 END) AS low_risk,
                    count(CASE WHEN a.fraud_score <= 0.3 THEN 1 END) AS very_low_risk
            """, fraud_threshold=FRAUD_SCORE_THRESHOLD,
                suspicious_threshold=SUSPICIOUS_THRESHOLD).single()
            
            communities = session.run("""
                MATCH (a:Account)
                WHERE a.community IS NOT NULL
                WITH a.community AS community, COUNT(a) AS node_count
                WHERE node_count >= 2  // Only count communities with at least 2 accounts
                
                MATCH (m:Account)
                WHERE m.community = community
                
                WITH community, 
                     COUNT(m) AS community_size,
                     AVG(m.fraud_score) AS avg_score,
                     COUNT(CASE WHEN m.fraud_score > $fraud_threshold THEN 1 END) AS high_risk_nodes
                
                RETURN count(DISTINCT community) AS count,
                       count(CASE WHEN avg_score > 0.5 OR high_risk_nodes > 0 THEN 1 END) AS high_risk_communities
            """, fraud_threshold=FRAUD_SCORE_THRESHOLD).single()
            
            cycles = session.run("""
                MATCH path = (a:Account)-[:SENT*2..4]->(a)
                RETURN count(DISTINCT a) AS accounts_in_cycles
            """).single()
            
            # Calculate metrics using actual values from database
            metrics_data = {
                "accounts": basic_metrics.get("total_accounts", 0) if basic_metrics else 0,
                "transactions": basic_metrics.get("total_transactions", 0) if basic_metrics else 0,
                "fraud": basic_metrics.get("detected_fraud_accounts", 0) if basic_metrics else 0,
                "detected_fraud_transactions": basic_metrics.get("detected_fraud_transactions", 0) if basic_metrics else 0,
                "very_high_risk": fraud_levels.get("very_high_risk", 0) if fraud_levels else 0,
                "high_risk": fraud_levels.get("high_risk", 0) if fraud_levels else 0,
                "medium_risk": fraud_levels.get("medium_risk", 0) if fraud_levels else 0,
                "low_risk": fraud_levels.get("low_risk", 0) if fraud_levels else 0,
                "very_low_risk": fraud_levels.get("very_low_risk", 0) if fraud_levels else 0,
                "communities": communities.get("count", 0) if communities else 0,
                "high_risk_communities": communities.get("high_risk_communities", 0) if communities else 0,
                "accounts_in_cycles": cycles.get("accounts_in_cycles", 0) if cycles else 0
            }
              # Sử dụng is_fraud để tính toán metrics (chỉ cho mục đích đánh giá model)
            # Lưu ý: Trong môi trường thực tế, chúng ta không biết giá trị ground truth is_fraud
            try:
                with detector.driver.session() as validation_session:
                    validation_metrics = validation_session.run("""
                        MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
                        RETURN 
                            count(DISTINCT tx) AS total_transactions,
                            count(DISTINCT CASE WHEN sender.fraud_score > $fraud_threshold OR receiver.fraud_score > $fraud_threshold THEN tx END) AS detected_fraud_transactions,
                            count(DISTINCT CASE WHEN tx.is_fraud = 1 THEN tx END) AS ground_truth_frauds,
                            count(DISTINCT CASE WHEN tx.is_fraud = 1 AND (sender.fraud_score > $fraud_threshold OR receiver.fraud_score > $fraud_threshold) THEN tx END) AS true_positives
                    """, fraud_threshold=FRAUD_SCORE_THRESHOLD).single()
                    
                    if validation_metrics:
                        detected_fraud_transactions = validation_metrics.get("detected_fraud_transactions", 0)
                        ground_truth_frauds = validation_metrics.get("ground_truth_frauds", 0)
                        true_positives = validation_metrics.get("true_positives", 0)
                        
                        # Tính toán precision, recall và F1 score từ dữ liệu thực tế
                        precision = true_positives / detected_fraud_transactions if detected_fraud_transactions > 0 else 0
                        recall = true_positives / ground_truth_frauds if ground_truth_frauds > 0 else 0
                        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
                        
                        metrics_data.update({
                            "precision": precision,
                            "recall": recall,
                            "f1_score": f1_score,
                            "true_positives": true_positives,
                            "ground_truth_frauds": ground_truth_frauds,
                            "detected_fraud_transactions": detected_fraud_transactions  # Cập nhật dữ liệu đã phát hiện
                        })
                
                print(f"Metrics calculation complete. Using is_fraud for evaluation purposes.")
            except Exception as validation_error:
                print(f"Warning: Could not calculate validation metrics with is_fraud: {str(validation_error)}")
                # Set default metrics even if validation fails
                metrics_data.update({
                    "precision": 0,
                    "recall": 0,
                    "f1_score": 0,
                    "true_positives": 0,
                    "ground_truth_frauds": 0,
                    "detected_fraud_transactions": 0
                })

            return jsonify({
                "metrics": metrics_data,
                "has_data": True,
                "validated": True  # Flag to indicate these are validated metrics
            })
    except Exception as e:
        print(f"Metrics API error: {str(e)}")
        return jsonify({
            "error": str(e), 
            "has_data": False,
            "metrics": None
        })

@api_bp.route('/suspicious')
def get_suspicious_accounts():
    try:
        with detector.driver.session() as session:
            suspicious = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score > $suspicious_threshold
                
                OPTIONAL MATCH (a)-[out:SENT]->()
                WITH a, count(out) AS out_count, sum(out.amount) AS out_amount
                
                OPTIONAL MATCH ()-[in:SENT]->(a)
                WITH a, out_count, out_amount, count(in) AS in_count, sum(in.amount) AS in_amount
                
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
            """, suspicious_threshold=SUSPICIOUS_THRESHOLD).data()
            
            print(f"Found {len(suspicious)} suspicious accounts")
            
            return jsonify({"suspicious": suspicious})
    except Exception as e:
        print(f"Suspicious accounts API error: {str(e)}")
        return jsonify({"error": str(e), "suspicious": []})

@api_bp.route('/network')
def get_network():
    try:
        with detector.driver.session() as session:
            rel_count = session.run("""
                MATCH ()-[r]->()
                RETURN count(r) as count
            """).single()
            
            print(f"Total relationships in database: {rel_count['count']}")

            top_nodes_result = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score > $suspicious_threshold
                WITH a ORDER BY a.fraud_score DESC LIMIT 10
                
                OPTIONAL MATCH (a)-[r:SENT]-(other:Account)
                WHERE a <> other
                WITH a, other, r
                WHERE other IS NOT NULL
                
                WITH COLLECT(DISTINCT a) + COLLECT(DISTINCT other) AS allNodes,
                     COLLECT(DISTINCT r) AS allRels
                
                WITH 
                    [n IN allNodes WHERE n.fraud_score > $suspicious_threshold] AS central_nodes,
                    [n IN allNodes WHERE n.fraud_score <= $suspicious_threshold OR n.fraud_score IS NULL] AS connected_nodes,
                    allRels AS relationships
                
                RETURN central_nodes, connected_nodes, relationships
            """, suspicious_threshold=SUSPICIOUS_THRESHOLD).single()

            network = {"nodes": [], "links": []}
            node_map = {}

            if top_nodes_result:
                central_nodes = top_nodes_result.get("central_nodes", [])
                connected_nodes = top_nodes_result.get("connected_nodes", [])
                relationships = top_nodes_result.get("relationships", [])

                # Add nodes
                for node in central_nodes:
                    if node and 'id' in node:
                        network["nodes"].append({
                            "id": node["id"],
                            "score": node.get("fraud_score", 0),
                            "type": "central"
                        })
                        node_map[node["id"]] = True

                for node in connected_nodes:
                    if node and 'id' in node and node["id"] not in node_map:
                        network["nodes"].append({
                            "id": node["id"],
                            "score": node.get("fraud_score", 0),
                            "type": "connected"
                        })
                        node_map[node["id"]] = True

                # Add links
                for rel in relationships:
                    if rel and rel.start_node and rel.end_node and "id" in rel.start_node and "id" in rel.end_node:
                        if rel.start_node["id"] in node_map and rel.end_node["id"] in node_map:
                            network["links"].append({
                                "source": rel.start_node["id"],
                                "target": rel.end_node["id"],
                                "value": rel.get("amount", 1),
                                "is_fraud": rel.start_node.get("fraud_score", 0) > 0.7 or rel.end_node.get("fraud_score", 0) > 0.7
                            })
            
            print(f"Network data prepared: {len(network['nodes'])} nodes, {len(network['links'])} links")
            return jsonify({"network": network})
    except Exception as e:
        print(f"Network API error: {str(e)}")
        return jsonify({"error": str(e), "network": {"nodes": [], "links": []}})

@api_bp.route('/reanalyze', methods=['POST'])
def reanalyze():
    try:
        # Thực hiện phân tích lại
        success = detector.analyze_fraud()
        if success:
            return jsonify({
                "success": True,
                "message": "Phân tích gian lận đã chạy thành công"
            })
        else:
            return jsonify({
                "success": False,
                "message": "Có lỗi khi thực hiện phân tích gian lận"
            })
    except Exception as e:
        print(f"Reanalyze API error: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        })

@api_bp.route('/debug-metrics')
def debug_metrics():
    try:
        with detector.driver.session() as session:            
            # Truy vấn trực tiếp các ngưỡng và số lượng giao dịch              
            debug_info = session.run("""
                MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
                
                RETURN count(DISTINCT sender) + count(DISTINCT receiver) AS total_accounts,
                       count(DISTINCT tx) AS total_transactions,
                       count(DISTINCT CASE WHEN sender.fraud_score > 0.7 THEN sender END) + 
                          count(DISTINCT CASE WHEN receiver.fraud_score > 0.7 THEN receiver END) AS fraud_accounts_07,
                       count(DISTINCT CASE WHEN sender.fraud_score > 0.6 THEN sender END) + 
                          count(DISTINCT CASE WHEN receiver.fraud_score > 0.6 THEN receiver END) AS fraud_accounts_06,
                       count(DISTINCT CASE WHEN sender.fraud_score > 0.5 THEN sender END) + 
                          count(DISTINCT CASE WHEN receiver.fraud_score > 0.5 THEN receiver END) AS fraud_accounts_05,
                       
                       count(DISTINCT CASE WHEN sender.fraud_score > 0.7 OR receiver.fraud_score > 0.7 THEN tx END) AS fraud_transactions_07,
                       count(DISTINCT CASE WHEN sender.fraud_score > 0.6 OR receiver.fraud_score > 0.6 THEN tx END) AS fraud_transactions_06,
                       count(DISTINCT CASE WHEN sender.fraud_score > 0.5 OR receiver.fraud_score > 0.5 THEN tx END) AS fraud_transactions_05,
                       
                       // Count of actual fraud transactions from ground truth (for evaluation only)
                       count(DISTINCT CASE WHEN tx.is_fraud = 1 THEN tx END) AS real_fraud_transactions,
                       
                       // True positives at different thresholds (using is_fraud for evaluation only)
                       count(DISTINCT CASE WHEN tx.is_fraud = 1 AND (sender.fraud_score > 0.7 OR receiver.fraud_score > 0.7) THEN tx END) AS true_positives_07,
                       count(DISTINCT CASE WHEN tx.is_fraud = 1 AND (sender.fraud_score > 0.6 OR receiver.fraud_score > 0.6) THEN tx END) AS true_positives_06,
                       count(DISTINCT CASE WHEN tx.is_fraud = 1 AND (sender.fraud_score > 0.5 OR receiver.fraud_score > 0.5) THEN tx END) AS true_positives_05
            """).single()
            
            # Convert Neo4j Record to a Python dictionary
            metrics_dict = {}
            if debug_info:
                for key in debug_info.keys():
                    metrics_dict[key] = debug_info[key]
            
            # Truy vấn cấu hình hiện tại
            config_info = {
                "FRAUD_SCORE_THRESHOLD": FRAUD_SCORE_THRESHOLD,
                "SUSPICIOUS_THRESHOLD": SUSPICIOUS_THRESHOLD
            }
            
            return jsonify({
                "debug_metrics": metrics_dict,
                "config": config_info
            })
            
    except Exception as e:
        print(f"Debug metrics API error: {str(e)}")
        return jsonify({"error": str(e)})

@api_bp.route('/community/<community_id>')
def get_community_details(community_id):
    try:
        print(f"API: Đang xử lý yêu cầu lấy chi tiết cộng đồng ID: {community_id}")
        with detector.driver.session() as session:
            # Lấy thông tin tổng quan về cộng đồng
            community_overview = session.run("""
                MATCH (a:Account)
                WHERE a.community = $community_id
                
                WITH count(a) AS total_accounts,
                     avg(a.fraud_score) AS avg_score,
                     count(CASE WHEN a.fraud_score > $fraud_threshold THEN 1 END) AS high_risk_accounts
                
                RETURN 
                    total_accounts,
                    avg_score,
                    high_risk_accounts,
                    1.0 * high_risk_accounts / total_accounts AS risk_ratio
            """, community_id=community_id, fraud_threshold=FRAUD_SCORE_THRESHOLD).single()
            
            print(f"API: Kết quả tổng quan cộng đồng ID {community_id}: {community_overview}")
            
            # Lấy danh sách các tài khoản trong cộng đồng
            community_accounts = session.run("""
                MATCH (a:Account)
                WHERE a.community = $community_id
                
                OPTIONAL MATCH (a)-[out:SENT]->()
                WITH a, count(out) AS out_count, sum(out.amount) AS out_amount
                
                OPTIONAL MATCH ()-[in:SENT]->(a)
                WITH a, out_count, out_amount, count(in) AS in_count, sum(in.amount) AS in_amount
                
                OPTIONAL MATCH path = (a)-[:SENT*2..3]->(a)
                WITH a, out_count, out_amount, in_count, in_amount, 
                     count(path) > 0 AS has_cycle
                
                RETURN 
                    a.id AS id, 
                    a.fraud_score AS score,
                    COALESCE(a.pagerank_score, 0) AS pagerank,
                    COALESCE(a.tx_imbalance, 0) AS imbalance,
                    CASE WHEN has_cycle THEN 1 ELSE 0 END AS in_cycle,
                    out_count AS sent_count,
                    in_count AS received_count,
                    out_amount AS sent_amount,
                    in_amount AS received_amount,
                    abs(out_amount - in_amount) AS imbalance_amount
                ORDER BY score DESC
            """, community_id=community_id).data()
            
            print(f"API: Tìm thấy {len(community_accounts)} tài khoản trong cộng đồng {community_id}")
            
            # Lấy thông tin về các giao dịch trong cộng đồng
            community_transactions = session.run("""
                MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
                WHERE sender.community = $community_id AND receiver.community = $community_id
                
                RETURN 
                    sender.id AS source,
                    receiver.id AS target,
                    tx.amount AS amount,
                    CASE WHEN sender.fraud_score > 0.7 OR receiver.fraud_score > 0.7 THEN 1 ELSE 0 END AS is_fraud,
                    COALESCE(tx.type, 'TRANSFER') AS type,
                    sender.fraud_score > $fraud_threshold OR receiver.fraud_score > $fraud_threshold AS high_risk
                ORDER BY amount DESC
                LIMIT 100  // Giới hạn số giao dịch để tránh quá tải
            """, community_id=community_id, fraud_threshold=FRAUD_SCORE_THRESHOLD).data()
            
            print(f"API: Tìm thấy {len(community_transactions)} giao dịch nội bộ trong cộng đồng {community_id}")
            
            # Lấy thông tin về mối quan hệ với các cộng đồng khác
            related_communities = session.run("""
                MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
                WHERE 
                    (sender.community = $community_id AND receiver.community <> $community_id)
                    OR (sender.community <> $community_id AND receiver.community = $community_id)
                    
                WITH 
                    CASE WHEN sender.community = $community_id THEN receiver.community ELSE sender.community END AS related_community,
                    count(tx) AS transaction_count,
                    sum(tx.amount) AS total_amount
                
                MATCH (a:Account)
                WHERE a.community = related_community
                
                WITH related_community, transaction_count, total_amount, count(a) AS account_count, avg(a.fraud_score) AS avg_score
                
                RETURN 
                    related_community AS id,
                    transaction_count,
                    total_amount,
                    account_count,
                    avg_score
                ORDER BY transaction_count DESC
                LIMIT 5
            """, community_id=community_id).data()
            
            print(f"API: Tìm thấy {len(related_communities)} cộng đồng liên quan đến cộng đồng {community_id}")
            
            response_data = {
                "overview": community_overview if community_overview else {},
                "accounts": community_accounts,
                "transactions": community_transactions,
                "related_communities": related_communities
            }
            
            print(f"API: Trả về dữ liệu chi tiết cộng đồng {community_id}")
            return jsonify(response_data)
    except Exception as e:
        print(f"Community details API error for community {community_id}: {str(e)}")
        return jsonify({
            "error": str(e),
            "overview": {},
            "accounts": [],
            "transactions": [],
            "related_communities": []
        })

@api_bp.route('/communities')
def get_all_communities():
    try:
        with detector.driver.session() as session:
            # Lấy danh sách tất cả các cộng đồng với thông tin tóm tắt
            all_communities = session.run("""
                MATCH (a:Account)
                WHERE a.community IS NOT NULL
                
                WITH a.community AS community_id, COUNT(a) AS account_count, AVG(a.fraud_score) AS avg_score
                WHERE account_count >= 2  // Chỉ hiển thị cộng đồng có ít nhất 2 tài khoản
                
                WITH 
                    community_id, 
                    account_count, 
                    avg_score,
                    count(CASE WHEN avg_score > $fraud_threshold THEN 1 END) AS high_risk,
                    1.0 * count(CASE WHEN avg_score > $fraud_threshold THEN 1 END) / account_count AS risk_ratio
                
                RETURN 
                    community_id AS id,
                    account_count AS size,
                    avg_score,
                    CASE
                        WHEN account_count <= 3 THEN 'small'
                        WHEN account_count <= 10 THEN 'medium'
                        ELSE 'large'
                    END AS size_category,
                    CASE
                        WHEN avg_score > 0.7 THEN 'high'
                        WHEN avg_score > 0.5 THEN 'medium'
                        ELSE 'low'
                    END AS risk_level,
                    risk_ratio
                ORDER BY risk_ratio DESC, account_count DESC
                LIMIT 200
            """, fraud_threshold=FRAUD_SCORE_THRESHOLD).data()
            
            return jsonify({
                "communities": all_communities
            })
    except Exception as e:
        print(f"All communities API error: {str(e)}")
        return jsonify({
            "error": str(e),
            "communities": []
        })

@api_bp.route('/fraud-transactions/<transaction_type>')
def get_fraud_transactions(transaction_type):
    try:
        with detector.driver.session() as session:
            transactions = session.run("""
                MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
                WHERE (sender.fraud_score > $fraud_threshold OR receiver.fraud_score > $fraud_threshold)
                AND (CASE 
                    WHEN $type = 'all' THEN true
                    WHEN tx.type IS NULL AND $type = 'other' THEN true
                    ELSE tx.type = $type 
                END)
                
                RETURN 
                    sender.id AS source,
                    sender.fraud_score AS source_score,
                    receiver.id AS target,
                    receiver.fraud_score AS target_score,
                    tx.amount AS amount,
                    tx.timestamp AS timestamp,
                    COALESCE(tx.type, 'Khác') AS type,
                    CASE 
                        WHEN sender.fraud_score > 0.7 OR receiver.fraud_score > 0.7 THEN true
                        ELSE false
                    END AS is_fraud,
                    sender.community AS source_community,
                    receiver.community AS target_community
                ORDER BY tx.amount DESC
                LIMIT 100
            """, type=transaction_type, fraud_threshold=FRAUD_SCORE_THRESHOLD).data()
            
            return jsonify({
                "transactions": transactions,
                "total": len(transactions)
            })
            
    except Exception as e:
        print(f"Fraud transactions API error: {str(e)}")
        return jsonify({
            "error": str(e),
            "transactions": [],
            "total": 0
        })
