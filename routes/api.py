from flask import jsonify
from . import api_bp
from fraud_detector import FraudDetector
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
                LIMIT 8
            """, fraud_threshold=FRAUD_SCORE_THRESHOLD).data()
            
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
                "score_distribution": score_distribution,
                "transaction_cycles": transaction_cycles
            })
    except Exception as e:
        print(f"Fraud-stats API error: {str(e)}")
        return jsonify({
            "error": str(e),
            "fraud_by_type": [],
            "communities": [],
            "score_distribution": None,
            "transaction_cycles": []
        })

@api_bp.route('/metrics')
def get_metrics():
    try:
        with detector.driver.session() as session:
            basic_metrics = session.run("""
                MATCH (a:Account)
                WITH count(a) AS total_accounts
                
                MATCH ()-[r:SENT]->() 
                WITH total_accounts, count(r) AS total_transactions
                
                MATCH (f:Account)
                WHERE f.fraud_score > $fraud_threshold
                WITH total_accounts, total_transactions, count(f) AS detected_fraud_accounts
                
                MATCH (s1:Account)-[r1:SENT]->(t1:Account)
                WHERE s1.fraud_score > $fraud_threshold OR t1.fraud_score > $fraud_threshold
                WITH total_accounts, total_transactions, detected_fraud_accounts, count(r1) AS detected_fraud_transactions
                
                MATCH (s2:Account)-[r2:SENT {is_fraud: 1}]->(t2:Account)
                WHERE s2.fraud_score > $fraud_threshold OR t2.fraud_score > $fraud_threshold
                WITH total_accounts, total_transactions, detected_fraud_accounts, 
                    detected_fraud_transactions, count(r2) AS true_positives
                
                MATCH ()-[r3:SENT {is_fraud: 1}]->()
                
                RETURN total_accounts, 
                    total_transactions,
                    detected_fraud_accounts,
                    detected_fraud_transactions,
                    true_positives,
                    count(r3) AS ground_truth_frauds
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
            """, 
                fraud_threshold=FRAUD_SCORE_THRESHOLD,
                suspicious_threshold=SUSPICIOUS_THRESHOLD).single()

            communities = session.run("""
                MATCH (a:Account)
                WHERE a.community IS NOT NULL 
                WITH a.community AS community, count(a) AS size, avg(a.fraud_score) AS avg_score
                RETURN count(DISTINCT community) AS count,
                       count(CASE WHEN avg_score > $fraud_threshold THEN 1 END) AS high_risk_communities
            """, fraud_threshold=FRAUD_SCORE_THRESHOLD).single()
            
            cycles = session.run("""
                MATCH path = (a:Account)-[:SENT*2..4]->(a)
                RETURN count(DISTINCT a) AS accounts_in_cycles
            """).single()
            
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
                "accounts_in_cycles": cycles.get("accounts_in_cycles", 0) if cycles else 0,
                "precision": 0,
                "recall": 0,
                "f1_score": 0
            }
            
            # Calculate performance metrics
            if basic_metrics:
                true_positives = basic_metrics.get("true_positives", 0)
                detected = basic_metrics.get("detected_fraud_transactions", 0)
                actual = basic_metrics.get("ground_truth_frauds", 0)

                if detected == 0 and actual == 0:
                    precision = 0
                    recall = 0
                elif detected == 0:
                    precision = 0
                    recall = 0
                elif actual == 0:
                    precision = 0
                    recall = 100.0
                else:
                    precision = true_positives / detected if detected > 0 else 0
                    recall = true_positives / actual if actual > 0 else 0
                
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
                    if rel.start_node["id"] in node_map and rel.end_node["id"] in node_map:
                        network["links"].append({
                            "source": rel.start_node["id"],
                            "target": rel.end_node["id"],
                            "amount": rel["amount"],
                            "type": rel.get("type", "unknown")
                        })

            return jsonify(network)
    except Exception as e:
        print(f"Network API error: {str(e)}")
        return jsonify({"error": str(e), "nodes": [], "links": []})
