from flask import jsonify
import traceback
from . import api_bp
from detector.fraud_detector import FraudDetector
from config import FRAUD_SCORE_THRESHOLD, SUSPICIOUS_THRESHOLD, HIGH_RISK_THRESHOLD, VERY_HIGH_RISK_THRESHOLD
from config import MODEL1_WEIGHT, MODEL2_WEIGHT, MODEL3_WEIGHT
from queries.api_queries import ApiQueries

detector = FraudDetector()
queries = ApiQueries()

@api_bp.route('/status')
def get_status():
    try:
        has_data, stats = detector.check_data()
        # Thêm thông tin về phân tích
        with detector.driver.session() as session:
            has_analysis = session.run(queries.HAS_ANALYSIS_QUERY).single()
            
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
            fraud_by_type = session.run(queries.FRAUD_BY_TYPE_QUERY).data()
            communities = session.run(queries.COMMUNITIES_QUERY, 
                                     fraud_threshold=FRAUD_SCORE_THRESHOLD).data()
            
            # Lấy thêm thống kê phân phối cộng đồng
            community_distribution = session.run(queries.COMMUNITY_DISTRIBUTION_QUERY).data()
            
            score_distribution = session.run(queries.SCORE_DISTRIBUTION_QUERY).single()
            
            transaction_cycles = session.run(queries.TRANSACTION_CYCLES_QUERY).data()
            
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
            basic_metrics = session.run(queries.BASIC_METRICS_QUERY,
                                       fraud_threshold=FRAUD_SCORE_THRESHOLD).single()

            fraud_levels = session.run(queries.FRAUD_LEVELS_QUERY,
                                      fraud_threshold=FRAUD_SCORE_THRESHOLD,
                                      suspicious_threshold=SUSPICIOUS_THRESHOLD).single()
            
            communities = session.run(queries.COMMUNITY_METRICS_QUERY,
                                     fraud_threshold=FRAUD_SCORE_THRESHOLD).single()
            
            cycles = session.run(queries.CYCLES_QUERY).single()
            
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
                    validation_metrics = validation_session.run(queries.VALIDATION_METRICS_QUERY,
                                                             fraud_threshold=FRAUD_SCORE_THRESHOLD).single()
                    
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
            suspicious = session.run(queries.SUSPICIOUS_ACCOUNTS_QUERY,
                                    suspicious_threshold=SUSPICIOUS_THRESHOLD).data()
            
            print(f"Found {len(suspicious)} suspicious accounts")
            
            return jsonify({"suspicious": suspicious})
    except Exception as e:
        print(f"Suspicious accounts API error: {str(e)}")
        return jsonify({"error": str(e), "suspicious": []})

@api_bp.route('/network')
def get_network():
    try:
        with detector.driver.session() as session:
            rel_count = session.run(queries.REL_COUNT_QUERY).single()
            
            print(f"Total relationships in database: {rel_count['count']}")

            top_nodes_result = session.run(queries.TOP_NODES_QUERY,
                                          suspicious_threshold=SUSPICIOUS_THRESHOLD).single()

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
            # Console log cho debugging
            print("Bắt đầu thu thập debug metrics...")
            
            # Tạo object chứa kết quả
            metrics = {}
            
            # Kiểm tra loại relationship thực tế trong database
            rel_types = session.run("""
                MATCH ()-[r]->() 
                RETURN type(r) as type, count(r) as count 
                ORDER BY count DESC
            """).data()
            
            # Log các loại relationship tìm thấy
            for r in rel_types:
                print(f"Relationship type: {r['type']} - Count: {r['count']}")
            
            # Số liệu tài khoản
            account_data = session.run("""
                MATCH (a:Account)
                RETURN count(a) as total_accounts
            """).single()
            
            if account_data:
                metrics["account_count"] = account_data["total_accounts"] 
                print(f"Tìm thấy {metrics['account_count']} tài khoản")
            else:
                metrics["account_count"] = 0
              # Số liệu giao dịch - sử dụng SENT thay vì TRANSFER
            txn_data = session.run("""
                MATCH ()-[r:SENT]->()
                RETURN count(r) as total_transactions
            """).single()
            
            if txn_data:
                metrics["transaction_count"] = txn_data["total_transactions"]
                print(f"Tìm thấy {metrics['transaction_count']} giao dịch")
            else:
                metrics["transaction_count"] = 0
            
            # Số tài khoản theo ngưỡng
            threshold_accounts = session.run("""
                MATCH (a:Account)
                WHERE a.fraud_score IS NOT NULL
                RETURN 
                    count(CASE WHEN a.fraud_score >= 0.5 THEN a END) as accounts_05,
                    count(CASE WHEN a.fraud_score >= 0.6 THEN a END) as accounts_06,
                    count(CASE WHEN a.fraud_score >= 0.7 THEN a END) as accounts_07
            """).single()
            
            if threshold_accounts:
                metrics["fraud_accounts_05"] = threshold_accounts["accounts_05"] or 0
                metrics["fraud_accounts_06"] = threshold_accounts["accounts_06"] or 0
                metrics["fraud_accounts_07"] = threshold_accounts["accounts_07"] or 0
                print(f"Tài khoản theo ngưỡng: 0.5={metrics['fraud_accounts_05']}, 0.6={metrics['fraud_accounts_06']}, 0.7={metrics['fraud_accounts_07']}")
            else:
                metrics["fraud_accounts_05"] = 0
                metrics["fraud_accounts_06"] = 0
                metrics["fraud_accounts_07"] = 0            # Số giao dịch theo ngưỡng - cần tính cả tài khoản nguồn VÀ đích
            tx_thresholds = session.run("""
                MATCH (src:Account)-[t:SENT]->(tgt:Account)
                WHERE src.fraud_score IS NOT NULL OR tgt.fraud_score IS NOT NULL
                RETURN 
                    count(CASE WHEN src.fraud_score >= 0.5 OR tgt.fraud_score >= 0.5 THEN t END) as txs_05,
                    count(CASE WHEN src.fraud_score >= 0.6 OR tgt.fraud_score >= 0.6 THEN t END) as txs_06,
                    count(CASE WHEN src.fraud_score >= 0.7 OR tgt.fraud_score >= 0.7 THEN t END) as txs_07
            """).single()
            
            if tx_thresholds:
                metrics["fraud_transactions_05"] = tx_thresholds["txs_05"] or 0
                metrics["fraud_transactions_06"] = tx_thresholds["txs_06"] or 0
                metrics["fraud_transactions_07"] = tx_thresholds["txs_07"] or 0
                print(f"Giao dịch theo ngưỡng: 0.5={metrics['fraud_transactions_05']}, 0.6={metrics['fraud_transactions_06']}, 0.7={metrics['fraud_transactions_07']}")
            else:
                metrics["fraud_transactions_05"] = 0
                metrics["fraud_transactions_06"] = 0
                metrics["fraud_transactions_07"] = 0
            
            # Thu thập số lượng giao dịch thực sự gian lận (is_fraud = 1)
            real_fraud_data = session.run("""
                MATCH ()-[r:SENT]->()
                WHERE r.is_fraud = 1
                RETURN count(r) as fraud_count
            """).single()
            
            if real_fraud_data:
                metrics["real_fraud_transactions"] = real_fraud_data["fraud_count"] or 0
                print(f"Giao dịch thực sự gian lận: {metrics['real_fraud_transactions']}")
            else:
                metrics["real_fraud_transactions"] = 0
            
            # Thu thập true positives ở các ngưỡng khác nhau
            true_positives = session.run("""
                MATCH (src:Account)-[tx:SENT]->(tgt:Account)
                WHERE tx.is_fraud = 1
                RETURN 
                    count(DISTINCT CASE WHEN src.fraud_score >= 0.5 OR tgt.fraud_score >= 0.5 THEN tx END) as tp_05,
                    count(DISTINCT CASE WHEN src.fraud_score >= 0.6 OR tgt.fraud_score >= 0.6 THEN tx END) as tp_06,
                    count(DISTINCT CASE WHEN src.fraud_score >= 0.7 OR tgt.fraud_score >= 0.7 THEN tx END) as tp_07
            """).single()
            
            if true_positives:
                metrics["true_positives_05"] = true_positives["tp_05"] or 0
                metrics["true_positives_06"] = true_positives["tp_06"] or 0
                metrics["true_positives_07"] = true_positives["tp_07"] or 0
                print(f"True positives: 0.5={metrics['true_positives_05']}, 0.6={metrics['true_positives_06']}, 0.7={metrics['true_positives_07']}")
            else:
                metrics["true_positives_05"] = 0
                metrics["true_positives_06"] = 0
                metrics["true_positives_07"] = 0
                
            # Thu thập metrics từ các mô hình - tính cả tài khoản nguồn VÀ đích
            model_metrics = session.run("""
                MATCH (src:Account)-[t:SENT]->(tgt:Account)
                RETURN 
                    count(CASE WHEN src.model1_score > 0.5 OR tgt.model1_score > 0.5 THEN t END) as model1_txs,
                    count(CASE WHEN src.model2_score > 0.5 OR tgt.model2_score > 0.5 THEN t END) as model2_txs,
                    count(CASE WHEN src.model3_score > 0.5 OR tgt.model3_score > 0.5 THEN t END) as model3_txs,
                    count(CASE WHEN src.high_confidence_pattern = true OR tgt.high_confidence_pattern = true THEN t END) as high_confidence_txs,
                    count(CASE WHEN src.funnel_pattern = true OR tgt.funnel_pattern = true THEN t END) as funnel_txs,
                    count(CASE WHEN src.round_pattern = true OR tgt.round_pattern = true THEN t END) as round_txs,
                    count(CASE WHEN src.chain_pattern = true OR tgt.chain_pattern = true THEN t END) as chain_txs,
                    count(CASE WHEN src.similar_to_fraud = true OR tgt.similar_to_fraud = true THEN t END) as similar_txs,
                    count(CASE WHEN src.high_velocity = true OR tgt.high_velocity = true THEN t END) as velocity_txs
            """).single()
            
            if model_metrics:
                # Đảm bảo không có giá trị null sau khi trả về
                metrics["model1_transactions"] = model_metrics.get("model1_txs", 0) or 0
                metrics["model2_transactions"] = model_metrics.get("model2_txs", 0) or 0
                metrics["model3_transactions"] = model_metrics.get("model3_txs", 0) or 0
                metrics["high_confidence_transactions"] = model_metrics.get("high_confidence_txs", 0) or 0
                metrics["funnel_disperse_transactions"] = model_metrics.get("funnel_txs", 0) or 0
                metrics["round_tx_transactions"] = model_metrics.get("round_txs", 0) or 0
                metrics["chain_transactions"] = model_metrics.get("chain_txs", 0) or 0
                metrics["similarity_transactions"] = model_metrics.get("similar_txs", 0) or 0
                metrics["velocity_transactions"] = model_metrics.get("velocity_txs", 0) or 0
            else:
                # Đặt tất cả về 0 nếu không có kết quả
                metrics["model1_transactions"] = 0
                metrics["model2_transactions"] = 0
                metrics["model3_transactions"] = 0
                metrics["high_confidence_transactions"] = 0
                metrics["funnel_disperse_transactions"] = 0
                metrics["round_tx_transactions"] = 0
                metrics["chain_transactions"] = 0
                metrics["similarity_transactions"] = 0
                metrics["velocity_transactions"] = 0
            
            # Trả về kết quả cuối cùng
            return jsonify({
                "config": {
                    "FRAUD_SCORE_THRESHOLD": FRAUD_SCORE_THRESHOLD,
                    "SUSPICIOUS_THRESHOLD": SUSPICIOUS_THRESHOLD
                },
                "debug_metrics": metrics
            })
            
    except Exception as e:
        print(f"Error collecting debug metrics: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": f"Error collecting debug metrics: {str(e)}",
            "config": {
                "FRAUD_SCORE_THRESHOLD": FRAUD_SCORE_THRESHOLD,
                "SUSPICIOUS_THRESHOLD": SUSPICIOUS_THRESHOLD
            },
            "debug_metrics": {
                "account_count": 0,
                "transaction_count": 0,
                "real_fraud_transactions": 0,
                "fraud_accounts_05": 0,
                "fraud_accounts_06": 0,
                "fraud_accounts_07": 0,
                "fraud_transactions_05": 0,
                "fraud_transactions_06": 0,
                "fraud_transactions_07": 0,
                "true_positives_05": 0,
                "true_positives_06": 0,
                "true_positives_07": 0,
                "model1_transactions": 0,
                "model2_transactions": 0,
                "model3_transactions": 0,
                "high_confidence_transactions": 0,
                "funnel_disperse_transactions": 0,
                "round_tx_transactions": 0,
                "chain_transactions": 0,
                "similarity_transactions": 0,
                "velocity_transactions": 0
            }
        }), 500            

@api_bp.route('/community/<community_id>')
def get_community_details(community_id):
    try:
        print(f"API: Đang xử lý yêu cầu lấy chi tiết cộng đồng ID: {community_id}")
        with detector.driver.session() as session:
            # Lấy thông tin tổng quan về cộng đồng
            community_overview = session.run(queries.COMMUNITY_OVERVIEW_QUERY,
                                           community_id=community_id,
                                           fraud_threshold=FRAUD_SCORE_THRESHOLD).single()
            
            print(f"API: Kết quả tổng quan cộng đồng ID {community_id}: {community_overview}")
            
            # Lấy danh sách các tài khoản trong cộng đồng
            community_accounts = session.run(queries.COMMUNITY_ACCOUNTS_QUERY,
                                           community_id=community_id).data()
            
            print(f"API: Tìm thấy {len(community_accounts)} tài khoản trong cộng đồng {community_id}")
            
            # Lấy thông tin về các giao dịch trong cộng đồng
            community_transactions = session.run(queries.COMMUNITY_TRANSACTIONS_QUERY,
                                               community_id=community_id,
                                               fraud_threshold=FRAUD_SCORE_THRESHOLD).data()
            
            print(f"API: Tìm thấy {len(community_transactions)} giao dịch nội bộ trong cộng đồng {community_id}")
            
            # Lấy thông tin về mối quan hệ với các cộng đồng khác
            related_communities = session.run(queries.RELATED_COMMUNITIES_QUERY,
                                            community_id=community_id).data()
            
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
            all_communities = session.run(queries.ALL_COMMUNITIES_QUERY,
                                        fraud_threshold=FRAUD_SCORE_THRESHOLD).data()
            
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
            transactions = session.run(queries.FRAUD_TRANSACTIONS_QUERY,
                                      type=transaction_type,
                                      fraud_threshold=FRAUD_SCORE_THRESHOLD).data()
            
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