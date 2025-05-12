from flask import jsonify
from . import api_bp
from detector.fraud_detector import FraudDetector
from config import FRAUD_SCORE_THRESHOLD, SUSPICIOUS_THRESHOLD
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
            # Truy vấn cập nhật với các thuộc tính mới từ bộ phát hiện tối ưu
            debug_info = session.run(queries.DEBUG_METRICS_QUERY).single()
            
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