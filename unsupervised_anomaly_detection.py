"""
Unsupervised Anomaly Detection Pipeline Using Neo4j Graph Data Science
Author: Xuan Quang
Date: May 14, 2025

Pipeline này thực hiện phát hiện gian lận không giám sát sử dụng các thuật toán học máy dựa trên đồ thị
để tính toán điểm bất thường (anomaly score) cho mỗi giao dịch.
"""

from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import time
import json
import os
import logging

# Disable Neo4j driver's INFO and WARNING logs
logging.getLogger("neo4j").setLevel(logging.ERROR)

# Cấu hình kết nối Neo4j
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"  # Thay đổi password nếu cần

class UnsupervisedFraudDetection:
    def __init__(self, uri, user, password):
        """Khởi tạo kết nối Neo4j và cấu hình phát hiện gian lận."""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.weights = {
            'degScore': 0.20,
            'prScore': 0.20,
            'normCommunitySize': 0.15,  # Inverted in calculation (1-normCommunitySize)
            'simScore': 0.10,
            'btwScore': 0.10,
            'hubScore': 0.05,
            'authScore': 0.05,
            'coreScore': 0.05,
            'triCount': 0.05,
            'cycleCount': 0.05,
            'tempBurst': 0.05
        }
        self.percentile_cutoff = 0.95  # Ngưỡng phân vị 95% mặc định
        
    def close(self):
        """Đóng kết nối Neo4j."""
        self.driver.close()
        
    def run_query(self, query, params=None):
        """Chạy truy vấn Cypher trên Neo4j và trả về tất cả các bản ghi."""
        with self.driver.session() as session:
            try:
                if params:
                    result = session.run(query, params)
                else:
                    result = session.run(query)
                    
                # Check if the result has records without consuming them
                has_records = result.peek() is not None
                
                if not has_records:
                    # For queries that don't return data (CREATE, SET, DELETE, ...)
                    return None
                else:
                    # For queries that return data, collect them first to avoid consumption issues
                    data = result.data()
                    if len(data) == 0:
                        return None
                        
                    # If we expect to use single() later (like in calculate_community_sizes)
                    # Return the first record directly
                    return data[0]
            except Exception as e:
                print(f"Query error: {str(e)}")
                raise e

    def examine_data(self):
        """
        Kiểm tra và xác thực dữ liệu trước khi thực hiện phân tích.
        - Kiểm tra sự phân bố của ground truth data
        - Kiểm tra kiểu dữ liệu của các đặc trưng
        - Kiểm tra sự nhất quán và đầy đủ của dữ liệu
        """
        print("🔍 Đang kiểm tra dữ liệu...")
        
        # 1. Kiểm tra sự tồn tại và phân bố của ground_truth_fraud
        ground_truth_query = """
        MATCH ()-[tx:SENT]->()
        WITH 
            COUNT(tx) AS total,
            SUM(CASE WHEN tx.ground_truth_fraud IS NOT NULL THEN 1 ELSE 0 END) AS has_ground_truth,
            SUM(CASE WHEN tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS fraud_cases
        RETURN 
            total, 
            has_ground_truth, 
            fraud_cases,
            toFloat(has_ground_truth) / total AS coverage_ratio,
            CASE WHEN has_ground_truth > 0 
                THEN toFloat(fraud_cases) / has_ground_truth 
                ELSE 0 
            END AS fraud_ratio
        """
        
        result = self.run_query(ground_truth_query)
        
        if result:
            total = result["total"]
            has_ground_truth = result["has_ground_truth"]
            fraud_cases = result["fraud_cases"]
            coverage_ratio = result["coverage_ratio"]
            fraud_ratio = result["fraud_ratio"]
            
            print(f"\n📊 Phân tích ground truth data:")
            print(f"  • Tổng số giao dịch: {total}")
            print(f"  • Số giao dịch có ground truth: {has_ground_truth} ({coverage_ratio*100:.2f}%)")
            print(f"  • Số giao dịch gian lận: {fraud_cases} ({fraud_ratio*100:.2f}%)")
            
            # Cảnh báo nếu tỷ lệ phủ quá thấp
            if coverage_ratio < 0.5:
                print(f"  ⚠️ Cảnh báo: Chỉ {coverage_ratio*100:.2f}% giao dịch có ground truth data.")
            
            # Cảnh báo nếu tỷ lệ gian lận quá cao hoặc quá thấp
            if fraud_ratio < 0.001:
                print(f"  ⚠️ Cảnh báo: Tỷ lệ gian lận quá thấp ({fraud_ratio*100:.4f}%).")
            elif fraud_ratio > 0.2:
                print(f"  ⚠️ Cảnh báo: Tỷ lệ gian lận quá cao ({fraud_ratio*100:.2f}%).")
        else:
            print("  ❌ Không thể truy vấn thông tin ground truth data.")
        
        # 2. Kiểm tra kiểu dữ liệu của ground_truth_fraud
        type_check_query = """
        MATCH ()-[tx:SENT]->()
        WHERE tx.ground_truth_fraud IS NOT NULL
        RETURN 
            CASE 
                WHEN toString(tx.ground_truth_fraud) IN ['true', 'false'] THEN 'String'
                WHEN toString(tx.ground_truth_fraud) IN ['0', '1'] THEN 'String'
                WHEN tx.ground_truth_fraud IN [true, false] THEN 'Boolean'
                ELSE 'Unknown'
            END AS data_type,
            COUNT(*) as count
        """
        
        try:
            # Run this query directly since it uses APOC
            with self.driver.session() as session:
                type_check_result = session.run(type_check_query).data()
                
            if type_check_result:
                print("\n📊 Kiểu dữ liệu của ground_truth_fraud:")
                for record in type_check_result:
                    print(f"  • {record['data_type']}: {record['count']} giao dịch")
                    
                    # Cảnh báo nếu có kiểu dữ liệu không phải boolean
                    if record['data_type'] != 'Boolean' and record['data_type'] != 'boolean':
                        print(f"  ⚠️ Cảnh báo: ground_truth_fraud có kiểu dữ liệu {record['data_type']} thay vì Boolean.")
        except Exception as e:
            print(f"  ❌ Không thể kiểm tra kiểu dữ liệu: {str(e)}")
            print("  🔄 Sẽ tiếp tục với giả định ground_truth_fraud là String hoặc Boolean.")
        
        # 3. Kiểm tra phân phối của anomaly score (nếu có)
        score_distribution_query = """
        MATCH ()-[tx:SENT]->()
        WHERE tx.anomaly_score IS NOT NULL
        WITH 
            MIN(tx.anomaly_score) AS min_score,
            MAX(tx.anomaly_score) AS max_score,
            AVG(tx.anomaly_score) AS avg_score,
            STDEV(tx.anomaly_score) AS std_score,
            percentileCont(tx.anomaly_score, 0.5) AS median_score,
            percentileCont(tx.anomaly_score, 0.95) AS p95_score,
            percentileCont(tx.anomaly_score, 0.99) AS p99_score,
            COUNT(*) AS count
        RETURN min_score, max_score, avg_score, std_score, median_score, p95_score, p99_score, count
        """
        
        score_result = self.run_query(score_distribution_query)
        
        if score_result and score_result["count"] > 0:
            print("\n📊 Phân phối của anomaly score:")
            print(f"  • Số giao dịch có anomaly score: {score_result['count']}")
            print(f"  • Giá trị nhỏ nhất: {score_result['min_score']:.6f}")
            print(f"  • Giá trị lớn nhất: {score_result['max_score']:.6f}")
            print(f"  • Giá trị trung bình: {score_result['avg_score']:.6f}")
            print(f"  • Độ lệch chuẩn: {score_result['std_score']:.6f}")
            print(f"  • Giá trị trung vị: {score_result['median_score']:.6f}")
            print(f"  • Phân vị 95%: {score_result['p95_score']:.6f}")
            print(f"  • Phân vị 99%: {score_result['p99_score']:.6f}")
            
            # Cảnh báo nếu phân phối không đều
            if score_result['std_score'] < 0.001:
                print("  ⚠️ Cảnh báo: Phân phối anomaly score quá tập trung (độ lệch chuẩn thấp).")
        
        print("✅ Đã hoàn thành kiểm tra dữ liệu.")
        
    def prepare_ground_truth(self):
        """Map isFraud từ CSV sang ground_truth_fraud để hỗ trợ đánh giá."""
        print("🔄 Đang chuẩn bị dữ liệu ground truth...")
        
        # Kiểm tra xem isFraud có tồn tại trong SENT relationships không
        check_query = """
        MATCH ()-[r:SENT]->()
        RETURN 
            COUNT(r) AS total,
            SUM(CASE WHEN r.isFraud IS NOT NULL THEN 1 ELSE 0 END) AS has_is_fraud
        """
        result = self.run_query(check_query)
        
        if result and result["has_is_fraud"] > 0:
            print(f"  • Tìm thấy {result['has_is_fraud']} giao dịch có trường isFraud")
            
            # Map từ isFraud sang ground_truth_fraud
            map_query = """
            MATCH ()-[r:SENT]->()
            WHERE r.isFraud IS NOT NULL AND r.ground_truth_fraud IS NULL
            SET r.ground_truth_fraud = CASE 
                WHEN r.isFraud = 1 OR r.isFraud = true OR r.isFraud = '1' THEN true 
                ELSE false 
            END
            RETURN COUNT(*) AS mapped
            """
            
            map_result = self.run_query(map_query)
            if map_result:
                print(f"  ✅ Đã map {map_result['mapped']} giao dịch từ isFraud sang ground_truth_fraud")
        else:
            print("  ⚠️ Không tìm thấy trường isFraud trong dữ liệu SENT relationships")
        
        # Kiểm tra kết quả
        final_check = """
        MATCH ()-[r:SENT]->()
        RETURN 
            COUNT(r) AS total,
            SUM(CASE WHEN r.ground_truth_fraud IS NOT NULL THEN 1 ELSE 0 END) AS has_ground_truth,
            SUM(CASE WHEN r.ground_truth_fraud = true THEN 1 ELSE 0 END) AS fraud_cases
        """
        
        final_result = self.run_query(final_check)
        if final_result:
            total = final_result["total"]
            has_ground_truth = final_result["has_ground_truth"]
            fraud_cases = final_result["fraud_cases"]
            
            print(f"\n📊 Thông tin ground truth sau khi chuẩn bị:")
            print(f"  • Tổng số giao dịch: {total}")
            print(f"  • Số giao dịch có ground truth: {has_ground_truth} ({has_ground_truth/total*100:.2f}%)")
            print(f"  • Số giao dịch gian lận: {fraud_cases} ({fraud_cases/has_ground_truth*100:.2f}% trong số có nhãn)")
        
    def create_graph_projections(self):
        """Tạo các graph projection dùng cho các thuật toán GDS."""
        print("🔄 Đang tạo các graph projection...")
        
        # Tạo timestamp để đảm bảo tên graph là duy nhất
        timestamp = int(time.time())
        self.main_graph_name = f'main-graph-{timestamp}'
        self.similarity_graph_name = f'account-similarity-{timestamp}'
        self.temporal_graph_name = f'temporal-graph-{timestamp}'
        
        # 1. Graph projection cho các Account và mối quan hệ SENT
        main_projection = f"""
        CALL gds.graph.project(
            '{self.main_graph_name}',
            'Account',
            {{
                SENT: {{
                    type: 'SENT',
                    orientation: 'NATURAL',
                    properties: {{
                        weight: {{
                            property: 'amount',
                            defaultValue: 0.0,
                            aggregation: 'NONE'
                        }}
                    }}
                }}
            }}
        )
        """
        self.run_query(main_projection)
        
        # 2. Graph projection cho account similarity
        filtered_similarity_projection = f"""
        CALL gds.graph.project.cypher(
            '{self.similarity_graph_name}',
            'MATCH (a:Account) 
            WHERE EXISTS((a)-[:SENT]->())  // Đảm bảo node có gửi transaction
            RETURN id(a) AS id, labels(a) AS labels',
            'MATCH (a:Account)-[:SENT]->(tx:Transaction)-[:RECEIVED]->(b:Account)
            RETURN id(a) AS source, id(b) AS target, "TRANSFER" AS type',
            {{
                validateRelationships: false
            }}
        ) YIELD graphName AS filteredGraphName
        RETURN filteredGraphName
        """
        self.run_query(filtered_similarity_projection)
        
        # 3. Graph projection cho temporal analysis
        temporal_projection = f"""
        CALL gds.graph.project(
            '{self.temporal_graph_name}',
            'Account',
            {{
                SENT: {{
                    type: 'SENT',
                    orientation: 'NATURAL',
                    properties: {{
                        weight: {{
                            property: 'step',
                            defaultValue: 0,
                            aggregation: 'NONE'
                        }}
                    }}
                }}
            }}
        )
        """
        self.run_query(temporal_projection)
        
        print("✅ Đã tạo xong các graph projection.")

    def extract_temporal_features(self):
        """Trích xuất các đặc trưng thời gian (temporal features) để phát hiện mẫu bất thường."""
        print("🔄 Đang trích xuất đặc trưng thời gian...")
        
        # 1. Tính tốc độ giao dịch (giao dịch/giờ) trong cửa sổ thời gian
        transaction_velocity_query = """
        MATCH (from:Account)-[tx:SENT]->()
        WITH from, tx.step as step
        ORDER BY from, step
        WITH from, collect(step) AS steps
        WITH from, steps, 
            size(steps) AS transaction_count,
            CASE WHEN size(steps) <= 1 THEN 0 ELSE toFloat(last(steps) - head(steps)) END AS time_span
        WITH from, transaction_count, 
            CASE WHEN time_span = 0 THEN 0 ELSE transaction_count / (time_span + 1) END AS velocity
        SET from.txVelocity = velocity
        """
        self.run_query(transaction_velocity_query)
        
        # 2. Phát hiện sự thay đổi đột ngột trong số tiền giao dịch (sửa lại để hoạt động đúng)
        simple_volatility_query = """
        MATCH (from:Account)-[tx:SENT]->()
        WITH from, tx
        ORDER BY from, tx.step
        WITH from, collect(tx.amount) as amount_list
        WITH from, amount_list,
            CASE WHEN size(amount_list) <= 1 THEN 0 
                ELSE (
                    // Tính range từng phần tử một
                    REDUCE(max_val = 0, x IN amount_list | 
                    CASE WHEN x > max_val THEN x ELSE max_val END
                    ) - 
                    REDUCE(min_val = toFloat(9999999999), x IN amount_list | 
                    CASE WHEN x < min_val AND x IS NOT NULL THEN x ELSE min_val END
                    )
                ) 
            END AS amount_range,
            CASE WHEN size(amount_list) = 0 THEN 0 
                ELSE REDUCE(sum = 0, x IN amount_list | sum + x) / size(amount_list) 
            END AS avg_amount
        SET from.amountVolatility = CASE WHEN avg_amount = 0 THEN 0 ELSE amount_range / avg_amount END,
            from.maxAmountRatio = CASE WHEN avg_amount = 0 THEN 0 
                                    ELSE REDUCE(max_val = 0, x IN amount_list | 
                                            CASE WHEN x > max_val THEN x ELSE max_val END
                                        ) / avg_amount
                                END
        """
        self.run_query(simple_volatility_query)
        
        # 3. Phát hiện burst (nhiều giao dịch trong thời gian ngắn) - (Đã hoạt động)
        burst_detection_query = """
        MATCH (from:Account)-[tx:SENT]->()
        WITH from, tx.step as step
        ORDER BY from, step
        WITH from, collect(step) AS steps
        UNWIND range(0, size(steps)-2) AS i
        WITH from, steps[i+1] - steps[i] AS time_diff
        WITH from, collect(time_diff) AS time_diffs
        WITH from, time_diffs,
            CASE WHEN size(time_diffs) = 0 THEN 0 
                ELSE size([t IN time_diffs WHERE t <= 3]) / toFloat(size(time_diffs)) 
            END AS burst_ratio
        SET from.tempBurst = burst_ratio
        """
        self.run_query(burst_detection_query)
        
        # 4. Thời gian trung bình và độ lệch chuẩn - (Đã hoạt động)
        time_patterns_query = """
        MATCH (from:Account)-[tx:SENT]->()
        WITH from, tx.step as step
        ORDER BY from, step
        WITH from, collect(step) AS steps
        UNWIND range(0, size(steps)-2) AS i
        WITH from, steps[i+1] - steps[i] AS time_diff
        WITH from, avg(time_diff) AS avg_time_between_tx,
            stDev(time_diff) AS std_time_between_tx
        SET from.avgTimeBetweenTx = avg_time_between_tx,
            from.stdTimeBetweenTx = CASE WHEN avg_time_between_tx = 0 THEN 0 
                                        ELSE std_time_between_tx / avg_time_between_tx 
                                    END
        """
        self.run_query(time_patterns_query)
        
        # Cập nhật trọng số
        self.weights['txVelocity'] = 0.05
        self.weights['amountVolatility'] = 0.07
        self.weights['tempBurst'] = 0.08
        self.weights['maxAmountRatio'] = 0.05
        self.weights['stdTimeBetweenTx'] = 0.05
        
        print("✅ Đã trích xuất các đặc trưng thời gian.")
        
    def run_algorithms(self):
        """Chạy tất cả các thuật toán GDS để tính toán các đặc trưng."""
        print("🔄 Đang chạy các thuật toán phân tích đồ thị...")
        
        # 1. Degree Centrality
        print("  - Đang chạy Degree Centrality...")
        degree_query = f"""
        CALL gds.degree.write(
            '{self.main_graph_name}',
            {{
                writeProperty: 'degScore',
                relationshipWeightProperty: 'weight'  // Thay 'amount' bằng 'weight'
            }}
        )
        """
        self.run_query(degree_query)
        
        # 2. PageRank
        print("  - Đang chạy PageRank...")
        pagerank_query = f"""
        CALL gds.pageRank.write(
            '{self.main_graph_name}',
            {{
                writeProperty: 'prScore',
                relationshipWeightProperty: 'weight',  // Thay 'amount' bằng 'weight'
                maxIterations: 20,
                dampingFactor: 0.85
            }}
        )
        """
        self.run_query(pagerank_query)
        
        # 3. Louvain Community Detection
        print("  - Đang chạy Louvain Community Detection...")
        community_query = f"""
        CALL gds.louvain.write(
            '{self.main_graph_name}',
            {{
                writeProperty: 'communityId',
                relationshipWeightProperty: 'weight',  // Thay 'amount' bằng 'weight'
                includeIntermediateCommunities: false,  // Added comma here
                tolerance: 0.0001,    // Tăng tolerance để hội tụ nhanh hơn
                maxIterations: 10,    // Giới hạn số lần lặp
                concurrency: 4        // Sử dụng đa luồng
            }}
        )
        """
        self.run_query(community_query)

        print("  - Đã chạy Louvain Community Detection. Đang tính toán kích thước cộng đồng...")

        # Tính toán và normalize community size
        community_size_query = """
        MATCH (n)
        WHERE n.communityId IS NOT NULL
        WITH n.communityId AS communityId, COUNT(*) AS size
        WHERE size >= 3  // Filter small communities with WHERE instead of HAVING
        MATCH (m)
        WHERE m.communityId = communityId
        SET m.communitySize = size

        WITH MIN(size) AS minSize, MAX(size) AS maxSize
        MATCH (n)
        WHERE n.communitySize IS NOT NULL
        SET n.normCommunitySize = 
            CASE 
                WHEN (maxSize - minSize) = 0 THEN 0
                ELSE (n.communitySize - minSize) / (maxSize - minSize)
            END
        """
        self.run_query(community_size_query)
        
        # 4. Node Similarity (Jaccard) - chỉ chạy cho các Account
        print("  - Đang chạy Node Similarity (Jaccard)...")
        similarity_query = f"""
        CALL gds.nodeSimilarity.write(
            '{self.similarity_graph_name}',
            {{
                writeProperty: 'simScore',
                writeRelationshipType: 'SIMILAR',
                similarityCutoff: 0.2,
                topK: 5,
                concurrency: 4
            }}
        )
        """
        try:
            self.run_query(similarity_query)
        except Exception as e:
            print(f"Lỗi khi chạy Node Similarity: {e}")
            # Sử dụng cách thay thế: Stream một lượng nhỏ kết quả và ghi vào đồ thị
            fallback_similarity_query = f"""
            CALL gds.nodeSimilarity.stream(
                '{self.similarity_graph_name}',
                {{
                    similarityCutoff: 0.2,
                    topK: 3,
                    concurrency: 4
                }}
            )
            YIELD node1, node2, similarity
            WITH gds.util.asNode(node1) AS source, gds.util.asNode(node2) AS target, similarity
            LIMIT 50000  // Giới hạn số lượng kết quả
            SET source.simScore = CASE 
                WHEN source.simScore IS NULL OR similarity > source.simScore 
                THEN similarity ELSE source.simScore END
            RETURN COUNT(*) as relationshipsProcessed
            """
            self.run_query(fallback_similarity_query)

        # Gán simScore mặc định = 0 cho các node chưa có score
        set_default_sim_query = """
        MATCH (n)
        WHERE n.simScore IS NULL
        SET n.simScore = 0.0
        """
        self.run_query(set_default_sim_query)
        
        # 5. Betweenness Centrality
        print("  - Đang chạy Betweenness Centrality...")
        betweenness_query = f"""
        CALL gds.betweenness.write(
            '{self.main_graph_name}',
            {{
                writeProperty: 'btwScore'
            }}
        )
        """
        self.run_query(betweenness_query)
        
        # 6. HITS (Hub and Authority Scores)
        print("  - Đang chạy HITS algorithm...")
        hits_query = f"""
        CALL gds.alpha.hits.write(
            '{self.main_graph_name}',
            {{
                writeProperty: '',
                hitsIterations: 20,  // Changed from 'iterations' to 'hitsIterations'
                authProperty: 'authScore',
                hubProperty: 'hubScore' 
            }}
        )
        """
        self.run_query(hits_query)
        
       # 7. K-Core Decomposition
        print("  - Đang chạy K-Core Decomposition...")
        # Create a specific undirected graph for K-Core
        kcore_graph_name = f'{self.main_graph_name}-undirected'
        kcore_projection_query = f"""
        CALL gds.graph.project(
            '{kcore_graph_name}',
            'Account',
            {{
                SENT: {{
                    type: 'SENT',
                    orientation: 'UNDIRECTED',
                    properties: {{
                        weight: {{
                            property: 'amount',  // Changed from 'weight' to 'amount'
                            defaultValue: 0.0,
                            aggregation: 'NONE'
                        }}
                    }}
                }}
            }}
        )
        """
        self.run_query(kcore_projection_query)

        # Then run K-Core on the undirected graph
        kcore_query = f"""
        CALL gds.kcore.write(
            '{kcore_graph_name}',
            {{
                writeProperty: 'coreScore'
            }}
        )
        """
        self.run_query(kcore_query)

        # Clean up the temporary graph
        kcore_cleanup_query = f"""
        CALL gds.graph.drop('{kcore_graph_name}', false)
        """
        self.run_query(kcore_cleanup_query)
        
        # 8. Clustering Coefficient (Triangle Count)
        print("  - Đang chạy Triangle Count...")
        # Create a specific undirected graph for Triangle Count (or reuse the K-Core graph)
        triangle_graph_name = f'{self.main_graph_name}-undirected-tri'
        triangle_projection_query = f"""
        CALL gds.graph.project(
            '{triangle_graph_name}',
            'Account',
            {{
                SENT: {{
                    type: 'SENT',
                    orientation: 'UNDIRECTED'
                }}
            }}
        )
        """
        self.run_query(triangle_projection_query)

        # Run Triangle Count on the undirected graph
        triangle_query = f"""
        CALL gds.triangleCount.write(
            '{triangle_graph_name}',
            {{
                writeProperty: 'triCount'
            }}
        )
        """
        self.run_query(triangle_query)

        # Clean up the temporary graph
        triangle_cleanup_query = f"""
        CALL gds.graph.drop('{triangle_graph_name}', false)
        """
        self.run_query(triangle_cleanup_query)
        # Gán triCount mặc định = 0 cho các node chưa có score
        set_default_tri_query = """
        MATCH (n)
        WHERE n.triCount IS NULL
        SET n.triCount = 0
        """
        self.run_query(set_default_tri_query)
        
        # 9. Motif/Cycle Detection (sử dụng APOC)
        print("  - Đang chạy Motif/Cycle Detection...")
        cycle_query = """
        MATCH (a:Account)
        OPTIONAL MATCH path = (a)-[:SENT]->(tx1:Transaction)-[:RECEIVED]->(b:Account)-[:SENT]->(tx2:Transaction)-[:RECEIVED]->(c:Account)-[:SENT]->(tx3:Transaction)-[:RECEIVED]->(a)
        WITH a, COUNT(path) AS cycleCount
        SET a.cycleCount = cycleCount
        """
        self.run_query(cycle_query)
        
        # 10. Temporal Burst Analysis
        print("  - Đang chạy Temporal Burst Analysis...")
        temporal_burst_query = """
        // Tính số lượng giao dịch trong 1 giờ và 24 giờ cho mỗi account
        MATCH (a:Account)-[tx:SENT]->()
        WITH a, tx.step AS step
        WITH a, step, COUNT(*) AS hourlyCount
        WITH a, COLLECT({step: step, count: hourlyCount}) AS hourlyCounts

        // Tính tempBurst1h (giá trị cao nhất trong 1 giờ)
        WITH a, hourlyCounts, 
            REDUCE(max = 0, h IN hourlyCounts | CASE WHEN h.count > max THEN h.count ELSE max END) AS maxHourly
        SET a.tempBurst1h = maxHourly

        // Tính tempBurst24h (số giờ liên tiếp với ít nhất 1 giao dịch - cách đơn giản hóa)
        WITH a, hourlyCounts
        UNWIND hourlyCounts AS h
        WITH a, h.step AS step
        ORDER BY step
        // Sử dụng tỷ lệ số giờ có giao dịch so với tổng khoảng thời gian làm proxy cho burst
        WITH a, MIN(step) AS minStep, MAX(step) AS maxStep, COUNT(DISTINCT step) AS uniqueSteps
        WITH a, 
            CASE 
                WHEN maxStep = minStep THEN 1  // Nếu chỉ có 1 giờ, burst là 1
                ELSE toFloat(uniqueSteps) / (maxStep - minStep + 1)  // Tỷ lệ giờ hoạt động trên tổng thời gian
            END * 24 AS burstDensity  // Scale to 24 hours
        SET a.tempBurst24h = burstDensity

        // Tính tempBurst tổng hợp (kết hợp cả 1h và 24h)
        WITH a
        SET a.tempBurst = (a.tempBurst1h * 0.7) + (a.tempBurst24h * 0.3)
        """
        self.run_query(temporal_burst_query)
        
        # Gán các giá trị mặc định cho node nếu chưa có
        set_default_values_query = """
        MATCH (n)
        SET n.degScore = COALESCE(n.degScore, 0),
            n.prScore = COALESCE(n.prScore, 0),
            n.communityId = COALESCE(n.communityId, -1),
            n.normCommunitySize = COALESCE(n.normCommunitySize, 0),
            n.simScore = COALESCE(n.simScore, 0),
            n.btwScore = COALESCE(n.btwScore, 0),
            n.authScore = COALESCE(n.authScore, 0),
            n.hubScore = COALESCE(n.hubScore, 0),
            n.coreScore = COALESCE(n.coreScore, 0),
            n.triCount = COALESCE(n.triCount, 0),
            n.cycleCount = COALESCE(n.cycleCount, 0),
            n.tempBurst = COALESCE(n.tempBurst, 0)
        """
        self.run_query(set_default_values_query)
        
        print("✅ Đã chạy xong tất cả các thuật toán.")
    
    def normalize_features(self):
        """Min-max normalize tất cả các đặc trưng về khoảng [0, 1]."""
        print("🔄 Đang normalize các đặc trưng...")
        
        features_to_normalize = [
            'degScore', 'prScore', 'simScore', 'btwScore', 'hubScore', 
            'authScore', 'coreScore', 'triCount', 'cycleCount', 'tempBurst',
            'txVelocity', 'amountVolatility', 'maxAmountRatio', 'stdTimeBetweenTx'
        ]
        for feature in features_to_normalize:
            normalize_query = f"""
            MATCH (n) 
            WHERE n.{feature} IS NOT NULL
            WITH MIN(n.{feature}) AS min_val, MAX(n.{feature}) AS max_val
            WHERE max_val <> min_val
            MATCH (m)
            WHERE m.{feature} IS NOT NULL
            SET m.{feature}_norm = (m.{feature} - min_val) / (max_val - min_val)
            """
            self.run_query(normalize_query)
            
            # Xóa các đặc trưng gốc và đổi tên các đặc trưng đã normalize
            rename_query = f"""
            MATCH (n)
            WHERE n.{feature}_norm IS NOT NULL
            SET n.{feature} = n.{feature}_norm
            REMOVE n.{feature}_norm
            """
            self.run_query(rename_query)
            
            # Thiết lập giá trị mặc định cho các node không có đặc trưng
            default_query = f"""
            MATCH (n)
            WHERE n.{feature} IS NULL
            SET n.{feature} = 0
            """
            self.run_query(default_query)
            
        print("✅ Đã normalize xong tất cả các đặc trưng.")
    
    def compute_anomaly_scores(self):
        """Tính điểm bất thường (anomaly score) dựa trên weighted sum."""
        print("🔄 Đang tính toán anomaly score...")
        
        # Tạo weighted sum của tất cả các đặc trưng đã normalize
        weights_str = ", ".join([f"{k} * {v}" for k, v in self.weights.items() if k != 'normCommunitySize'])
        
        anomaly_score_query = """
        MATCH (a:Account)
        WITH a, 
            a.degScore AS degScore, 
            a.prScore AS prScore,
            a.simScore AS simScore,
            a.btwScore AS btwScore,
            a.hubScore AS hubScore,
            a.authScore AS authScore,
            a.coreScore AS coreScore,
            a.triCount AS triCount,
            a.cycleCount AS cycleCount,
            a.tempBurst AS tempBurst,
            a.txVelocity AS txVelocity,
            a.amountVolatility AS amountVolatility,
            a.maxAmountRatio AS maxAmountRatio,
            a.stdTimeBetweenTx AS stdTimeBetweenTx,
            a.normCommunitySize AS normCommunitySize
        
        // Tính anomaly score = weighted sum của các đặc trưng
        WITH a, 
            (degScore * 0.15) + 
            (prScore * 0.15) + 
            (simScore * 0.1) + 
            (btwScore * 0.1) + 
            (hubScore * 0.05) + 
            (authScore * 0.05) + 
            (coreScore * 0.05) + 
            (triCount * 0.05) + 
            (cycleCount * 0.05) + 
            (tempBurst * 0.08) + 
            (txVelocity * 0.05) +
            (amountVolatility * 0.07) +
            (maxAmountRatio * 0.05) +
            (stdTimeBetweenTx * 0.05) +
            (0.10 * (1 - coalesce(normCommunitySize, 0))) AS score
        
        SET a.anomaly_score = score
        """
        self.run_query(anomaly_score_query)
        
        # Chuyển anomaly score từ Account sang Transaction
        transfer_score_query = """
        MATCH (a:Account)-[r:SENT]->()
        SET r.anomaly_score = a.anomaly_score
        """
        self.run_query(transfer_score_query)
        
        print("✅ Đã tính toán xong anomaly score.")
    
    def flag_anomalies(self):
        """Đánh dấu giao dịch bất thường dựa trên ngưỡng phân vị (percentile)."""
        print(f"🔄 Đang đánh dấu các giao dịch bất thường (ngưỡng phân vị: {self.percentile_cutoff*100}%)...")
        
        # Tính giá trị ngưỡng percentile
        percentile_query = f"""
        MATCH ()-[tx:SENT]->()
        WITH percentileCont(tx.anomaly_score, {self.percentile_cutoff}) AS threshold
        MATCH ()-[tx2:SENT]->()
        WHERE tx2.anomaly_score >= threshold
        SET tx2.flagged = true
        RETURN threshold, COUNT(tx2) AS flagged_count
        """
        
        # Remove the .single() call since run_query now returns the dictionary directly
        result = self.run_query(percentile_query)
        
        if result:
            threshold = result["threshold"]
            flagged_count = result["flagged_count"]
            
            # Đánh dấu các giao dịch không vượt ngưỡng là không bất thường
            default_flagged_query = """
            MATCH ()-[tx:SENT]->()
            WHERE tx.flagged IS NULL
            SET tx.flagged = false
            """
            self.run_query(default_flagged_query)
            
            print(f"✅ Đã đánh dấu {flagged_count} giao dịch bất thường (threshold: {threshold:.6f}).")
        else:
            print("⚠️ Không thể tính ngưỡng percentile cho anomaly score.")
    
    def evaluate_performance(self):
        """Đánh giá hiệu suất phát hiện bất thường dựa trên ground truth."""
        print("🔄 Đang đánh giá hiệu suất phát hiện bất thường...")
        
        eval_query = """
        MATCH ()-[tx:SENT]->()
        WITH
            SUM(CASE WHEN tx.flagged = true AND tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS true_positives,
            SUM(CASE WHEN tx.flagged = true AND tx.ground_truth_fraud = false THEN 1 ELSE 0 END) AS false_positives,
            SUM(CASE WHEN tx.flagged = false AND tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS false_negatives,
            SUM(CASE WHEN tx.flagged = false AND tx.ground_truth_fraud = false THEN 1 ELSE 0 END) AS true_negatives,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN tx.ground_truth_fraud = true THEN 1 ELSE 0 END) AS total_fraud
        
        // Calculate precision and recall first
        WITH
            true_positives, false_positives, false_negatives, true_negatives, 
            total_transactions, total_fraud,
            CASE WHEN (true_positives + false_positives) > 0 
                THEN toFloat(true_positives) / (true_positives + false_positives) 
                ELSE 0 
            END AS precision,
            CASE WHEN (true_positives + false_negatives) > 0 
                THEN toFloat(true_positives) / (true_positives + false_negatives) 
                ELSE 0 
            END AS recall
        
        // Then use precision and recall to calculate F1 score
        RETURN 
            true_positives,
            false_positives,
            false_negatives,
            true_negatives,
            total_transactions,
            total_fraud,
            precision,
            recall,
            CASE 
                WHEN (precision + recall) > 0 
                THEN 2 * precision * recall / (precision + recall) 
                ELSE 0 
            END AS f1_score
        """
        
        result = self.run_query(eval_query)
        
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
    
    def analyze_feature_importance(self):
        """Phân tích tầm quan trọng của các đặc trưng sử dụng Python thay vì APOC."""
        print("🔄 Đang phân tích tầm quan trọng của các đặc trưng...")
        
        import numpy as np
        
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
        features = list(self.weights.keys())
        correlations = {}
        
        for feature in features:
            # LẤY ĐẶC TRƯNG TỪ NODE ACCOUNT, KẾT HỢP VỚI GROUND_TRUTH_FRAUD TỪ RELATIONSHIP
            data_query = f"""
            MATCH (a:Account)-[tx:SENT]->()
            WHERE tx.ground_truth_fraud IS NOT NULL AND a.{feature} IS NOT NULL
            RETURN tx.ground_truth_fraud AS fraud, a.{feature} AS feature_value
            """
            
            try:
                # Get all records
                result = []
                with self.driver.session() as session:
                    result = session.run(data_query).data()
                
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
    
    def delete_graph_projections(self):
        """Xóa các graph projections đã tạo."""
        print("🔄 Đang xóa các graph projections...")
        
        # Xóa các graph projections cụ thể
        try:
            self.run_query(f"CALL gds.graph.drop('{self.main_graph_name}', false)")
            self.run_query(f"CALL gds.graph.drop('{self.similarity_graph_name}', false)")
            self.run_query(f"CALL gds.graph.drop('{self.temporal_graph_name}', false)")
            self.run_query(f"CALL gds.graph.drop('{self.main_graph_name}-undirected', false)")
            self.run_query(f"CALL gds.graph.drop('{self.main_graph_name}-undirected-tri', false)")
            print("✅ Đã xóa tất cả các graph projections.")
        except Exception as e:
            print(f"⚠️ Lưu ý khi xóa graph: {str(e)}")

    def cleanup_properties_and_relationships(self):
        """Xóa tất cả các thuộc tính được thêm vào trong quá trình phân tích để tránh đầy database."""
        print("🔄 Đang dọn dẹp các thuộc tính phân tích...")
        
        # Danh sách các thuộc tính được thêm vào trong quá trình phân tích
        added_properties = [
            'degScore', 'prScore', 'communityId', 'communitySize', 'normCommunitySize',
            'simScore', 'btwScore', 'hubScore', 'authScore', 'coreScore', 'triCount',
            'cycleCount', 'tempBurst', 'tempBurst1h', 'tempBurst24h', 'anomaly_score', 'flagged'
        ]
        
        # Xóa thuộc tính trên tất cả các node
        properties_to_remove = ", ".join([f"n.{prop}" for prop in added_properties])
        cleanup_query = f"""
        MATCH (n)
        REMOVE {properties_to_remove}
        """

        relationship_cleanup_query = """
        MATCH ()-[r:SENT]->()
        REMOVE r.anomaly_score, r.flagged
        """
        
        try:
            self.run_query(cleanup_query)
            self.run_query(relationship_cleanup_query)
            print(f"✅ Đã xóa {len(added_properties)} thuộc tính phân tích khỏi database.")
        except Exception as e:
            print(f"❌ Lỗi khi dọn dẹp thuộc tính: {str(e)}")
            
        # Xóa các mối quan hệ SIMILAR (từ Node Similarity)
        try:
            self.run_query("MATCH ()-[r:SIMILAR]-() DELETE r")
            print("✅ Đã xóa các mối quan hệ SIMILAR.")
        except Exception as e:
            print(f"❌ Lỗi khi xóa quan hệ SIMILAR: {str(e)}")
    
    def run_pipeline(self, percentile_cutoff=None):
        """
        Chạy toàn bộ pipeline phát hiện bất thường.
        
        Args:
            percentile_cutoff: Ngưỡng phân vị để đánh dấu giao dịch bất thường (mặc định: 0.95)
        
        Returns:
            dict: Metrics đánh giá hiệu suất
        """
        if percentile_cutoff is not None:
            self.percentile_cutoff = percentile_cutoff
            
        start_time = time.time()
        
        print("=" * 50)
        print("🚀 Bắt đầu chạy pipeline phát hiện bất thường không giám sát")
        print("=" * 50)
        
        # 1. Chuẩn bị dữ liệu ground truth
        self.prepare_ground_truth()

        # 2. Kiểm tra và sửa lỗi dữ liệu
        self.examine_data()
        
        # 3. Tạo graph projections
        self.create_graph_projections()

        # 4. Trích xuất đặc trưng thời gian (THÊM BƯỚC MỚI)
        self.extract_temporal_features()
        
        # 5. Chạy các thuật toán Graph Data Science
        self.run_algorithms()
        
        # 6. Normalize các đặc trưng
        self.normalize_features()
        
        # 7. Tính toán anomaly score
        self.compute_anomaly_scores()
        
        # 8. Đánh dấu các giao dịch bất thường
        self.flag_anomalies()
        
        # 9. Đánh giá hiệu suất
        metrics = self.evaluate_performance()
        
        # 10. Phân tích tầm quan trọng của các đặc trưng
        feature_importances = self.analyze_feature_importance()

        # 11. Xóa các graph projections
        self.delete_graph_projections()

        # 12. Dọn dẹp các thuộc tính và mối quan hệ không cần thiết
        self.cleanup_properties_and_relationships()

        end_time = time.time()
        execution_time = end_time - start_time
        
        print("\n⏱️ Thời gian thực thi: {:.2f} giây".format(execution_time))
        print("=" * 50)
        print("✅ Hoàn thành pipeline phát hiện bất thường không giám sát")
        print("=" * 50)
        
        return metrics

# Chạy pipeline
if __name__ == "__main__":    
    # Khởi tạo và chạy pipeline
    fraud_detector = UnsupervisedFraudDetection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # Chạy với ngưỡng phân vị tối ưu đã xác định
        percentile = 0.97
        print(f"\n📊 Chạy pipeline với ngưỡng phân vị {percentile*100}%")
        metrics = fraud_detector.run_pipeline(percentile)

        print("\n" + "=" * 50)
        print(f"🏆 Kết quả với ngưỡng phân vị {percentile*100}%")
        print(f"   F1 Score: {metrics['metrics']['f1_score']:.4f}")
        print(f"   Precision: {metrics['metrics']['precision']:.4f}")
        print(f"   Recall: {metrics['metrics']['recall']:.4f}")
        print("=" * 50)

        # Thử với nhiều ngưỡng percentile khác nhau để tìm ngưỡng tối ưu
        # percentiles = [0.93, 0.95, 0.97]
        # results = []
        
        # for p in percentiles:
        #     print(f"\n📊 Thử nghiệm với ngưỡng phân vị {p*100}%")
        #     metrics = fraud_detector.run_pipeline(p)
        #     results.append({
        #         "percentile": p,
        #         "metrics": metrics
        #     })

        # # Tìm ngưỡng tốt nhất dựa trên F1 score
        # best_result = max(results, key=lambda x: x["metrics"]["metrics"]["f1_score"])
        
        # print("\n" + "=" * 50)
        # print(f"🏆 Ngưỡng phân vị tối ưu: {best_result['percentile']*100}%")
        # print(f"   F1 Score: {best_result['metrics']['metrics']['f1_score']:.4f}")
        # print(f"   Precision: {best_result['metrics']['metrics']['precision']:.4f}")
        # print(f"   Recall: {best_result['metrics']['metrics']['recall']:.4f}")
        # print("=" * 50)
        
        # # Lưu kết quả so sánh
        # comparison = {
        #     "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        #     "percentile_comparison": [
        #         {
        #             "percentile": r["percentile"],
        #             "f1_score": r["metrics"]["metrics"]["f1_score"],
        #             "precision": r["metrics"]["metrics"]["precision"],
        #             "recall": r["metrics"]["metrics"]["recall"]
        #         } for r in results
        #     ],
        #     "best_percentile": best_result["percentile"]
        # }
        
        # with open('percentile_comparison.json', 'w', encoding='utf-8') as f:
        #     json.dump(comparison, f, indent=2)
        
        # print("✅ Đã lưu kết quả so sánh các ngưỡng phân vị vào file percentile_comparison.json")
    
    finally:
        # Đóng kết nối Neo4j
        fraud_detector.close()
