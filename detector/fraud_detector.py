from detector.database_manager import DatabaseManager
from detector.data_importer import DataImporter
from analysis.account_analyzer import AccountAnalyzer
from analysis.transaction_analyzer import TransactionAnalyzer
from analysis.pattern_detector import PatternDetector

class FraudDetector:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.data_importer = DataImporter(self.db_manager)
        self.account_analyzer = AccountAnalyzer(self.db_manager.driver)
        self.transaction_analyzer = TransactionAnalyzer(self.db_manager.driver)
        self.pattern_detector = PatternDetector(self.db_manager.driver)
        # Thêm property driver để đảm bảo khả năng tương thích với các API
        self.driver = self.db_manager.driver
        
    def check_data(self):
        """Kiểm tra xem đã có dữ liệu trong database chưa"""
        return self.db_manager.check_data()
        
    def clear_database(self):
        """Xóa toàn bộ dữ liệu trong database"""
        return self.db_manager.clear_database()
            
    def import_data(self, csv_path):
        """Import dữ liệu sử dụng API Neo4j thay vì LOAD CSV"""
        return self.data_importer.import_data(csv_path)
        
    def finalize_and_evaluate(self):
        """Chuẩn hóa điểm và đánh giá kết quả"""
        # Sử dụng các phương thức tương đương từ các module đã có
        with self.db_manager.driver.session() as session:
            print("🔍 Đang hoàn tất phân tích...")
            
            # Sử dụng PatternDetector để tính điểm cuối cùng
            self.pattern_detector.calculate_fraud_scores()
            
            # Đánh dấu tài khoản gian lận thực sự
            print("  Đang đánh dấu tài khoản gian lận thực sự...")
            session.run("""
                MATCH (a:Account)
                WHERE exists((a)-[:SENT {is_fraud: 1}]->()) OR exists((a)<-[:SENT {is_fraud: 1}]-())
                SET a.real_fraud = true
            """)
            
            return True

    def process_high_risk_communities(self):
        """Xử lý các cộng đồng có nguy cơ cao"""
        # Sử dụng PatternDetector có các phương thức tương tự
        # Phương thức này được bao gồm trong analyze_fraud
        return True

    def analyze_fraud(self):
        """Chạy phân tích gian lận với các thuật toán đồ thị"""
        try:
            with self.db_manager.driver.session() as session:
                # Xóa dữ liệu phân tích cũ
                print("🔍 Đang xóa phân tích cũ...")
                session.run("""
                    MATCH (a:Account) 
                    REMOVE a.fraud_score, a.community, a.pagerank_score, 
                        a.degree_score, a.similarity_score, a.path_score, a.known_fraud,
                        a.base_score, a.tx_anomaly, a.high_tx_volume, a.only_sender
                """)
    
                # Xóa mối quan hệ SIMILAR_TO
                print("🔍 Đang xóa mối quan hệ từ phân tích trước...")
                session.run("""
                    MATCH ()-[r:SIMILAR_TO]->()
                    DELETE r
                """)
                
                # Tạo index
                session.run("CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.id)")
                
                # Xóa projected graph cũ
                print("🔍 Đang xóa projected graph cũ...")
                try:
                    result = session.run("""
                        CALL gds.graph.list()
                        YIELD graphName
                        WHERE graphName = 'fraud_graph'
                        RETURN count(*) > 0 AS exists
                    """).single()
    
                    if result and result.get('exists', False):
                        print("  Đã tìm thấy projected graph trong danh sách, đang xóa...")
                        session.run("CALL gds.graph.drop('fraud_graph', false)")
                except Exception as e:
                    print(f"  Lỗi khi kiểm tra projected graph: {e}")                # 1. Tạo projected graph ban đầu (chỉ dùng amount, không dùng is_fraud)
                print("🔍 Đang tạo projected graph...")
                session.run("""
                    CALL gds.graph.project(
                        'fraud_graph',
                        'Account',
                        'SENT',
                        {
                            relationshipProperties: {
                                amount: {property: 'amount', defaultValue: 0.0}
                            }
                        }
                    )
                """)
                
                # 2. Degree Centrality
                print("🔍 Đang tính Degree Centrality...")
                session.run("""
                    CALL gds.degree.write('fraud_graph', {
                        writeProperty: 'degree_score'
                    })
                """)
                
                # 3. PageRank
                print("🔍 Đang chạy PageRank...")
                session.run("""
                    CALL gds.pageRank.write('fraud_graph', {
                        writeProperty: 'pagerank_score',
                        maxIterations: 20
                    })
                """)
                
                # 4. Louvain - Phát hiện cộng đồng (không dùng seedProperty vì gây lỗi)
                print("🔍 Đang phát hiện cộng đồng với Louvain...")
                session.run("""
                    CALL gds.louvain.write('fraud_graph', {
                        writeProperty: 'community',
                        maxLevels: 10,           // Giới hạn số level của thuật toán
                        maxIterations: 20,       // Số lần lặp tối đa
                        tolerance: 0.0001,       // Giá trị ngưỡng để hội tụ cao hơn
                        includeIntermediateCommunities: false  // Không lưu các cộng đồng trung gian
                    })
                """)
                
                # Thống kê các cộng đồng phát hiện được
                community_stats = session.run("""
                    MATCH (a:Account)
                    WHERE a.community IS NOT NULL
                    WITH a.community AS community, count(*) AS size
                    RETURN 
                        count(*) AS total_communities,
                        sum(CASE WHEN size = 1 THEN 1 ELSE 0 END) AS single_node_communities,
                        sum(CASE WHEN size >= 2 AND size <= 5 THEN 1 ELSE 0 END) AS small_communities,
                        sum(CASE WHEN size > 5 AND size <= 20 THEN 1 ELSE 0 END) AS medium_communities,
                        sum(CASE WHEN size > 20 THEN 1 ELSE 0 END) AS large_communities,
                        avg(size) AS avg_community_size,
                        max(size) AS max_community_size
                """).single()
                
                if community_stats:
                    print(f"  Tổng số cộng đồng: {community_stats['total_communities']}")
                    print(f"  Cộng đồng một nút: {community_stats['single_node_communities']} " + 
                          f"({community_stats['single_node_communities']/community_stats['total_communities']*100:.1f}%)")
                    print(f"  Cộng đồng nhỏ (2-5 nút): {community_stats['small_communities']}")
                    print(f"  Cộng đồng trung bình (6-20 nút): {community_stats['medium_communities']}")
                    print(f"  Cộng đồng lớn (>20 nút): {community_stats['large_communities']}")
                    print(f"  Kích thước trung bình: {community_stats['avg_community_size']:.2f}")
                    print(f"  Kích thước lớn nhất: {community_stats['max_community_size']}")
                    
                    # Nếu có quá nhiều cộng đồng đơn lẻ, thực hiện gom nhóm lại
                    single_percent = community_stats['single_node_communities']/community_stats['total_communities']
                    if single_percent > 0.8:  # Nếu hơn 80% là cộng đồng một nút
                        print("  Đang gom nhóm các cộng đồng nhỏ...")
                        # Gán cộng đồng mới dựa trên pagerank_score (chia thành 5 nhóm)
                        session.run("""
                            MATCH (a:Account)
                            WHERE a.community IS NOT NULL AND (a)-[:SENT]->() OR ()-[:SENT]->(a)
                            WITH a, a.pagerank_score AS score
                            ORDER BY score DESC
                            WITH collect(a) AS all_accounts, count(*) AS total
                            WITH all_accounts, total, 
                                 total / 5 AS group_size
                            UNWIND range(0, 4) as group_id
                            WITH all_accounts, group_id, group_size
                            WITH all_accounts[group_id * group_size..(group_id + 1) * group_size] AS accounts, group_id
                            UNWIND accounts AS account
                            SET account.consolidated_community = group_id
                            RETURN count(*) as grouped
                        """)
                        # Xử lý các nút không có giao dịch nào
                        session.run("""
                            MATCH (a:Account)
                            WHERE NOT EXISTS(a.consolidated_community)
                            SET a.consolidated_community = 5
                        """)
                        # Sử dụng consolidated_community thay vì community
                        session.run("""
                            MATCH (a:Account)
                            SET a.community = a.consolidated_community
                            REMOVE a.consolidated_community
                        """)
                        
                # 5. Node Similarity
                print("🔍 Đang tính Node Similarity...")
                session.run("""
                    CALL gds.nodeSimilarity.write('fraud_graph', {
                        writeProperty: 'similarity_score',
                        writeRelationshipType: 'SIMILAR_TO',
                        topK: 10
                    })
                """)
                
                # 6. Phân tích giao dịch
                print("🔍 Đang tính giao dịch ra/vào...")
                self.transaction_analyzer.process_transaction_stats()
    
                # 7. Phân tích hành vi tài khoản
                print("🔍 Đang đánh dấu hành vi bất thường...")
                self.account_analyzer.process_account_behaviors()
                self.account_analyzer.process_transaction_anomalies()
                
                # 8. Tính điểm gian lận
                print("🔍 Đang tính điểm gian lận tổng hợp...")
                self.pattern_detector.calculate_fraud_scores()
                
                # 9. Hoàn tất phân tích
                print("🔍 Đang hoàn tất phân tích...")
                self.finalize_and_evaluate()
                
                # 10. Xóa projected graph để giải phóng bộ nhớ
                print("🔍 Đang xóa projected graph...")
                self.db_manager.cleanup_projected_graph()
                print("✅ Phân tích gian lận hoàn tất.")
                return True
                
        except Exception as e:
            print(f"Lỗi khi phân tích gian lận: {e}")
            return False
            
    def cleanup_projected_graph(self):
        """Xóa projected graph với cơ chế timeout và bỏ qua việc kiểm tra tồn tại"""
        return self.db_manager.cleanup_projected_graph()
    
    def close(self):
        """Đóng kết nối đến Neo4j"""
        self.db_manager.close()
