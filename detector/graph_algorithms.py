import time
from .database_manager import DatabaseManager
from .queries.graph_algorithms_queries import (
    # Degree Centrality
    get_degree_query,
    
    # PageRank
    get_pagerank_query,
    
    # Community Detection
    get_community_query,
    COMMUNITY_SIZE_QUERY,
    
    # Node Similarity
    get_similarity_query,
    get_fallback_similarity_query,
    SET_DEFAULT_SIM_QUERY,
    
    # Betweenness Centrality
    get_betweenness_query,
    
    # HITS Algorithm
    get_hits_query,
    
    # K-Core
    get_kcore_projection_query,
    get_kcore_query,
    get_kcore_cleanup_query,
    
    # Triangle Count
    get_triangle_projection_query,
    get_triangle_query,
    get_triangle_cleanup_query,
    SET_DEFAULT_TRI_QUERY,
    
    # Cycle Detection
    CYCLE_QUERY,
    
    # Temporal Burst Analysis
    TEMPORAL_BURST_QUERY,
    
    # Default Values
    SET_DEFAULT_VALUES_QUERY
)

class GraphAlgorithms:
    def __init__(self, db_manager: DatabaseManager, main_graph_name=None, similarity_graph_name=None, temporal_graph_name=None):
        """Khởi tạo với db_manager và tên của các graph projections."""
        self.db_manager = db_manager
        self.main_graph_name = main_graph_name  # Nhận tên từ bên ngoài
        self.similarity_graph_name = similarity_graph_name
        self.temporal_graph_name = temporal_graph_name
    
    def run_algorithms(self):
        """Chạy tất cả các thuật toán GDS để tính toán các đặc trưng."""
        print("🔄 Đang chạy các thuật toán phân tích đồ thị...")
        
        # 1. Degree Centrality
        print("  - Đang chạy Degree Centrality...")
        self.db_manager.run_query(get_degree_query(self.main_graph_name))
        
        # 2. PageRank
        print("  - Đang chạy PageRank...")
        self.db_manager.run_query(get_pagerank_query(self.main_graph_name))
        
        # 3. Louvain Community Detection
        print("  - Đang chạy Louvain Community Detection...")
        self.db_manager.run_query(get_community_query(self.main_graph_name))

        print("  - Đã chạy Louvain Community Detection. Đang tính toán kích thước cộng đồng...")

        # Tính toán và normalize community size
        self.db_manager.run_query(COMMUNITY_SIZE_QUERY)
        
        # 4. Node Similarity (Jaccard) - chỉ chạy cho các Account
        print("  - Đang chạy Node Similarity (Jaccard)...")
        try:
            self.db_manager.run_query(get_similarity_query(self.similarity_graph_name))
        except Exception as e:
            print(f"Lỗi khi chạy Node Similarity: {e}")
            # Sử dụng cách thay thế: Stream một lượng nhỏ kết quả và ghi vào đồ thị
            self.db_manager.run_query(get_fallback_similarity_query(self.similarity_graph_name))

        # Gán simScore mặc định = 0 cho các node chưa có score
        self.db_manager.run_query(SET_DEFAULT_SIM_QUERY)
        
        # 5. Betweenness Centrality
        print("  - Đang chạy Betweenness Centrality...")
        self.db_manager.run_query(get_betweenness_query(self.main_graph_name))
        
        # 6. HITS (Hub and Authority Scores)
        print("  - Đang chạy HITS algorithm...")
        self.db_manager.run_query(get_hits_query(self.main_graph_name))
        
        # 7. K-Core Decomposition
        print("  - Đang chạy K-Core Decomposition...")
        # Create a specific undirected graph for K-Core
        self.db_manager.run_query(get_kcore_projection_query(self.main_graph_name))

        # Then run K-Core on the undirected graph
        self.db_manager.run_query(get_kcore_query(self.main_graph_name))

        # Clean up the temporary graph
        self.db_manager.run_query(get_kcore_cleanup_query(self.main_graph_name))
        
        # 8. Clustering Coefficient (Triangle Count)
        print("  - Đang chạy Triangle Count...")
        # Create a specific undirected graph for Triangle Count (or reuse the K-Core graph)
        self.db_manager.run_query(get_triangle_projection_query(self.main_graph_name))

        # Run Triangle Count on the undirected graph
        self.db_manager.run_query(get_triangle_query(self.main_graph_name))

        # Clean up the temporary graph
        self.db_manager.run_query(get_triangle_cleanup_query(self.main_graph_name))
        
        # Gán triCount mặc định = 0 cho các node chưa có score
        self.db_manager.run_query(SET_DEFAULT_TRI_QUERY)
        
        # 9. Motif/Cycle Detection (sử dụng APOC)
        print("  - Đang chạy Motif/Cycle Detection...")
        self.db_manager.run_query(CYCLE_QUERY)
        
        # 10. Temporal Burst Analysis
        print("  - Đang chạy Temporal Burst Analysis...")
        self.db_manager.run_query(TEMPORAL_BURST_QUERY)
        
        # Gán các giá trị mặc định cho node nếu chưa có
        self.db_manager.run_query(SET_DEFAULT_VALUES_QUERY)
        
        print("✅ Đã chạy xong tất cả các thuật toán.")