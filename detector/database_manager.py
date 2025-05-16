from neo4j import GraphDatabase
import pandas as pd
import logging
import time
from .utils.config import BATCH_SIZE, MAX_NODES, MAX_RELATIONSHIPS
from .queries.database_manager_queries import (
    # Import queries
    CREATE_ACCOUNT_INDEX,
    CREATE_ACCOUNTS_QUERY,
    CREATE_TRANSACTIONS_QUERY,
    
    # Check queries
    COUNT_ALL_NODES,
    COUNT_ACCOUNTS,
    COUNT_TRANSACTIONS,
    CHECK_ANALYZED,
    
    # Cleanup queries
    DROP_ACCOUNT_INDEX,
    DELETE_ALL,
    
    # Graph projection queries
    get_main_projection,
    get_similarity_projection,
    get_temporal_projection,
    get_drop_graph_query,
    
    # Property cleanup queries
    get_cleanup_node_properties_query,
    CLEANUP_RELATIONSHIP_PROPERTIES,
    DELETE_SIMILAR_RELATIONSHIPS
)

# Disable Neo4j driver's INFO and WARNING logs
logging.getLogger("neo4j").setLevel(logging.ERROR)

class DatabaseManager:
    def __init__(self, uri, user, password):
        """Khởi tạo kết nối Neo4j."""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        """Đóng kết nối Neo4j."""
        if self.driver:
            self.driver.close()
    
    def run_query(self, query, params=None):
        """Chạy truy vấn Cypher và trả về kết quả."""
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
                        
                    # If we expect to use single() later
                    # Return the first record directly
                    return data[0]
            except Exception as e:
                print(f"Query error: {str(e)}")
                raise e
                    
    def import_data(self, csv_path):
        """Import dữ liệu sử dụng API Neo4j thay vì LOAD CSV"""
        try:            # Đọc file CSV
            df = pd.read_csv(csv_path)
            print(f"Đã đọc file CSV: {len(df)} giao dịch")
            
            # Kiểm tra các cột bắt buộc
            required_columns = ['nameOrig', 'nameDest', 'amount', 'step', 'type']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Thiếu cột {col} trong file CSV")
              # Kiểm tra và xử lý cột is_fraud hoặc isFraud
            if 'is_fraud' not in df.columns:
                if 'isFraud' in df.columns:
                    print("Tìm thấy cột isFraud, mapping sang is_fraud")
                    df['is_fraud'] = df['isFraud']
                else:
                    print("Không tìm thấy cột is_fraud hoặc isFraud trong dữ liệu, tạo cột mặc định với giá trị 0")
                    df['is_fraud'] = 0
            
            with self.driver.session() as session:
                # Tạo index và xóa dữ liệu cũ
                session.run(CREATE_ACCOUNT_INDEX)
                
                # Lấy danh sách tài khoản độc nhất
                all_accounts = set(df['nameOrig'].unique()) | set(df['nameDest'].unique())
                print(f"Tổng số tài khoản: {len(all_accounts)}")
                
                # Giới hạn số tài khoản nếu cần
                if len(all_accounts) > MAX_NODES:
                    print(f"Giới hạn số tài khoản tối đa: {MAX_NODES}")
                    all_accounts = list(all_accounts)[:MAX_NODES]
                
                # 1. Tạo tài khoản (nodes)
                print("Đang tạo tài khoản...")
                start_time = time.time()
                
                account_batches = [list(all_accounts)[i:i+BATCH_SIZE] for i in range(0, len(all_accounts), BATCH_SIZE)]
                for i, account_batch in enumerate(account_batches):
                    session.run(CREATE_ACCOUNTS_QUERY, {"accounts": account_batch})
                    print(f"  Đã tạo {(i+1)*BATCH_SIZE if (i+1)*BATCH_SIZE < len(all_accounts) else len(all_accounts)}/{len(all_accounts)} tài khoản")
                
                # 2. Tạo giao dịch (relationships) - giới hạn số lượng nếu cần
                if len(df) > MAX_RELATIONSHIPS:
                    print(f"Giới hạn số giao dịch tối đa: {MAX_RELATIONSHIPS}")
                    df = df.head(MAX_RELATIONSHIPS)
                
                print("Đang tạo giao dịch...")
                tx_batches = [df.iloc[i:i+BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]
                
                for i, batch_df in enumerate(tx_batches):
                    records = []
                    for _, row in batch_df.iterrows():
                        # Chỉ thêm giao dịch nếu cả hai tài khoản đều trong danh sách tài khoản đã lọc
                        if row['nameOrig'] in all_accounts and row['nameDest'] in all_accounts:
                            records.append({
                                "from_ac": row['nameOrig'],
                                "to_ac": row['nameDest'],
                                "amount": float(row['amount']),
                                "step": int(row['step']),
                                "is_fraud": int(row['is_fraud']),
                                "type": row['type']
                            })
                    
                    session.run(CREATE_TRANSACTIONS_QUERY, {"batch": records})
                    
                    progress = (i+1)/len(tx_batches)*100
                    print(f"  Đã tạo {progress:.1f}% giao dịch ({(i+1)*BATCH_SIZE if (i+1)*BATCH_SIZE < len(df) else len(df)}/{len(df)})")
                
                print(f"Hoàn thành import trong {time.time() - start_time:.2f}s")
                return True
                
        except Exception as e:
            print(f"Lỗi khi import dữ liệu: {e}")
            return False

    def check_data(self):
        """Kiểm tra xem đã có dữ liệu trong database chưa"""
        with self.driver.session() as session:
            result = session.run(COUNT_ALL_NODES)
            count = result.single()["count"]
            
            # Lấy thêm thống kê
            stats = {}
            try:
                accounts = session.run(COUNT_ACCOUNTS).single()
                stats["accounts"] = accounts["count"] if accounts else 0
                
                transactions = session.run(COUNT_TRANSACTIONS).single()
                stats["transactions"] = transactions["count"] if transactions else 0
                
                has_analysis = session.run(CHECK_ANALYZED).single()
                stats["has_analysis"] = has_analysis["count"] > 0 if has_analysis else False
            except:
                stats = {"accounts": 0, "transactions": 0, "has_analysis": False}
                
            return count > 0, stats
        
    def clear_database(self):
        """Xóa toàn bộ dữ liệu trong database"""
        with self.driver.session() as session:
            try:
                # Xóa các indexes trước (nếu có)
                try:
                    session.run(DROP_ACCOUNT_INDEX)
                except:
                    pass
                    
                # Xóa tất cả nodes và relationships
                session.run(DELETE_ALL)
                
                # Kiểm tra lại để đảm bảo đã xóa thành công
                result = session.run(COUNT_ALL_NODES).single()
                is_empty = result["count"] == 0
                
                if is_empty:
                    print("Đã xóa thành công toàn bộ dữ liệu từ database")
                else:
                    print(f"Vẫn còn {result['count']} nodes trong database")
                    
                return is_empty
            except Exception as e:
                print(f"Lỗi khi xóa database: {e}")
                return False
            
    def create_graph_projections(self):
        """Tạo các graph projection dùng cho các thuật toán GDS."""
        print("🔄 Đang tạo các graph projection...")
        
        # Tạo timestamp để đảm bảo tên graph là duy nhất
        timestamp = int(time.time())
        self.main_graph_name = f'main-graph-{timestamp}'
        self.similarity_graph_name = f'account-similarity-{timestamp}'
        self.temporal_graph_name = f'temporal-graph-{timestamp}'
        
        # 1. Graph projection cho các Account và mối quan hệ SENT
        self.run_query(get_main_projection(self.main_graph_name))
        
        # 2. Graph projection cho account similarity
        self.run_query(get_similarity_projection(self.similarity_graph_name))
        
        # 3. Graph projection cho temporal analysis
        self.run_query(get_temporal_projection(self.temporal_graph_name))
        
        print("✅ Đã tạo xong các graph projection.")
            
    def delete_graph_projections(self):
        """Xóa các graph projections đã tạo."""
        print("🔄 Đang xóa các graph projections...")
        
        # Xóa các graph projections cụ thể
        try:
            self.run_query(get_drop_graph_query(self.main_graph_name))
            self.run_query(get_drop_graph_query(self.similarity_graph_name))
            self.run_query(get_drop_graph_query(self.temporal_graph_name))
            self.run_query(get_drop_graph_query(f"{self.main_graph_name}-undirected"))
            self.run_query(get_drop_graph_query(f"{self.main_graph_name}-undirected-tri"))
            print("✅ Đã xóa tất cả các graph projections.")
        except Exception as e:
            print(f"⚠️ Lưu ý khi xóa graph: {str(e)}")
                
    def cleanup_properties(self):
        """Xóa tất cả các thuộc tính được thêm vào trong quá trình phân tích để tránh đầy database."""
        print("🔄 Đang dọn dẹp các thuộc tính phân tích...")
        
        # Danh sách các thuộc tính được thêm vào trong quá trình phân tích
        added_properties = [
            'degScore', 'prScore', 'communityId', 'communitySize', 'normCommunitySize',
            'simScore', 'btwScore', 'hubScore', 'authScore', 'coreScore', 'triCount',
            'cycleCount', 'tempBurst', 'tempBurst1h', 'tempBurst24h', 'anomaly_score', 'flagged'
        ]
        
        try:
            # Xóa thuộc tính trên tất cả các node
            self.run_query(get_cleanup_node_properties_query(added_properties))
            
            # Xóa thuộc tính trên các relationship
            self.run_query(CLEANUP_RELATIONSHIP_PROPERTIES)
            print(f"✅ Đã xóa {len(added_properties)} thuộc tính phân tích khỏi database.")
            
            # Xóa các mối quan hệ SIMILAR (từ Node Similarity)
            self.run_query(DELETE_SIMILAR_RELATIONSHIPS)
            print("✅ Đã xóa các mối quan hệ SIMILAR.")
            
            return True
        except Exception as e:
            print(f"❌ Lỗi khi dọn dẹp thuộc tính: {str(e)}")
            return False