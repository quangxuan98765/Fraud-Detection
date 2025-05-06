from neo4j import GraphDatabase
import pandas as pd
import os
import time
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, BATCH_SIZE, MAX_NODES, MAX_RELATIONSHIPS

class FraudDetector:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
    def check_data(self):
        """Kiểm tra xem đã có dữ liệu trong database chưa"""
        with self.driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count")
            count = result.single()["count"]
            
            # Lấy thêm thống kê
            stats = {}
            try:
                accounts = session.run("MATCH (a:Account) RETURN count(a) as count").single()
                stats["accounts"] = accounts["count"] if accounts else 0
                
                transactions = session.run("MATCH ()-[r:SENT]->() RETURN count(r) as count").single()
                stats["transactions"] = transactions["count"] if transactions else 0
                
                has_analysis = session.run("MATCH (a:Account) WHERE a.fraud_score IS NOT NULL RETURN count(a) as count").single()
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
                    session.run("DROP INDEX ON :Account(id)")
                except:
                    pass
                    
                # Xóa tất cả nodes và relationships
                session.run("MATCH (n) DETACH DELETE n")
                
                # Kiểm tra lại để đảm bảo đã xóa thành công
                result = session.run("MATCH (n) RETURN count(n) as count").single()
                is_empty = result["count"] == 0
                
                if is_empty:
                    print("Đã xóa thành công toàn bộ dữ liệu từ database")
                else:
                    print(f"Vẫn còn {result['count']} nodes trong database")
                    
                return is_empty
            except Exception as e:
                print(f"Lỗi khi xóa database: {e}")
                return False
            
    def import_data(self, csv_path):
        """Import dữ liệu sử dụng API Neo4j thay vì LOAD CSV"""
        try:
            # Đọc file CSV
            df = pd.read_csv(csv_path)
            print(f"Đã đọc file CSV: {len(df)} giao dịch")
            
            # Kiểm tra các cột bắt buộc
            required_columns = ['nameOrig', 'nameDest', 'amount', 'step', 'isFraud', 'type']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Thiếu cột {col} trong file CSV")
            
            with self.driver.session() as session:
                # Tạo index và xóa dữ liệu cũ
                session.run("CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.id)")
                
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
                    query = """
                    UNWIND $accounts AS id
                    MERGE (a:Account {id: id})
                    """
                    session.run(query, {"accounts": account_batch})
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
                                "is_fraud": int(row['isFraud']),
                                "type": row['type']
                            })
                    
                    query = """
                    UNWIND $batch AS tx
                    MATCH (from:Account {id: tx.from_ac})
                    MATCH (to:Account {id: tx.to_ac})
                    CREATE (from)-[r:SENT {
                        amount: tx.amount,
                        step: tx.step,
                        is_fraud: tx.is_fraud,
                        type: tx.type
                    }]->(to)
                    """
                    session.run(query, {"batch": records})
                    
                    progress = (i+1)/len(tx_batches)*100
                    print(f"  Đã tạo {progress:.1f}% giao dịch ({(i+1)*BATCH_SIZE if (i+1)*BATCH_SIZE < len(df) else len(df)}/{len(df)})")
                
                print(f"Hoàn thành import trong {time.time() - start_time:.2f}s")
                return True
                
        except Exception as e:
            print(f"Lỗi khi import dữ liệu: {e}")
            return False
    
    def analyze_fraud(self):
        """Chạy phân tích gian lận với Graph Projection"""
        with self.driver.session() as session:
            # Xóa dữ liệu phân tích cũ
            print("🔍 Đang xóa phân tích cũ...")
            session.run("MATCH (a:Account) REMOVE a.fraud_score, a.community, a.pagerank_score")
            
            # Tạo index
            session.run("CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.id)")
            
            try:
                # Xóa projected graph cũ nếu có
                print("🔍 Đang xóa projected graph cũ...")
                try:
                    session.run("CALL gds.graph.drop('fraud_graph', false)")
                except:
                    pass
                    
                # 1. Tạo projected graph
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
                
                # 2. Chạy PageRank
                print("🔍 Đang chạy PageRank...")
                session.run("""
                    CALL gds.pageRank.write('fraud_graph', {
                        writeProperty: 'pagerank_score'
                    })
                """)
                
                # 3. Phát hiện cộng đồng
                print("🔍 Đang phát hiện cộng đồng...")
                session.run("""
                    CALL gds.louvain.write('fraud_graph', {
                        writeProperty: 'community'
                    })
                """)
                
                # 4. Tính điểm gian lận
                print("🔍 Đang tính điểm gian lận...")
                session.run("""
                    MATCH (a:Account)
                    WITH a,
                        COALESCE(a.pagerank_score, 0) AS pagerank
                    
                    // Tính số giao dịch đi/đến
                    OPTIONAL MATCH (a)-[out:SENT]->()
                    WITH a, pagerank, count(out) AS out_count, sum(out.amount) AS out_amount
                    
                    OPTIONAL MATCH ()-[in:SENT]->(a)
                    WITH a, pagerank, out_count, out_amount, count(in) AS in_count, sum(in.amount) AS in_amount
                    
                    // Tính điểm gian lận
                    SET a.fraud_score = 
                        0.3 * pagerank +
                        0.3 * (CASE WHEN out_count > in_count THEN 0.8 ELSE 0.2 END) +
                        0.4 * (CASE 
                            WHEN ABS(out_amount - in_amount) > 50000 THEN 0.9
                            WHEN ABS(out_amount - in_amount) > 10000 THEN 0.6
                            ELSE 0.2
                        END)
                """)
                
                # 5. Xóa projected graph để giải phóng bộ nhớ
                print("🔍 Đang xóa projected graph...")
                session.run("CALL gds.graph.drop('fraud_graph', false)")
                
                print("✅ Phân tích gian lận hoàn tất.")
                return True
                
            except Exception as e:
                print(f"Lỗi khi phân tích gian lận: {e}")
    
    def close(self):
        """Đóng kết nối đến Neo4j"""
        if self.driver:
            self.driver.close()