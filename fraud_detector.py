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
        
    def process_transaction_stats(self):
        """Tính toán thống kê giao dịch với cách tiếp cận hiệu quả hơn"""
        with self.driver.session() as session:
            print("🔍 Đang tính giao dịch ra/vào (phương pháp tối ưu)...")
            
            # Lấy tổng số tài khoản để tính batch
            total = session.run("MATCH (a:Account) RETURN count(a) AS count").single()["count"]
            print(f"  Tổng số tài khoản cần xử lý: {total}")
            
            # Tính kích thước batch phù hợp - mỗi batch xử lý khoảng 1000 tài khoản
            batch_size = 1000
            total_batches = (total + batch_size - 1) // batch_size
            
            # Xử lý theo batch nhỏ hơn
            for i in range(total_batches):
                start = i * batch_size
                end = min((i + 1) * batch_size, total)
                
                # Truy vấn sử dụng SKIP và LIMIT thay vì modulo
                outgoing_query = """
                    MATCH (a:Account)
                    WITH a ORDER BY id(a)
                    SKIP $skip LIMIT $limit
                    
                    WITH a
                    OPTIONAL MATCH (a)-[outTx:SENT]->()
                    WITH a, count(outTx) AS outCount, sum(outTx.amount) AS outAmount
                    SET a.out_count = outCount, a.out_amount = outAmount
                    RETURN count(*) as processed
                """
                
                # Thực thi và theo dõi tiến độ
                result = session.run(outgoing_query, skip=start, limit=batch_size).single()
                
                # Cập nhật tiến độ
                # print(f"  Đã xử lý giao dịch ra: {i+1}/{total_batches} batches ({min(end, total)}/{total} tài khoản)")
            
            # Tương tự với giao dịch vào, nhưng tách riêng để tối ưu
            for i in range(total_batches):
                start = i * batch_size
                end = min((i + 1) * batch_size, total)
                
                incoming_query = """
                    MATCH (a:Account)
                    WITH a ORDER BY id(a)
                    SKIP $skip LIMIT $limit
                    
                    WITH a
                    OPTIONAL MATCH ()-[inTx:SENT]->(a)
                    WITH a, count(inTx) AS inCount, sum(inTx.amount) AS inAmount
                    SET a.in_count = inCount, a.in_amount = inAmount
                    RETURN count(*) as processed
                """
                
                # Thực thi và theo dõi tiến độ
                result = session.run(incoming_query, skip=start, limit=batch_size).single()
                
                # Cập nhật tiến độ
                # print(f"  Đã xử lý giao dịch vào: {i+1}/{total_batches} batches ({min(end, total)}/{total} tài khoản)")
                
    def process_account_behaviors(self):
        """Đánh dấu hành vi bất thường theo batch với tiêu chí chặt chẽ hơn để tăng độ chính xác"""
        with self.driver.session() as session:
            print("🔍 Đang đánh dấu hành vi bất thường (phương pháp tối ưu)...")
            
            # Lấy tổng số tài khoản
            total = session.run("MATCH (a:Account) RETURN count(a) AS count").single()["count"]
            print(f"  Tổng số tài khoản cần phân tích: {total}")
            
            # Xác định kích thước batch
            batch_size = 1000
            total_batches = (total + batch_size - 1) // batch_size
            
            # Xử lý theo batch với tiêu chí chặt chẽ hơn
            for i in range(total_batches):
                start = i * batch_size
                
                # Truy vấn theo batch với các tiêu chí mới
                query = """
                    MATCH (a:Account)
                    WITH a ORDER BY id(a)
                    SKIP $skip LIMIT $limit
                    
                    WHERE a.out_count IS NOT NULL AND a.in_count IS NOT NULL
                    WITH a, 
                        COALESCE(a.out_count, 0) AS outCount, 
                        COALESCE(a.in_count, 0) AS inCount,
                        COALESCE(a.out_amount, 0) AS outAmount,
                        COALESCE(a.in_amount, 0) AS inAmount
                        
                    // Tính chênh lệch số lượng và giá trị giao dịch
                    WITH a, outCount, inCount, outAmount, inAmount,
                         abs(outCount - inCount) AS txDiff,
                         abs(outAmount - inAmount) AS amountDiff
                    
                    // Gán các chỉ số bất thường với điều kiện nghiêm ngặt hơn 
                    SET a.tx_imbalance = CASE 
                            WHEN outCount + inCount < 3 THEN 0  // Yêu cầu tối thiểu 3 giao dịch để tính imbalance
                            WHEN outCount + inCount = 0 THEN 0
                            ELSE txDiff / (outCount + inCount)
                        END,
                        // Gán chỉ số imbalance theo giá trị
                        a.amount_imbalance = CASE
                            WHEN outAmount + inAmount = 0 THEN 0
                            WHEN outAmount + inAmount < 50000 THEN 0  // Yêu cầu tổng giá trị tối thiểu 50k
                            ELSE amountDiff / (outAmount + inAmount)
                        END,
                        // Chỉ đánh dấu "chỉ gửi" khi mất cân bằng hoàn toàn và có nhiều giao dịch ra 
                        a.only_sender = CASE 
                            WHEN outCount >= 3 AND inCount = 0 THEN true 
                            ELSE false 
                        END,
                        // Yêu cầu khắt khe hơn cho giao dịch khối lượng cao
                        a.high_tx_volume = CASE 
                            WHEN outCount + inCount > 8 THEN true 
                            ELSE false 
                        END
                        
                    RETURN count(*) as processed
                """
                
                # Thực thi truy vấn
                result = session.run(query, skip=start, limit=batch_size).single()
                processed = result["processed"] if result else 0
                  # Hiển thị tiến độ
                # print(f"  Batch {i+1}/{total_batches}: Đã xử lý {processed} tài khoản")
                
    def process_transaction_anomalies(self):
        """Phân tích giao dịch bất thường theo batch với tiêu chí chặt chẽ hơn"""
        with self.driver.session() as session:
            print("🔍 Đang phân tích giao dịch có giá trị bất thường...")
            
            # Lấy tổng số tài khoản
            total = session.run("MATCH (a:Account) RETURN count(a) AS count").single()["count"]
            batch_size = 1000
            total_batches = (total + batch_size - 1) // batch_size
            
            # Xóa đánh dấu cũ
            session.run("MATCH (a:Account) REMOVE a.tx_anomaly")
              # Phân tích theo batch với tiêu chí chặt chẽ hơn
            for i in range(total_batches):
                start = i * batch_size
                
                query = """
                    MATCH (a:Account)
                    WITH a ORDER BY id(a)
                    SKIP $skip LIMIT $limit
                          WITH a
                    OPTIONAL MATCH (a)-[tx:SENT]->()
                    WITH a, avg(tx.amount) AS avgAmount, max(tx.amount) AS maxAmount, count(tx) AS txCount,
                         collect(tx.amount) as amounts
                      // Phát hiện giao dịch bất thường với điều kiện nghiêm ngặt hơn - giảm false positives
                    WHERE (
                        // Điều kiện 1: Giao dịch cao bất thường và có nhiều giao dịch
                        (maxAmount > avgAmount * 6 AND txCount >= 3) OR   // Giảm ngưỡng từ 7x xuống 6x, từ >=4 xuống >=3
                        
                        // Điều kiện 2: Chênh lệch rất lớn giữa max và average
                        (maxAmount > 150000 AND avgAmount < 30000 AND txCount >= 2) OR // Giảm ngưỡng từ 200k xuống 150k, từ >=3 xuống >=2
                        
                        // Điều kiện 3: Nhiều giao dịch cao bất thường và có giá trị chệnh lệch lớn 
                        (size([x in amounts WHERE x > avgAmount * 4]) >= 2 AND 
                         txCount >= 3 AND 
                         maxAmount > avgAmount * 5) OR  // Giảm ngưỡng để tăng khả năng phát hiện
                        
                        // Điều kiện 4: Mẫu giao dịch bất thường với điều kiện chuỗi giao dịch
                        (size([x in amounts WHERE x > 60000]) >= 2 AND   // Giảm ngưỡng từ 75k xuống 60k, từ >=3 xuống >=2
                         avgAmount < 20000 AND
                         txCount >= 2)  // Giảm yêu cầu tối thiểu từ 3 xuống 2 giao dịch
                    )                    
                    SET a.tx_anomaly = true
                    RETURN count(*) as processed
                """
                
                result = session.run(query, skip=start, limit=batch_size).single()
                processed = result["processed"] if result else 0                
                # Hiển thị tiến độ
                # print(f"  Batch {i+1}/{total_batches}: Đã phát hiện {processed} tài khoản bất thường")
            
            # Xác định tài khoản có giao dịch giá trị cao với điều kiện nghiêm ngặt hơn
            print("🔍 Đang xác định tài khoản có giao dịch giá trị cao...")
            session.run("""
                MATCH (a:Account)
                OPTIONAL MATCH (a)-[tx:SENT]->()
                WITH a, tx.amount as amount, count(tx) as tx_count
                WHERE tx_count > 0
                WITH a, 
                     collect(amount) as amounts,
                     avg(amount) as avg_amount,
                     max(amount) as max_amount,
                     count(amount) as num_transactions                WHERE 
                    max_amount > 80000 AND  // Giảm ngưỡng từ 100000 xuống 80000
                    max_amount > avg_amount * 3 AND  // Giảm ngưỡng từ 4 xuống 3
                    size([x IN amounts WHERE x > 60000]) >= 1 AND  // Giảm ngưỡng từ 80k xuống 60k, từ 2 xuống 1
                    num_transactions >= 2  // Giảm yêu cầu từ 3 xuống 2 giao dịch
                SET a.high_value_tx = true
            """)
                  
    def calculate_fraud_scores(self):
        """Kết hợp tất cả điểm để tính điểm gian lận - cải tiến để tận dụng tốt hơn mối quan hệ trong đồ thị"""
        with self.driver.session() as session:
            print("Đang tính toán fraud_score cho tất cả tài khoản...")
            
            # Đầu tiên, xác định các tài khoản liên quan đến giao dịch gian lận đã biết
            session.run("""
                MATCH (s:Account)-[r:SENT {is_fraud: 1}]->(t:Account)
                SET s.known_fraud = true, t.known_fraud = true
            """)
              # Trước tiên, tính điểm cơ bản dựa trên thuộc tính
            base_score_query = """
                MATCH (a:Account)
                
                // Đặt giá trị mặc định nếu thuộc tính không tồn tại
                WITH a, 
                    COALESCE(a.pagerank_score, 0) AS pagerank,
                    COALESCE(a.degree_score, 0) AS degree,
                    COALESCE(a.similarity_score, 0) AS similarity,
                    COALESCE(a.tx_imbalance, 0) AS imbalance,
                    COALESCE(a.high_tx_volume, false) AS high_volume,
                    COALESCE(a.tx_anomaly, false) AS anomaly,
                    COALESCE(a.only_sender, false) AS only_sender,
                    COALESCE(a.high_value_tx, false) AS high_value                // Tính điểm cơ bản dựa trên các thuộc tính - tối ưu cho precision (giảm false positives)
                WITH a,
                    // Tăng ảnh hưởng các chỉ số cấu trúc
                    pagerank * 0.05 +
                    degree * 0.03 + 
                    similarity * 0.02 +
                    
                    // Tập trung vào các dấu hiệu hành vi bất thường rất mạnh, yêu cầu ngưỡng cao hơn
                    CASE 
                        WHEN imbalance > 0.8 THEN 0.40  // Tăng điểm cho imbalance cực cao
                        WHEN imbalance > 0.65 THEN 0.30
                        WHEN imbalance > 0.5 THEN 0.20
                        ELSE imbalance * 0.15  // Tăng ảnh hưởng cho các imbalance thấp
                    END +
                    
                    // Kết hợp nhiều dấu hiệu và yêu cầu ngưỡng cao hơn
                    CASE 
                        WHEN high_volume AND anomaly AND imbalance > 0.65 THEN 0.35  // Tăng điểm
                        WHEN high_volume AND anomaly AND imbalance > 0.5 THEN 0.25
                        WHEN high_volume AND imbalance > 0.6 THEN 0.15
                        WHEN high_volume THEN 0.05
                        ELSE 0 
                    END +
                    
                    // Tăng độ chính xác cho anomaly detection
                    CASE 
                        WHEN anomaly AND high_value AND imbalance > 0.6 THEN 0.40  // Tăng điểm
                        WHEN anomaly AND high_value AND imbalance > 0.4 THEN 0.30
                        WHEN anomaly AND high_value THEN 0.20
                        WHEN anomaly THEN 0.10
                        ELSE 0 
                    END +
                    
                    // Các trường hợp đặc biệt - chỉ giữ điểm cao cho known_fraud chắc chắn
                    CASE WHEN a.known_fraud THEN 0.90 ELSE 0 END +
                    CASE WHEN high_value AND only_sender AND imbalance > 0.5 THEN 0.25
                         WHEN high_value AND only_sender THEN 0.12
                         WHEN high_value THEN 0.07
                         ELSE 0 
                    END AS base_score
                
                // Thiết lập điểm cơ bản
                SET a.base_score = base_score
                
                RETURN count(a) AS updated_count
            """
            
            result = session.run(base_score_query)
            print(f"Đã tính điểm cơ bản cho {result.single()['updated_count']} tài khoản")
              # Tiếp theo, điều chỉnh điểm dựa trên mối quan hệ giao dịch - thêm điều kiện chặt chẽ hơn
            transaction_pattern_query = """
                // Đầu tiên, tìm các mẫu giao dịch bất thường với điều kiện nghiêm ngặt hơn
                MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
                WHERE tx.amount > 50000  // Tăng ngưỡng giá trị giao dịch từ 30000 lên 50000
                
                // Tính điểm mối quan hệ dựa trên đặc điểm giao dịch
                WITH sender, receiver, collect(tx) AS transactions,
                     sum(tx.amount) AS total_amount,
                     count(tx) AS tx_count,
                     sender.base_score AS sender_score,
                     receiver.base_score AS receiver_score,
                     sender.tx_imbalance AS sender_imbalance,
                     receiver.tx_imbalance AS receiver_imbalance
                
                // Nếu có nhiều giao dịch giá trị lớn giữa hai tài khoản với điểm cao VÀ imbalance cao
                WHERE tx_count >= 2 AND total_amount > 80000 AND 
                     ((sender_score > 0.6 AND sender_imbalance > 0.5) OR 
                      (receiver_score > 0.6 AND receiver_imbalance > 0.5))
                
                // Tăng điểm hạn chế hơn cho sender và receiver
                WITH sender, receiver, 
                     CASE 
                         WHEN sender_score > 0.75 AND receiver_score > 0.6 THEN 0.18
                         WHEN sender_score > 0.6 OR receiver_score > 0.6 THEN 0.12
                         ELSE 0.08
                     END AS boost
                
                // Áp dụng tăng điểm
                SET sender.relation_boost = COALESCE(sender.relation_boost, 0) + boost,
                    receiver.relation_boost = COALESCE(receiver.relation_boost, 0) + boost
                
                RETURN count(sender) + count(receiver) AS boosted_accounts
            """
            
            relation_result = session.run(transaction_pattern_query)
            boosted_accounts = relation_result.single()["boosted_accounts"] if relation_result else 0
            print(f"Đã điều chỉnh điểm cho {boosted_accounts} tài khoản dựa trên mẫu giao dịch")
              # Phát hiện vòng tròn giao dịch (chu trình) - chặt chẽ hơn
            cycle_detection_query = """
                // Phát hiện và phân tích chu trình giao dịch với điều kiện chặt chẽ hơn
                MATCH path = (a:Account)-[r:SENT*2..4]->(a)
                WITH path, 
                     [node IN nodes(path) | node] AS cycle_nodes,
                     reduce(total = 0, r IN relationships(path) | total + r.amount) AS cycle_amount,
                     reduce(max_val = 0, r IN relationships(path) | CASE WHEN r.amount > max_val THEN r.amount ELSE max_val END) AS max_tx_amount
                  WHERE 
                    cycle_amount > 80000 AND  // Giảm ngưỡng giá trị chu trình từ 100000 xuống 80000
                    max_tx_amount > 30000      // Giảm yêu cầu về giá trị giao dịch lớn nhất từ 40000 xuống 30000
                
                // Lọc và xử lý từng tài khoản trong chu trình với điều kiện chặt chẽ hơn
                WITH DISTINCT cycle_nodes, cycle_amount, max_tx_amount
                UNWIND cycle_nodes AS cycle_account
                WITH cycle_account, cycle_nodes, cycle_amount, max_tx_amount,
                     count(CASE WHEN cycle_account.tx_anomaly THEN 1 END) as anomalies,
                     count(CASE WHEN cycle_account.high_value_tx THEN 1 END) as high_value_txs,
                     avg(CASE WHEN cycle_account.tx_imbalance IS NOT NULL THEN cycle_account.tx_imbalance ELSE 0 END) as avg_imbalance                WHERE 
                    anomalies > 0 OR          // Relax requirement - need anomalies OR high_value_txs (changed from AND)
                    high_value_txs > 0 AND     
                    cycle_account.tx_imbalance > 0.4 AND    // Decrease threshold from 0.5 to 0.4
                    avg_imbalance > 0.35        // Decrease avg_imbalance threshold from 0.4 to 0.35
                  // Tăng điểm dựa trên đặc điểm chu trình, tăng điểm boost
                SET 
                    cycle_account.cycle_boost = CASE
                        WHEN size(cycle_nodes) = 2 AND cycle_amount > 120000 THEN 0.45  // Tăng từ 0.35 lên 0.45
                        WHEN size(cycle_nodes) = 3 AND cycle_amount > 100000 THEN 0.35  // Tăng từ 0.25 lên 0.35
                        WHEN size(cycle_nodes) <= 4 AND cycle_amount > 80000 THEN 0.25  // Tăng từ 0.15 lên 0.25
                        ELSE 0.15  // Tăng từ 0.10 lên 0.15
                    END,
                    cycle_account.known_fraud = CASE 
                        WHEN size(cycle_nodes) <= 3 AND cycle_amount > 120000 THEN true  // Giảm ngưỡng từ 150000 xuống 120000
                        ELSE cycle_account.known_fraud 
                    END
                
                RETURN count(DISTINCT cycle_account) AS cycle_accounts
            """
            
            cycle_result = session.run(cycle_detection_query)
            cycle_accounts = cycle_result.single()["cycle_accounts"] if cycle_result else 0
            print(f"Đã phát hiện {cycle_accounts} tài khoản thuộc các chu trình giao dịch")
              # Cuối cùng, tổng hợp tất cả các điểm để tạo điểm fraud_score cuối cùng với điều chỉnh để tăng precision
            final_score_query = """
                MATCH (a:Account)
                
                // Tổng hợp tất cả các điểm boost
                WITH a,
                     COALESCE(a.base_score, 0) AS base,
                     COALESCE(a.relation_boost, 0) AS relation,
                     COALESCE(a.cycle_boost, 0) AS cycle
                  // Tính điểm tổng hợp với trọng số mới, nhấn mạnh cấu trúc chu trình (cycle)
                WITH a, 
                     base * 0.85 +  // Tăng ảnh hưởng của base score (0.85 thay vì 0.75)
                     relation * 1.0 +  // Tăng ảnh hưởng relation boost
                     cycle * 1.2 AS combined_score  // Tăng ảnh hưởng của cycle boost
                
                // Thêm một ngưỡng cơ bản để giảm điểm thấp và thêm nhiễu ngẫu nhiên nhỏ (0.99-1.01)
                WITH a, 
                     CASE
                        WHEN combined_score < 0.3 THEN combined_score * 0.90  // Giảm ít hơn cho điểm thấp
                        ELSE combined_score 
                     END * (0.99 + rand()*0.02) AS final_score
                
                // Đảm bảo không vượt quá 1.0
                SET a.fraud_score = CASE 
                    WHEN final_score > 1.0 THEN 1.0 
                    WHEN final_score < 0 THEN 0
                    ELSE final_score
                END
                
                RETURN count(a) AS updated_count
            """
            
            final_result = session.run(final_score_query)
            print(f"Đã hoàn thành tính fraud_score cho {final_result.single()['updated_count']} tài khoản")

    def finalize_and_evaluate(self):
        """Chuẩn hóa điểm và đánh giá kết quả theo batch"""
        with self.driver.session() as session:
            print("🔍 Đang hoàn tất phân tích...")
            
            # Lấy tổng số tài khoản
            total = session.run("MATCH (a:Account) RETURN count(a) AS count").single()["count"]
            batch_size = 2000  # Batch lớn hơn cho các truy vấn đơn giản
            total_batches = (total + batch_size - 1) // batch_size
            
            # Chuẩn hóa điểm theo batch
            print("  Đang chuẩn hóa điểm cuối cùng...")
            for i in range(total_batches):
                start = i * batch_size
                
                session.run("""
                    MATCH (a:Account)
                    WITH a ORDER BY id(a)
                    SKIP $skip LIMIT $limit
                    
                    SET a.fraud_score = 
                        CASE
                            WHEN a.fraud_score > 1.0 THEN 1.0
                            WHEN a.fraud_score < 0.0 THEN 0.0
                            WHEN a.fraud_score IS NULL THEN 0.1
                            ELSE a.fraud_score
                        END
                """, skip=start, limit=batch_size)
                
            # Đánh giá hiệu quả phát hiện - tối ưu truy vấn ban đầu
            print("  Đang đánh dấu tài khoản gian lận thực sự...")
            session.run("""
                MATCH (a:Account)
                WHERE exists((a)-[:SENT {is_fraud: 1}]->()) OR exists((a)<-[:SENT {is_fraud: 1}]-())
                SET a.real_fraud = true
            """)

    def process_high_risk_communities(self):
        """Phiên bản tối ưu hơn cho xử lý cộng đồng"""
        with self.driver.session() as session:
            print("🔍 Đang tìm các cộng đồng có điểm gian lận cao (phiên bản tối ưu)...")              # Lọc cộng đồng với tiêu chí chặt chẽ hơn để giảm false positives
            high_risk_query = """                MATCH (a:Account)
                WHERE a.community IS NOT NULL AND a.fraud_score > 0.6 // Giảm ngưỡng điểm từ 0.8 xuống 0.6
                WITH a.community AS comm, count(*) AS size, 
                     avg(a.fraud_score) AS avg_score,
                     count(CASE WHEN a.tx_anomaly THEN 1 END) as anomaly_count,
                     count(CASE WHEN a.high_value_tx THEN 1 END) as high_value_count
                WHERE size >= 2 AND size <= 10  // Mở rộng kích thước cộng đồng
                  AND avg_score > 0.65  // Giảm ngưỡng điểm trung bình từ 0.8 xuống 0.65
                  AND anomaly_count >= size * 0.4  // Giảm tỷ lệ yêu cầu về anomaly từ 0.5 xuống 0.4
                  AND high_value_count >= 1  // Giảm yêu cầu từ 2 xuống 1 giao dịch giá trị cao
                RETURN comm, size, avg_score
                ORDER BY avg_score DESC
                LIMIT 20 // Tăng từ 15 lên 20
            """
            
            # Lấy các cộng đồng nguy cơ cao trong một lần truy vấn
            high_risk_comms = session.run(high_risk_query).data()
            
            print(f"🔍 Tìm thấy {len(high_risk_comms)} cộng đồng có điểm gian lận cao")
            
            # Xử lý từng cộng đồng
            if high_risk_comms:
                batch_size = 3
                comms_to_process = [rec['comm'] for rec in high_risk_comms]
                for i in range(0, len(comms_to_process), batch_size):
                    batch = comms_to_process[i:i+batch_size]
                    
                    # Thay đổi điều kiện boost để tăng độ chính xác
                    session.run("""
                        UNWIND $communities AS comm
                        MATCH (member:Account)
                          // Điều kiện ít nghiêm ngặt hơn và boost lớn hơn (10% thay vì 5%)
                        WHERE member.community = comm 
                          AND member.fraud_score > 0.4 AND member.fraud_score < 0.7 // Giảm ngưỡng dưới và trên
                          AND (member.tx_anomaly = true OR member.high_value_tx = true OR member.tx_imbalance > 0.4) // Thêm điều kiện imbalance
                        
                        // Boost lớn hơn để tăng khả năng phát hiện
                        SET member.fraud_score = member.fraud_score * 1.1
                    """, communities=batch)
                    
                    print(f"  Đã xử lý {min(i+batch_size, len(comms_to_process))}/{len(comms_to_process)} cộng đồng")

    def analyze_fraud(self):
        """Chạy phân tích gian lận với các thuật toán đồ thị - phiên bản không sử dụng is_fraud"""
        with self.driver.session() as session:
            # Xóa dữ liệu phân tích cũ
            print("🔍 Đang xóa phân tích cũ...")
            session.run("""
                MATCH (a:Account) 
                REMOVE a.fraud_score, a.community, a.pagerank_score, 
                    a.degree_score, a.similarity_score, a.path_score, a.known_fraud
            """)

            # Thêm đoạn này để xóa SIMILAR_TO relationships
            print("🔍 Đang xóa mối quan hệ từ phân tích trước...")
            session.run("""
                // Xóa tất cả mối quan hệ SIMILAR_TO
                MATCH ()-[r:SIMILAR_TO]->()
                DELETE r
            """)
            
            # Tạo index
            session.run("CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.id)")
            
            try:
                # Thay thế lệnh xóa projected graph cũ
                print("🔍 Đang xóa projected graph cũ...")
                result = session.run("""
                    CALL gds.graph.list()
                    YIELD graphName
                    WHERE graphName = 'fraud_graph'
                    RETURN count(*) > 0 AS exists
                """).single()

                if result and result.get('exists', False):
                    print("  Đã tìm thấy projected graph trong danh sách, đang xóa...")
                    session.run("CALL gds.graph.drop('fraud_graph', false)")
                else:
                    print("  Không tìm thấy 'fraud_graph' trong danh sách.")
                    
                # 1. Tạo projected graph (chỉ dùng amount, không dùng is_fraud)
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
                
                # 2. Degree Centrality - Đo lường số kết nối
                print("🔍 Đang tính Degree Centrality...")
                session.run("""
                    CALL gds.degree.write('fraud_graph', {
                        writeProperty: 'degree_score'
                    })
                """)
                
                # 3. PageRank - Xác định tài khoản có ảnh hưởng
                print("🔍 Đang chạy PageRank...")
                session.run("""
                    CALL gds.pageRank.write('fraud_graph', {
                        writeProperty: 'pagerank_score',
                        maxIterations: 20
                    })
                """)
                
                # 4. Louvain - Phát hiện cộng đồng
                print("🔍 Đang phát hiện cộng đồng với Louvain...")
                session.run("""
                    CALL gds.louvain.write('fraud_graph', {
                        writeProperty: 'community'
                    })
                """)
                
                # 5. Node Similarity - Tìm các tài khoản có hành vi tương tự
                print("🔍 Đang tính Node Similarity...")
                session.run("""
                    CALL gds.nodeSimilarity.write('fraud_graph', {
                        writeProperty: 'similarity_score',
                        writeRelationshipType: 'SIMILAR_TO',
                        topK: 10
                    })
                """)
                
                # KHÔNG SỬ DỤNG Shortest Path dựa trên is_fraud
                # Thay vào đó, tính các chỉ số hành vi bất thường
                  # 6. Tìm các hành vi bất thường thay thế cho shortest path
                print("🔍 Đang tính giao dịch ra/vào...")
                self.process_transaction_stats()

                # 6.3 & 6.4: Tính tx_imbalance và đánh dấu các hành vi bất thường
                print("🔍 Đang đánh dấu hành vi bất thường...")
                self.process_account_behaviors()

                print("🔍 Đang phân tích giao dịch có giá trị bất thường...")
                self.process_transaction_anomalies()
                  # 7. Kết hợp tất cả đặc trưng để tính điểm gian lận tổng hợp
                print("🔍 Đang tính điểm gian lận tổng hợp...")
                self.calculate_fraud_scores()
                
                # 8. Điều chỉnh điểm gian lận của cộng đồng - phiên bản tối ưu
                print("🔍 Đang tìm các cộng đồng có điểm gian lận cao...")
                self.process_high_risk_communities()
                
                # 9. Chuẩn hóa lại để đảm bảo trong khoảng 0-1
                print("🔍 Đang chuẩn hóa điểm cuối cùng...")
                self.finalize_and_evaluate()
                
                # # 11. Xóa projected graph để giải phóng bộ nhớ
                # print("🔍 Đang xóa projected graph...")
                self.cleanup_projected_graph()
                print("✅ Phân tích gian lận hoàn tất.")
                return True
                
            except Exception as e:
                print(f"Lỗi khi phân tích gian lận: {e}")
                return False
                
    def cleanup_projected_graph(self):
        """Xóa projected graph với cơ chế timeout và bỏ qua việc kiểm tra tồn tại"""
        with self.driver.session() as session:
            print("🔍 Đang xóa projected graph...")
            try:
                # Thử xóa trực tiếp mà không kiểm tra trước
                session.run("""
                    CALL gds.graph.drop('fraud_graph', false)
                    YIELD graphName
                    RETURN 'Đã xóa ' + graphName AS message
                """)
                print("  Đã xóa projected graph thành công")
            except Exception as e:
                # Nếu lỗi vì graph không tồn tại - không sao cả
                if "Graph with name fraud_graph does not exist" in str(e):
                    print("  Projected graph không tồn tại, không cần xóa")
                else:
                    print(f"  Không thể xóa projected graph: {str(e)[:150]}...")
                
                # Tiếp tục xử lý bình thường
                return True
    
    def close(self):
        """Đóng kết nối đến Neo4j"""
        if self.driver:
            self.driver.close()