import pandas as pd
import time
from config import BATCH_SIZE, MAX_NODES, MAX_RELATIONSHIPS

class DataImporter:
    def __init__(self, database_manager):
        self.db_manager = database_manager
        
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
            
            with self.db_manager.driver.session() as session:
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
