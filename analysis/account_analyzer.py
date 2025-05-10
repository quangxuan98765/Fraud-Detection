from config import FRAUD_SCORE_THRESHOLD, SUSPICIOUS_THRESHOLD

class AccountAnalyzer:
    def __init__(self, driver):
        self.driver = driver

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
                            WHEN outCount + inCount < 3 THEN 0  // Yêu cầu tối thiểu 3 giao dịch
                            WHEN outCount + inCount = 0 THEN 0
                            ELSE txDiff / (outCount + inCount)
                        END,
                        a.amount_imbalance = CASE
                            WHEN outAmount + inAmount = 0 THEN 0
                            WHEN outAmount + inAmount < 50000 THEN 0
                            ELSE amountDiff / (outAmount + inAmount)
                        END,
                        a.only_sender = CASE 
                            WHEN outCount >= 3 AND inCount = 0 THEN true 
                            ELSE false 
                        END,
                        a.high_tx_volume = CASE 
                            WHEN outCount + inCount > 8 THEN true 
                            ELSE false 
                        END
                        
                    RETURN count(*) as processed
                """
                
                # Thực thi truy vấn
                result = session.run(query, skip=start, limit=batch_size).single()
                processed = result["processed"] if result else 0

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
                    WHERE (
                        (maxAmount > avgAmount * 6 AND txCount >= 3) OR
                        (maxAmount > 150000 AND avgAmount < 30000 AND txCount >= 2) OR
                        (size([x in amounts WHERE x > avgAmount * 4]) >= 2 AND 
                         txCount >= 3 AND 
                         maxAmount > avgAmount * 5) OR
                        (size([x in amounts WHERE x > 60000]) >= 2 AND
                         avgAmount < 20000 AND
                         txCount >= 2)
                    )                    
                    SET a.tx_anomaly = true
                    RETURN count(*) as processed
                """
                
                result = session.run(query, skip=start, limit=batch_size).single()
                processed = result["processed"] if result else 0

            # Xác định tài khoản có giao dịch giá trị cao
            session.run("""
                MATCH (a:Account)
                OPTIONAL MATCH (a)-[tx:SENT]->()
                WITH a, tx.amount as amount, count(tx) as tx_count
                WHERE tx_count > 0
                WITH a, 
                     collect(amount) as amounts,
                     avg(amount) as avg_amount,
                     max(amount) as max_amount,
                     count(amount) as num_transactions
                WHERE 
                    max_amount > 80000 AND
                    max_amount > avg_amount * 3 AND
                    size([x IN amounts WHERE x > 60000]) >= 1 AND
                    num_transactions >= 2
                SET a.high_value_tx = true
            """)
