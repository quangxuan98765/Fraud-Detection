class TransactionAnalyzer:
    def __init__(self, driver):
        self.driver = driver

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
