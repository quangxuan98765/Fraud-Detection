"""
Chứa các thuật toán đồ thị nâng cao để cải thiện độ chính xác phát hiện gian lận
"""
from .database_manager import DatabaseManager
import time

class AdvancedGraphAlgorithms:
    def __init__(self, db_manager: DatabaseManager, main_graph_name=None):
        self.db_manager = db_manager
        self.main_graph_name = main_graph_name
        # Thêm tên các graph mới
        self.embedding_graph_name = "embedding-graph"
        self.pattern_graph_name = "pattern-graph"
        self.temporal_pattern_graph_name = "temporal-pattern-graph"

    def run_advanced_algorithms(self):
        """Chạy các thuật toán đồ thị nâng cao để cải thiện độ chính xác."""
        print("🔄 Đang chạy các thuật toán phân tích đồ thị nâng cao...")
        
        # Kiểm tra và tạo graph projection nếu cần
        self._ensure_graph_projections()
        
        # 1. Node Embedding với FastRP
        self._run_node_embedding()
        
        # 2. Phát hiện cấu trúc mẫu đặc trưng của gian lận
        self._detect_fraud_patterns()
        
        # 3. Mạng đồ thị thời gian (Temporal Graph Network)
        self._analyze_temporal_patterns()
        
        # 4. Phát hiện các cấu trúc vòng lặp đáng ngờ phức tạp hơn
        self._detect_complex_cycles()
        
        # 5. Phân tích dòng tiền đáng ngờ
        self._analyze_money_flow()
        
        # 6. Phát hiện các cụm giao dịch đáng ngờ
        self._detect_suspicious_communities()
        
        # Dọn dẹp các graph projections
        self._cleanup_graph_projections()
        
        print("✅ Đã chạy xong các thuật toán đồ thị nâng cao.")
    
    def _ensure_graph_projections(self):
        """Kiểm tra và tạo các graph projections cần thiết."""
        print("  - Kiểm tra và tạo graph projections...")
        
        # Kiểm tra xem graph chính đã tồn tại hay chưa
        check_query = """
        CALL gds.graph.exists($graphName)
        YIELD exists
        RETURN exists
        """
        
        result = self.db_manager.run_query(check_query, {"graphName": self.main_graph_name})
        
        if not result or not result.get("exists", False):
            print(f"  - Graph '{self.main_graph_name}' không tồn tại, đang tạo mới...")
            
            # Tạo graph projection chính cho phân tích
            create_main_query = """
            CALL gds.graph.project(
                $graphName,
                'Account',
                {
                    SENT: {
                        type: 'SENT',
                        orientation: 'NATURAL',
                        properties: {
                            weight: {
                                property: 'amount',
                                defaultValue: 0.0,
                                aggregation: 'NONE'
                            }
                        }
                    }
                }
            )
            """
            
            try:
                self.db_manager.run_query(create_main_query, {"graphName": self.main_graph_name})
                print(f"  ✅ Đã tạo graph projection '{self.main_graph_name}'")
            except Exception as e:
                print(f"  ❌ Lỗi khi tạo graph projection: {str(e)}")
                # Nếu không tạo được graph chính, đặt tên graph mới
                self.main_graph_name = f"transactions-graph-{int(time.time())}"
                print(f"  🔄 Thử tạo với tên mới: {self.main_graph_name}")
                self.db_manager.run_query(create_main_query, {"graphName": self.main_graph_name})
                
        else:
            print(f"  ✅ Graph projection '{self.main_graph_name}' đã tồn tại")
            
        # Tạo graph cho embedding nếu cần
        self._create_graph_for_embedding()
        
    def _create_graph_for_embedding(self):
        """Tạo graph projection cụ thể cho FastRP embedding."""
        check_query = """
        CALL gds.graph.exists($graphName) 
        YIELD exists
        RETURN exists
        """
        
        result = self.db_manager.run_query(check_query, {"graphName": self.embedding_graph_name})
        
        if not result or not result.get("exists", False):
            print(f"  - Đang tạo graph projection '{self.embedding_graph_name}' cho embedding...")
            
            create_query = """
            CALL gds.graph.project(
                $graphName,
                'Account',
                {
                    SENT: {
                        type: 'SENT',
                        orientation: 'NATURAL',
                        properties: {
                            weight: {
                                property: 'amount',
                                defaultValue: 0.0,
                                aggregation: 'SUM'
                            }
                        }
                    }
                },
                {
                    nodeProperties: [
                        'degScore', 'hubScore', 'btwScore', 'maxAmountRatio'
                    ]
                }
            )
            """
            
            try:
                self.db_manager.run_query(create_query, {"graphName": self.embedding_graph_name})
                print(f"  ✅ Đã tạo graph '{self.embedding_graph_name}' cho embedding")
            except Exception as e:
                print(f"  ⚠️ Lỗi khi tạo graph cho embedding: {str(e)}")
                print("  ⚠️ Sẽ tiếp tục với graph chính")
                self.embedding_graph_name = self.main_graph_name
    
    def _cleanup_graph_projections(self):
        """Dọn dẹp các graph projections đã tạo."""
        print("  - Đang dọn dẹp các graph projections...")
        
        # Danh sách các graph cần dọn dẹp
        graph_names = [
            self.embedding_graph_name,
            self.pattern_graph_name,
            self.temporal_pattern_graph_name
        ]
        
        for graph_name in graph_names:
            if graph_name and graph_name != self.main_graph_name:  # Không xóa graph chính
                try:
                    drop_query = "CALL gds.graph.drop($graphName, false)"
                    self.db_manager.run_query(drop_query, {"graphName": graph_name})
                    print(f"  ✅ Đã xóa graph '{graph_name}'")
                except Exception as e:
                    print(f"  ⚠️ Không thể xóa graph '{graph_name}': {str(e)}")
        
        # Không xóa graph chính vì có thể được sử dụng tiếp
        print(f"  ℹ️ Giữ lại graph chính '{self.main_graph_name}' để sử dụng tiếp")
    
    def _run_node_embedding(self):
        """Chạy thuật toán nhúng node (node embedding) FastRP."""
        print("  - Đang chạy Node Embedding với FastRP...")
        
        # Tạo embedding cho các node
        query = f"""
        CALL gds.fastRP.write(
            '{self.embedding_graph_name}',
            {{
                writeProperty: 'embedding',
                embeddingDimension: 128,
                iterationWeights: [0.8, 1.0, 1.0, 1.0],
                relationshipWeightProperty: 'weight',
                featureProperties: ['degScore', 'hubScore', 'btwScore', 'maxAmountRatio']
            }}
        )
        """
        self.db_manager.run_query(query)
          # Sử dụng embedding để tính toán fraud score
        embedding_score_query = """
        MATCH (a:Account)
        WHERE a.embedding IS NOT NULL AND size(a.embedding) > 0
        WITH a, a.embedding AS emb,
             // Tăng cường feature này cho tài khoản có degree cao
             a.degScore * 1.5 AS weightedDegree,
             a.hubScore * 1.2 AS weightedHub
             
        // Chuyển đổi embedding vector thành single score
        WITH a, 
            REDUCE(score = 0.0, i in range(0, size(emb)-1) | 
                score + CASE 
                    // Tăng ảnh hưởng của các chiều embedding quan trọng
                    WHEN i < 16 THEN emb[i] * 1.5  
                    WHEN i < 32 THEN emb[i] * 1.2
                    ELSE emb[i]
                END
            ) / size(emb) AS embScore,
            weightedDegree,
            weightedHub
            
        // Kết hợp embedding score với các đặc trưng quan trọng khác
        SET a.embeddingScore = embScore,
            a.combinedScore = (embScore * 0.6) + (weightedDegree * 0.3) + (weightedHub * 0.1)
        """
        self.db_manager.run_query(embedding_score_query)
        
        # Chuẩn hóa combinedScore
        normalize_query = """
        MATCH (a:Account)
        WHERE a.combinedScore IS NOT NULL
        WITH MIN(a.combinedScore) AS min_val, MAX(a.combinedScore) AS max_val
        MATCH (a:Account)
        WHERE a.combinedScore IS NOT NULL
        SET a.combinedScore = (a.combinedScore - min_val) / (max_val - min_val)
        """
        self.db_manager.run_query(normalize_query)
    
    def _detect_fraud_patterns(self):
        """Phát hiện mẫu gian lận dựa trên cấu trúc đồ thị."""
        print("  - Đang phát hiện mẫu gian lận đặc trưng...")
        
        # Mẫu 1: Phát hiện mô hình "Fan-out/Fan-in" (phân tán/tập trung)
        fan_pattern_query = """
        // Fan-out pattern (1 tài khoản gửi tiền cho nhiều tài khoản khác)
        MATCH (a:Account)-[sent:SENT]->(receiver:Account)
        WITH a, count(receiver) AS fanOutCount, sum(sent.amount) AS totalSent
        WHERE fanOutCount > 5
        
        // Phát hiện giao dịch tiếp theo (phân tán rồi tập trung lại)
        OPTIONAL MATCH (receiver:Account)<-[s1:SENT]-(a)-[s2:SENT]->(intermediate:Account)-[s3:SENT]->(collector:Account)
        WHERE s1.step = s2.step AND s2.step < s3.step AND s3.step - s2.step <= 5
        WITH a, fanOutCount, totalSent, COUNT(DISTINCT collector) AS collectCount
        
        // Tính điểm fan pattern
        SET a.fanPatternScore = CASE 
            WHEN collectCount > 0 THEN 0.8  // Phát hiện mẫu fan-out/fan-in
            WHEN fanOutCount > 15 THEN 0.6  // Nhiều giao dịch đi
            WHEN fanOutCount > 10 THEN 0.4  // Khá nhiều giao dịch đi
            ELSE (fanOutCount / 10) * 0.3   // Tỷ lệ thấp hơn
        END
        """
        self.db_manager.run_query(fan_pattern_query)
        
        # Mẫu 2: Phát hiện mô hình "Money mule" (chuyển tiền qua nhiều trung gian)
        mule_pattern_query = """
        // Tìm đường đi từ source đến final destination chỉ trong vài bước
        MATCH path = (src:Account)-[:SENT]->()-[:RECEIVED]->(a1:Account)-[:SENT]->()-[:RECEIVED]->(a2:Account)
        WHERE id(src) <> id(a2)  // không phải chu trình
        WITH src, a1, a2, path
        
        // Kiểm tra tính chất đáng ngờ: thời gian rất ngắn giữa lúc nhận và gửi tiếp
        MATCH (a1)-[tx1:SENT]->()-[:RECEIVED]->(a2)
        MATCH (src)-[tx0:SENT]->()-[:RECEIVED]->(a1)
        WHERE abs(tx1.step - tx0.step) <= 3  // Chuyển tiền nhanh chóng
        
        // Tính điểm mule pattern
        WITH src, a1, a2, tx0.amount AS initialAmount, tx1.amount AS relayedAmount
        WITH src, a1, a2, 
            abs(initialAmount - relayedAmount) / 
                CASE WHEN initialAmount = 0 THEN 1 ELSE initialAmount END AS amountRatio
        
        // Đánh dấu các tài khoản trung gian
        SET a1.mulePatternScore = CASE
            WHEN amountRatio < 0.1 THEN 0.9  // Số tiền gần như không đổi
            WHEN amountRatio < 0.2 THEN 0.7  // Số tiền thay đổi ít
            ELSE 0.5                        // Số tiền thay đổi nhiều
        END
        """
        self.db_manager.run_query(mule_pattern_query)
        
        # Mẫu 3: Phát hiện mô hình "Structuring" (chia nhỏ giao dịch tránh ngưỡng)
        structuring_query = """
        // Tìm các cặp tài khoản có nhiều giao dịch nhỏ trong thời gian ngắn
        MATCH (src:Account)-[sent:SENT]->(receiver:Account)
        WITH src, receiver, count(sent) AS txCount, 
            collect(sent.amount) AS amounts,
            collect(sent.step) AS steps
        WHERE txCount >= 3
        
        // Tính toán min và max của steps riêng biệt
        WITH src, receiver, txCount, amounts, steps,
            reduce(min = 999999, s IN steps | CASE WHEN s < min THEN s ELSE min END) AS minStep,
            reduce(max = 0, s IN steps | CASE WHEN s > max THEN s ELSE max END) AS maxStep
            
        // Kiểm tra khoảng thời gian giữa các giao dịch có ngắn không
        WITH src, receiver, txCount, amounts, (maxStep - minStep) AS timeSpan
        WHERE timeSpan <= 10
        
        // Tính tổng tiền và giá trị trung bình
        WITH src, receiver, txCount, timeSpan,
            reduce(s = 0, a IN amounts | s + a) AS totalAmount,
            reduce(s = 0, a IN amounts | s + a) / size(amounts) AS avgAmount
        
        // Đánh giá dấu hiệu structuring
        SET src.structuringScore = CASE
            WHEN txCount >= 5 AND timeSpan <= 3 THEN 0.95  // Nhiều giao dịch trong thời gian rất ngắn
            WHEN txCount >= 4 AND timeSpan <= 5 THEN 0.85  // Nhiều giao dịch trong thời gian ngắn
            WHEN txCount >= 3 AND timeSpan <= 8 THEN 0.7   // Nhiều giao dịch trong thời gian khá ngắn
            ELSE 0.5                                      // Ít dấu hiệu hơn
        END
        """
        self.db_manager.run_query(structuring_query)
        
        # Kết hợp các mẫu phát hiện được
        combine_patterns_query = """
        MATCH (a:Account)
        SET a.patternScore = 
            COALESCE(a.fanPatternScore, 0) * 0.35 +
            COALESCE(a.mulePatternScore, 0) * 0.4 +
            COALESCE(a.structuringScore, 0) * 0.25
        """
        self.db_manager.run_query(combine_patterns_query)
    
    def _analyze_temporal_patterns(self):
        """Phân tích các mẫu thời gian bất thường."""
        print("  - Đang phân tích mẫu thời gian đáng ngờ...")
        
        # Phát hiện thay đổi hành vi đột ngột
        behavior_change_query = """
        // Tìm các tài khoản có lịch sử giao dịch
        MATCH (a:Account)-[tx:SENT]->()
        WITH a, tx
        ORDER BY tx.timestamp  // Sắp xếp các giao dịch theo thời gian
        WITH a, collect(tx) AS transactions
        WHERE size(transactions) >= 6
        
        // Tính toán phân chia giao dịch thành hai nửa
        WITH a, transactions, 
            size(transactions) / 2 AS midPoint
        
        // Tính toán số tiền trung bình cho mỗi nửa
        WITH a, transactions, midPoint,
            [tx IN transactions[0..toInteger(midPoint)] | tx.amount] AS firstHalfAmounts,
            [tx IN transactions[toInteger(midPoint)..] | tx.amount] AS secondHalfAmounts
        
        // Tính toán giá trị trung bình của mỗi nửa
        WITH a, 
            reduce(total = 0.0, amount IN firstHalfAmounts | total + amount) / 
                size(firstHalfAmounts) AS firstHalfAvg,
            reduce(total = 0.0, amount IN secondHalfAmounts | total + amount) / 
                size(secondHalfAmounts) AS secondHalfAvg
        
        // Phát hiện thay đổi mạnh
        WITH a, firstHalfAvg, secondHalfAvg,
            abs(secondHalfAvg - firstHalfAvg) / 
                CASE WHEN firstHalfAvg = 0 THEN 1 ELSE firstHalfAvg END AS changeFactor
        
        // Đánh giá mức độ thay đổi
        SET a.behaviorChangeScore = CASE
            WHEN changeFactor > 5.0 THEN 0.9  // Thay đổi rất lớn
            WHEN changeFactor > 3.0 THEN 0.7  // Thay đổi lớn
            WHEN changeFactor > 2.0 THEN 0.5  // Thay đổi đáng kể
            WHEN changeFactor > 1.0 THEN 0.3  // Thay đổi nhỏ
            ELSE 0.1                         // Không thay đổi nhiều
        END
        """
        self.db_manager.run_query(behavior_change_query)

        # Kết hợp các phân tích thời gian
        combine_temporal_query = """
        MATCH (a:Account)
        SET a.temporalScore = (
            COALESCE(a.abnormalHourScore, 0) * 0.5 +
            COALESCE(a.behaviorChangeScore, 0) * 0.3 +
            COALESCE(a.tempBurst, 0) * 0.2
        )
        """
        self.db_manager.run_query(combine_temporal_query)
    
    def _detect_complex_cycles(self):
        """Phát hiện các chu trình phức tạp đáng ngờ."""
        print("  - Đang phát hiện các chu trình gian lận phức tạp...")
        
        # Phát hiện chu trình độ dài 2 (a->b->a)
        cycle2_query = """
        MATCH path = (a:Account)-[tx1:SENT]->(b:Account)-[tx2:SENT]->(a)
        WHERE id(a) <> id(b)  // Tránh tự vòng
        
        WITH a, b, tx1.amount AS amount1, tx2.amount AS amount2,
            tx1.step AS step1, tx2.step AS step2
        WHERE step1 IS NOT NULL AND step2 IS NOT NULL
        
        // Đánh giá chênh lệch số tiền và thời gian
        WITH a, b, 
            CASE WHEN amount1 > amount2 THEN amount1 - amount2 ELSE amount2 - amount1 END AS amountDiff,
            CASE WHEN step1 > step2 THEN step1 - step2 ELSE step2 - step1 END AS timeSpan,
            CASE WHEN amount1 > amount2 THEN amount1 ELSE amount2 END AS maxAmount
        WHERE timeSpan <= 20 AND (amountDiff / maxAmount) < 0.3
        
        // Tính điểm chu trình
        WITH a, b, timeSpan, amountDiff / maxAmount AS amountRatio
        
        // Cập nhật điểm cho các node trong chu trình
        SET a.cycleScore = CASE
            WHEN timeSpan <= 5 AND amountRatio < 0.1 THEN 0.95
            WHEN timeSpan <= 10 AND amountRatio < 0.2 THEN 0.85
            WHEN timeSpan <= 20 AND amountRatio < 0.3 THEN 0.7
            ELSE 0.5
        END,
        b.cycleScore = CASE
            WHEN timeSpan <= 5 AND amountRatio < 0.1 THEN 0.9
            WHEN timeSpan <= 10 AND amountRatio < 0.2 THEN 0.8
            WHEN timeSpan <= 20 AND amountRatio < 0.3 THEN 0.65
            ELSE 0.45
        END
        
        RETURN count(*) as cycleCount
        """
        
        # Phát hiện chu trình độ dài 3 (a->b->c->a)
        cycle3_query = """
        MATCH path = (a:Account)-[tx1:SENT]->(b:Account)-[tx2:SENT]->(c:Account)-[tx3:SENT]->(a)
        WHERE id(a) <> id(b) AND id(b) <> id(c) AND id(a) <> id(c)
        
        WITH a, b, c, tx1.amount AS amount1, tx2.amount AS amount2, tx3.amount AS amount3,
            tx1.step AS step1, tx2.step AS step2, tx3.step AS step3
        WHERE step1 IS NOT NULL AND step2 IS NOT NULL AND step3 IS NOT NULL
        
        // Đánh giá chênh lệch số tiền và thời gian
        WITH a, b, c, 
            amount1, amount2, amount3,
            step1, step2, step3,
            CASE 
                WHEN amount1 >= amount2 AND amount1 >= amount3 THEN amount1
                WHEN amount2 >= amount1 AND amount2 >= amount3 THEN amount2
                ELSE amount3
            END AS maxAmount,
            CASE 
                WHEN amount1 <= amount2 AND amount1 <= amount3 THEN amount1
                WHEN amount2 <= amount1 AND amount2 <= amount3 THEN amount2
                ELSE amount3
            END AS minAmount,
            CASE 
                WHEN step1 >= step2 AND step1 >= step3 THEN step1
                WHEN step2 >= step1 AND step2 >= step3 THEN step2
                ELSE step3
            END AS maxStep,
            CASE 
                WHEN step1 <= step2 AND step1 <= step3 THEN step1
                WHEN step2 <= step1 AND step2 <= step3 THEN step2
                ELSE step3
            END AS minStep
        
        WITH a, b, c, 
            (maxAmount - minAmount) AS amountDiff,
            (maxStep - minStep) AS timeSpan,
            maxAmount
        WHERE timeSpan <= 20 AND (amountDiff / maxAmount) < 0.3
        
        // Tính điểm chu trình
        WITH a, b, c, timeSpan, amountDiff / maxAmount AS amountRatio
        
        // Cập nhật điểm cho các node trong chu trình
        SET a.cycleScore = CASE
            WHEN timeSpan <= 5 AND amountRatio < 0.1 THEN 0.95
            WHEN timeSpan <= 10 AND amountRatio < 0.2 THEN 0.85
            WHEN timeSpan <= 20 AND amountRatio < 0.3 THEN 0.7
            ELSE 0.5
        END,
        b.cycleScore = CASE
            WHEN timeSpan <= 5 AND amountRatio < 0.1 THEN 0.9
            WHEN timeSpan <= 10 AND amountRatio < 0.2 THEN 0.8
            WHEN timeSpan <= 20 AND amountRatio < 0.3 THEN 0.65
            ELSE 0.45
        END,
        c.cycleScore = CASE
            WHEN timeSpan <= 5 AND amountRatio < 0.1 THEN 0.9
            WHEN timeSpan <= 10 AND amountRatio < 0.2 THEN 0.8
            WHEN timeSpan <= 20 AND amountRatio < 0.3 THEN 0.65
            ELSE 0.45
        END
        
        RETURN count(*) as cycleCount
        """
        
        # Run both queries separately instead of using UNION
        cycle2_result = self.db_manager.run_query(cycle2_query)
        cycle3_result = self.db_manager.run_query(cycle3_query)
        
        # Print results for information
        cycle2_count = cycle2_result.get("cycleCount", 0) if cycle2_result else 0
        cycle3_count = cycle3_result.get("cycleCount", 0) if cycle3_result else 0
        print(f"    • Phát hiện {cycle2_count} chu trình độ dài 2 và {cycle3_count} chu trình độ dài 3")
        
        # Thiết lập giá trị mặc định cho tài khoản không tham gia chu trình
        default_cycle_query = """
        MATCH (a:Account)
        WHERE a.cycleScore IS NULL
        SET a.cycleScore = 0
        RETURN count(*) as defaultedNodes
        """
        default_result = self.db_manager.run_query(default_cycle_query)
        default_count = default_result.get("defaultedNodes", 0) if default_result else 0
        print(f"    • Đặt giá trị mặc định cho {default_count} tài khoản không tham gia chu trình")
    
    def _analyze_money_flow(self):
        """Phân tích dòng tiền đáng ngờ dựa trên mẫu giao dịch."""
        print("  - Đang phân tích dòng tiền đáng ngờ...")
        
        # Phát hiện dòng tiền đáng ngờ
        money_flow_query = """
        // Phân tích cân bằng tiền vào/ra
        MATCH (a:Account)
        OPTIONAL MATCH (a)-[out:SENT]->()
        WITH a, sum(out.amount) AS totalOut
        
        OPTIONAL MATCH (a)<-[:RECEIVED]-()-[:SENT]-(in:Account)
        WITH a, totalOut, sum(in.amount) AS totalIn
        
        // Tính tỷ lệ tiền vào/ra
        WITH a, totalIn, totalOut,
             CASE WHEN totalIn = 0 THEN 999999 
                  ELSE totalOut / totalIn 
             END AS outInRatio,
             CASE WHEN totalOut = 0 AND totalIn > 0 THEN 1
                  WHEN totalOut = 0 AND totalIn = 0 THEN 0
                  ELSE 0
             END AS sinkFlag
        
        // Đánh giá dòng tiền đáng ngờ
        SET a.moneyFlowScore = CASE
            WHEN sinkFlag = 1 AND totalIn > 10000 THEN 0.95  // "Hố đen" lớn (chỉ nhận tiền)
            WHEN outInRatio > 0.98 AND outInRatio < 1.02 AND totalIn > 5000 THEN 0.9  // Cân bằng hoàn hảo với giá trị lớn
            WHEN outInRatio > 0.95 AND outInRatio < 1.05 AND totalIn > 1000 THEN 0.8  // Cân bằng gần hoàn hảo
            WHEN totalIn = 0 AND totalOut > 10000 THEN 0.85  // "Nguồn" lớn (chỉ gửi tiền)
            ELSE 0.0
        END
        """
        self.db_manager.run_query(money_flow_query)
    
    def _detect_suspicious_communities(self):
        """Phát hiện các cộng đồng đáng ngờ trong đồ thị."""
        print("  - Đang phát hiện các cộng đồng đáng ngờ...")
        
        # Phát hiện cộng đồng đáng ngờ
        community_query = """
        // Nhóm tài khoản theo cộng đồng
        MATCH (a:Account)
        WHERE a.communityId IS NOT NULL
        WITH a.communityId AS communityId, collect(a) AS communityAccounts
        
        // Phân tích các giao dịch trong cộng đồng
        UNWIND communityAccounts AS a
        MATCH (a)-[tx:SENT]->()
        WITH communityId, communityAccounts, count(tx) AS internalTxCount
        
        // Phân tích các giao dịch ra ngoài cộng đồng
        UNWIND communityAccounts AS a
        MATCH (a)-[tx:SENT]->()-[:RECEIVED]->(b:Account)
        WHERE NOT b IN communityAccounts
        WITH communityId, communityAccounts, internalTxCount, count(tx) AS externalTxCount
        
        // Tính tỷ lệ giao dịch nội bộ
        WITH communityId, communityAccounts,
            CASE WHEN internalTxCount + externalTxCount = 0 THEN 0
                 ELSE internalTxCount / toFloat(internalTxCount + externalTxCount)
            END AS internalRatio,
            size(communityAccounts) AS communitySize
        
        // Đánh giá cộng đồng đáng ngờ
        WITH communityId, communityAccounts, internalRatio, communitySize,
             CASE
                 WHEN communitySize >= 3 AND communitySize <= 10 AND internalRatio > 0.8 THEN 0.9
                 WHEN communitySize > 10 AND communitySize <= 20 AND internalRatio > 0.7 THEN 0.8
                 WHEN communitySize > 20 AND internalRatio > 0.6 THEN 0.7
                 WHEN internalRatio > 0.9 THEN 0.85
                 ELSE internalRatio * 0.5
             END AS suspiciousScore
        
        // Gán điểm cho từng tài khoản trong cộng đồng
        UNWIND communityAccounts AS a
        SET a.communitySuspiciousScore = suspiciousScore
        """
        self.db_manager.run_query(community_query)
        
        # Thiết lập giá trị mặc định
        default_community_query = """
        MATCH (a:Account)
        WHERE a.communitySuspiciousScore IS NULL
        SET a.communitySuspiciousScore = 0
        """
        self.db_manager.run_query(default_community_query)
        
        # Kết hợp tất cả các điểm đánh ngờ
        final_score_query = """
        MATCH (a:Account)
        SET a.advancedFraudScore = (
            COALESCE(a.combinedScore, 0) * 0.25 +
            COALESCE(a.patternScore, 0) * 0.3 +
            COALESCE(a.temporalScore, 0) * 0.15 +
            COALESCE(a.cycleScore, 0) * 0.15 +
            COALESCE(a.moneyFlowScore, 0) * 0.1 +
            COALESCE(a.communitySuspiciousScore, 0) * 0.05
        )
        """
        self.db_manager.run_query(final_score_query)
        
        # Chuẩn hóa điểm cuối cùng
        normalize_final_query = """
        MATCH (a:Account)
        WITH MIN(a.advancedFraudScore) AS min_score, MAX(a.advancedFraudScore) AS max_score
        MATCH (a:Account)
        SET a.advancedFraudScore = CASE 
            WHEN max_score = min_score THEN 0
            ELSE (a.advancedFraudScore - min_score) / (max_score - min_score)
        END
        """
        self.db_manager.run_query(normalize_final_query)
        
        # Truyền advanced fraud score từ Account đến Transaction
        propagate_score_query = """
        MATCH (a:Account)-[tx:SENT]->()
        SET tx.advancedAnomalyScore = a.advancedFraudScore
        """
        self.db_manager.run_query(propagate_score_query)
