class ApiQueries:
    # Status check query
    HAS_ANALYSIS_QUERY = """
        MATCH (a:Account)
        WHERE a.fraud_score IS NOT NULL
        RETURN count(a) > 0 AS has_analysis
    """
    
    # Fraud stats queries
    FRAUD_BY_TYPE_QUERY = """
        MATCH (sender:Account)-[r:SENT]->(receiver:Account)
        WHERE sender.fraud_score > 0.5 OR receiver.fraud_score > 0.5
        
        WITH CASE
            WHEN r.type IS NULL THEN 'Khác'
            ELSE r.type 
        END AS type,
        COUNT(r) as count,
        SUM(r.amount) as total_amount
        
        RETURN type, count, total_amount
        ORDER BY count DESC
        LIMIT 5
    """
    
    COMMUNITIES_QUERY = """
        MATCH (a:Account)
        WHERE a.community IS NOT NULL AND a.fraud_score IS NOT NULL
        
        WITH a.community AS community, 
             COUNT(a) AS count, 
             AVG(a.fraud_score) AS avg_score,
             COUNT(CASE WHEN a.fraud_score > $fraud_threshold THEN 1 END) AS high_risk_count
        
        WHERE count >= 3 AND high_risk_count > 0
        
        RETURN 
            community, 
            count, 
            avg_score,
            high_risk_count,
            1.0 * high_risk_count / count AS risk_ratio
        ORDER BY risk_ratio DESC, count DESC
        LIMIT 10
    """
    
    COMMUNITY_DISTRIBUTION_QUERY = """
        MATCH (a:Account)
        WHERE a.community IS NOT NULL
        
        WITH a.community AS community, COUNT(a) AS size
        
        RETURN 
            CASE 
                WHEN size = 1 THEN 'Single'
                WHEN size >= 2 AND size <= 5 THEN 'Small'
                WHEN size > 5 AND size <= 20 THEN 'Medium'
                WHEN size > 20 AND size <= 100 THEN 'Large'
                ELSE 'Very Large'
            END AS size_category,
            COUNT(DISTINCT community) AS community_count
        ORDER BY 
            CASE size_category
                WHEN 'Single' THEN 1
                WHEN 'Small' THEN 2
                WHEN 'Medium' THEN 3
                WHEN 'Large' THEN 4
                ELSE 5
            END
    """
    
    SCORE_DISTRIBUTION_QUERY = """
        MATCH (a:Account)
        WHERE a.fraud_score IS NOT NULL
        RETURN 
            COUNT(CASE WHEN a.fraud_score > 0.85 THEN 1 END) AS very_high,
            COUNT(CASE WHEN a.fraud_score > 0.75 AND a.fraud_score <= 0.85 THEN 1 END) AS high,
            COUNT(CASE WHEN a.fraud_score > 0.65 AND a.fraud_score <= 0.75 THEN 1 END) AS medium,
            COUNT(CASE WHEN a.fraud_score > 0.45 AND a.fraud_score <= 0.65 THEN 1 END) AS low,
            COUNT(CASE WHEN a.fraud_score > 0.25 AND a.fraud_score <= 0.45 THEN 1 END) AS very_low,
            COUNT(CASE WHEN a.fraud_score <= 0.25 THEN 1 END) AS negligible,
            AVG(a.fraud_score) AS avg_score
    """
    
    TRANSACTION_CYCLES_QUERY = """
        MATCH path = (a:Account)-[:SENT*2..4]->(a)
        
        WITH SIZE(nodes(path)) AS cycle_length,
             COUNT(DISTINCT a) AS unique_accounts,
             COUNT(path) AS cycle_count
        
        RETURN 
            cycle_length,
            cycle_count,
            unique_accounts
        ORDER BY cycle_length
    """
      # Metrics queries
    BASIC_METRICS_QUERY = """
        MATCH (a:Account)
        OPTIONAL MATCH (a)-[r1:SENT]->()
        OPTIONAL MATCH ()-[r2:SENT]->(a)
        WITH a, r1, r2
        WITH 
            a,
            COLLECT(r1) + COLLECT(r2) AS all_txs
        
        RETURN count(DISTINCT a) AS total_accounts,
               count(DISTINCT all_txs) AS total_transactions,
               count(DISTINCT CASE WHEN a.fraud_score > $fraud_threshold THEN a END) AS detected_fraud_accounts,
               count(DISTINCT CASE WHEN SIZE(all_txs) > 0 AND a.fraud_score > $fraud_threshold THEN all_txs END) AS detected_fraud_transactions
    """
    
    FRAUD_LEVELS_QUERY = """
        MATCH (a:Account)
        WHERE a.fraud_score IS NOT NULL
        RETURN 
            count(CASE WHEN a.fraud_score > 0.75 THEN 1 END) AS very_high_risk,
            count(CASE WHEN a.fraud_score > $fraud_threshold AND a.fraud_score <= 0.75 THEN 1 END) AS high_risk,
            count(CASE WHEN a.fraud_score > $suspicious_threshold AND a.fraud_score <= $fraud_threshold THEN 1 END) AS medium_risk,
            count(CASE WHEN a.fraud_score > 0.3 AND a.fraud_score <= $suspicious_threshold THEN 1 END) AS low_risk,
            count(CASE WHEN a.fraud_score <= 0.3 THEN 1 END) AS very_low_risk
    """
    
    COMMUNITY_METRICS_QUERY = """
        MATCH (a:Account)
        WHERE a.community IS NOT NULL
        WITH a.community AS community, COUNT(a) AS node_count
        WHERE node_count >= 2  // Only count communities with at least 2 accounts
        
        MATCH (m:Account)
        WHERE m.community = community
        
        WITH community, 
             COUNT(m) AS community_size,
             AVG(m.fraud_score) AS avg_score,
             COUNT(CASE WHEN m.fraud_score > $fraud_threshold THEN 1 END) AS high_risk_nodes
          RETURN count(DISTINCT community) AS count,
               count(CASE WHEN avg_score > 0.5 OR high_risk_nodes > 0 THEN 1 END) AS high_risk_communities
    """
    
    CYCLES_QUERY = """
        MATCH path = (a:Account)-[:SENT*2..4]->(a)
        RETURN count(DISTINCT a) AS accounts_in_cycles
    """
    
    VALIDATION_METRICS_QUERY = """
        MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
        RETURN 
            count(DISTINCT tx) AS total_transactions,
            count(DISTINCT CASE WHEN sender.fraud_score > $fraud_threshold OR receiver.fraud_score > $fraud_threshold THEN tx END) AS detected_fraud_transactions,
            count(DISTINCT CASE WHEN tx.is_fraud = 1 THEN tx END) AS ground_truth_frauds,
            count(DISTINCT CASE WHEN tx.is_fraud = 1 AND (sender.fraud_score > $fraud_threshold OR receiver.fraud_score > $fraud_threshold) THEN tx END) AS true_positives
    """
    
    # Suspicious accounts query
    SUSPICIOUS_ACCOUNTS_QUERY = """
        MATCH (a:Account)
        WHERE a.fraud_score > $suspicious_threshold
        
        OPTIONAL MATCH (a)-[out:SENT]->()
        WITH a, count(out) AS out_count, sum(out.amount) AS out_amount
        
        OPTIONAL MATCH ()-[in:SENT]->(a)
        WITH a, out_count, out_amount, count(in) AS in_count, sum(in.amount) AS in_amount
        
        OPTIONAL MATCH path = (a)-[:SENT*2..3]->(a)
        WITH a, out_count, out_amount, in_count, in_amount, 
             count(path) > 0 AS has_cycle
        
        RETURN 
            a.id AS id, 
            a.fraud_score AS score,
            a.community AS community,
            COALESCE(a.pagerank_score, 0) AS pagerank,
            COALESCE(a.degree_score, 0) AS degree,
            COALESCE(a.similarity_score, 0) AS similarity,
            COALESCE(a.tx_imbalance, 0) AS imbalance,
            COALESCE(a.base_score, 0) AS base_score,
            COALESCE(a.relation_boost, 0) AS relation_boost,
            COALESCE(a.cycle_boost, 0) AS cycle_boost,
            CASE WHEN has_cycle THEN 1 ELSE 0 END AS in_cycle,
            out_count AS sent_count,
            in_count AS received_count,
            out_amount AS sent_amount,
            in_amount AS received_amount,
            abs(out_amount - in_amount) AS imbalance_amount
        ORDER BY score DESC
        LIMIT 15
    """
    
    # Network queries
    REL_COUNT_QUERY = """
        MATCH ()-[r]->()
        RETURN count(r) as count
    """
    
    TOP_NODES_QUERY = """
        MATCH (a:Account)
        WHERE a.fraud_score > $suspicious_threshold
        WITH a ORDER BY a.fraud_score DESC LIMIT 10
        
        OPTIONAL MATCH (a)-[r:SENT]-(other:Account)
        WHERE a <> other
        WITH a, other, r
        WHERE other IS NOT NULL
        
        WITH COLLECT(DISTINCT a) + COLLECT(DISTINCT other) AS allNodes,
             COLLECT(DISTINCT r) AS allRels
        
        WITH 
            [n IN allNodes WHERE n.fraud_score > $suspicious_threshold] AS central_nodes,
            [n IN allNodes WHERE n.fraud_score <= $suspicious_threshold OR n.fraud_score IS NULL] AS connected_nodes,
            allRels AS relationships
        
        RETURN central_nodes, connected_nodes, relationships
    """
    
    # Debug metrics query
    DEBUG_METRICS_QUERY = """
        MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
        
        RETURN count(DISTINCT sender) + count(DISTINCT receiver) AS total_accounts,
               count(DISTINCT tx) AS total_transactions,
               count(DISTINCT CASE WHEN sender.fraud_score > 0.7 THEN sender END) + 
                  count(DISTINCT CASE WHEN receiver.fraud_score > 0.7 THEN receiver END) AS fraud_accounts_07,
               count(DISTINCT CASE WHEN sender.fraud_score > 0.6 THEN sender END) + 
                  count(DISTINCT CASE WHEN receiver.fraud_score > 0.6 THEN receiver END) AS fraud_accounts_06,
               count(DISTINCT CASE WHEN sender.fraud_score > 0.5 THEN sender END) + 
                  count(DISTINCT CASE WHEN receiver.fraud_score > 0.5 THEN receiver END) AS fraud_accounts_05,
               
               count(DISTINCT CASE WHEN sender.fraud_score > 0.7 OR receiver.fraud_score > 0.7 THEN tx END) AS fraud_transactions_07,
               count(DISTINCT CASE WHEN sender.fraud_score > 0.6 OR receiver.fraud_score > 0.6 THEN tx END) AS fraud_transactions_06,
               count(DISTINCT CASE WHEN sender.fraud_score > 0.5 OR receiver.fraud_score > 0.5 THEN tx END) AS fraud_transactions_05,
               
               // Count of actual fraud transactions from ground truth (for evaluation only)
               count(DISTINCT CASE WHEN tx.is_fraud = 1 THEN tx END) AS real_fraud_transactions,
               
               // True positives at different thresholds (using is_fraud for evaluation only)
               count(DISTINCT CASE WHEN tx.is_fraud = 1 AND (sender.fraud_score > 0.7 OR receiver.fraud_score > 0.7) THEN tx END) AS true_positives_07,
               count(DISTINCT CASE WHEN tx.is_fraud = 1 AND (sender.fraud_score > 0.6 OR receiver.fraud_score > 0.6) THEN tx END) AS true_positives_06,
               count(DISTINCT CASE WHEN tx.is_fraud = 1 AND (sender.fraud_score > 0.5 OR receiver.fraud_score > 0.5) THEN tx END) AS true_positives_05,
               
               // Metrics for new features in optimized detector
               count(DISTINCT CASE WHEN sender.model1_score > 0.6 OR receiver.model1_score > 0.6 THEN tx END) AS model1_transactions,
               count(DISTINCT CASE WHEN sender.model2_score > 0.6 OR receiver.model2_score > 0.6 THEN tx END) AS model2_transactions,
               count(DISTINCT CASE WHEN sender.model3_score > 0.6 OR receiver.model3_score > 0.6 THEN tx END) AS model3_transactions,
               count(DISTINCT CASE WHEN sender.high_confidence_pattern = true OR receiver.high_confidence_pattern = true THEN tx END) AS high_confidence_transactions,
               count(DISTINCT CASE WHEN sender.funnel_disperse_pattern = true OR receiver.funnel_disperse_pattern = true THEN tx END) AS funnel_disperse_transactions,
               count(DISTINCT CASE WHEN sender.round_tx_pattern = true OR receiver.round_tx_pattern = true THEN tx END) AS round_tx_transactions,
               count(DISTINCT CASE WHEN sender.increasing_chain = true OR receiver.increasing_chain = true THEN tx END) AS chain_transactions,
               count(DISTINCT CASE WHEN sender.similar_to_fraud = true OR receiver.similar_to_fraud = true THEN tx END) AS similarity_transactions,
               count(DISTINCT CASE WHEN sender.high_velocity = true OR receiver.high_velocity = true THEN tx END) AS velocity_transactions
    """
    
    # Community details queries
    COMMUNITY_OVERVIEW_QUERY = """
        MATCH (a:Account)
        WHERE a.community = $community_id
        
        WITH count(a) AS total_accounts,
             avg(a.fraud_score) AS avg_score,
             count(CASE WHEN a.fraud_score > $fraud_threshold THEN 1 END) AS high_risk_accounts
        
        RETURN 
            total_accounts,
            avg_score,
            high_risk_accounts,
            1.0 * high_risk_accounts / total_accounts AS risk_ratio
    """
    
    COMMUNITY_ACCOUNTS_QUERY = """
        MATCH (a:Account)
        WHERE a.community = $community_id
        
        OPTIONAL MATCH (a)-[out:SENT]->()
        WITH a, count(out) AS out_count, sum(out.amount) AS out_amount
        
        OPTIONAL MATCH ()-[in:SENT]->(a)
        WITH a, out_count, out_amount, count(in) AS in_count, sum(in.amount) AS in_amount
        
        OPTIONAL MATCH path = (a)-[:SENT*2..3]->(a)
        WITH a, out_count, out_amount, in_count, in_amount, 
             count(path) > 0 AS has_cycle
        
        RETURN 
            a.id AS id, 
            a.fraud_score AS score,
            COALESCE(a.pagerank_score, 0) AS pagerank,
            COALESCE(a.tx_imbalance, 0) AS imbalance,
            CASE WHEN has_cycle THEN 1 ELSE 0 END AS in_cycle,
            out_count AS sent_count,
            in_count AS received_count,
            out_amount AS sent_amount,
            in_amount AS received_amount,
            abs(out_amount - in_amount) AS imbalance_amount
        ORDER BY score DESC
    """
    
    COMMUNITY_TRANSACTIONS_QUERY = """
        MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
        WHERE sender.community = $community_id AND receiver.community = $community_id
        
        RETURN 
            sender.id AS source,
            receiver.id AS target,
            tx.amount AS amount,
            CASE WHEN sender.fraud_score > 0.7 OR receiver.fraud_score > 0.7 THEN 1 ELSE 0 END AS is_fraud,
            COALESCE(tx.type, 'TRANSFER') AS type,
            sender.fraud_score > $fraud_threshold OR receiver.fraud_score > $fraud_threshold AS high_risk
        ORDER BY amount DESC
        LIMIT 100  // Giới hạn số giao dịch để tránh quá tải
    """
    
    RELATED_COMMUNITIES_QUERY = """
        MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
        WHERE 
            (sender.community = $community_id AND receiver.community <> $community_id)
            OR (sender.community <> $community_id AND receiver.community = $community_id)
            
        WITH 
            CASE WHEN sender.community = $community_id THEN receiver.community ELSE sender.community END AS related_community,
            count(tx) AS transaction_count,
            sum(tx.amount) AS total_amount
        
        MATCH (a:Account)
        WHERE a.community = related_community
        
        WITH related_community, transaction_count, total_amount, count(a) AS account_count, avg(a.fraud_score) AS avg_score
        
        RETURN 
            related_community AS id,
            transaction_count,
            total_amount,
            account_count,
            avg_score
        ORDER BY transaction_count DESC
        LIMIT 5
    """
    
    # All communities query
    ALL_COMMUNITIES_QUERY = """
        MATCH (a:Account)
        WHERE a.community IS NOT NULL
        
        WITH a.community AS community_id, COUNT(a) AS account_count, AVG(a.fraud_score) AS avg_score
        WHERE account_count >= 2  // Chỉ hiển thị cộng đồng có ít nhất 2 tài khoản
        
        WITH 
            community_id, 
            account_count, 
            avg_score,
            count(CASE WHEN avg_score > $fraud_threshold THEN 1 END) AS high_risk,
            1.0 * count(CASE WHEN avg_score > $fraud_threshold THEN 1 END) / account_count AS risk_ratio
        
        RETURN 
            community_id AS id,
            account_count AS size,
            avg_score,
            CASE
                WHEN account_count <= 3 THEN 'small'
                WHEN account_count <= 10 THEN 'medium'
                ELSE 'large'
            END AS size_category,
            CASE
                WHEN avg_score > 0.7 THEN 'high'
                WHEN avg_score > 0.5 THEN 'medium'
                ELSE 'low'
            END AS risk_level,
            risk_ratio
        ORDER BY risk_ratio DESC, account_count DESC
        LIMIT 200
    """
    
    # Fraud transactions query
    FRAUD_TRANSACTIONS_QUERY = """
        MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
        WHERE (sender.fraud_score > $fraud_threshold OR receiver.fraud_score > $fraud_threshold)
        AND (CASE 
            WHEN $type = 'all' THEN true
            WHEN tx.type IS NULL AND $type = 'other' THEN true
            ELSE tx.type = $type 
        END)
        
        RETURN 
            sender.id AS source,
            sender.fraud_score AS source_score,
            receiver.id AS target,
            receiver.fraud_score AS target_score,
            tx.amount AS amount,
            tx.timestamp AS timestamp,
            COALESCE(tx.type, 'Khác') AS type,
            CASE 
                WHEN sender.fraud_score > 0.7 OR receiver.fraud_score > 0.7 THEN true
                ELSE false
            END AS is_fraud,
            sender.community AS source_community,
            receiver.community AS target_community
        ORDER BY tx.amount DESC
        LIMIT 100
    """

    GET_BASIC_STATS = """
    // Lấy thống kê cơ bản về database - chia thành hai phần riêng biệt
    MATCH (a:Account) 
    RETURN count(a) as account_count
    """
    GET_TRANSACTION_COUNT = """
    // Đếm tổng số giao dịch - tách riêng để tránh timeout
    MATCH ()-[t:SENT]->()
    RETURN count(t) as transaction_count
    """
    
    # Sửa query lấy ground truth    
    GET_GROUND_TRUTH = """
    // Đếm giao dịch gian lận thực sự (nới lỏng điều kiện)
    MATCH (a:Account)-[t:SENT]->()
    WHERE a.is_fraud = true OR a.fraud = true OR a.known_fraud = true
    RETURN count(DISTINCT t) as real_fraud_count
    """
      # Sửa query lấy giao dịch theo ngưỡng
    GET_TRANSACTIONS_BY_THRESHOLD = """
    // Đếm giao dịch từ hoặc đến các tài khoản vượt ngưỡng
    MATCH (src:Account)-[t:SENT]->(tgt:Account)
    RETURN 
        count(DISTINCT CASE WHEN src.fraud_score >= 0.5 OR tgt.fraud_score >= 0.5 THEN t END) as transactions_05,
        count(DISTINCT CASE WHEN src.fraud_score >= 0.6 OR tgt.fraud_score >= 0.6 THEN t END) as transactions_06,
        count(DISTINCT CASE WHEN src.fraud_score >= 0.7 OR tgt.fraud_score >= 0.7 THEN t END) as transactions_07
    """
      # Sửa query lấy số liệu từ mô hình
    GET_MODEL_METRICS = """
    // Lấy số liệu từ các mô hình - sửa property names
    MATCH (src:Account)-[t:SENT]->(tgt:Account)
    RETURN 
        count(DISTINCT CASE WHEN src.model1_score > 0.5 OR tgt.model1_score > 0.5 THEN t END) as model1_txs,
        count(DISTINCT CASE WHEN src.model2_score > 0.5 OR tgt.model2_score > 0.5 THEN t END) as model2_txs,
        count(DISTINCT CASE WHEN src.model3_score > 0.5 OR tgt.model3_score > 0.5 THEN t END) as model3_txs,
        count(DISTINCT CASE WHEN src.high_confidence_pattern = true OR tgt.high_confidence_pattern = true THEN t END) as high_confidence_txs,
        count(DISTINCT CASE WHEN src.funnel_pattern = true OR tgt.funnel_pattern = true THEN t END) as funnel_txs,
        count(DISTINCT CASE WHEN src.round_pattern = true OR tgt.round_pattern = true THEN t END) as round_txs,
        count(DISTINCT CASE WHEN src.chain_pattern = true OR tgt.chain_pattern = true THEN t END) as chain_txs,
        count(DISTINCT CASE WHEN src.similar_to_fraud = true OR tgt.similar_to_fraud = true THEN t END) as similar_txs,
        count(DISTINCT CASE WHEN src.high_velocity = true OR tgt.high_velocity = true THEN t END) as velocity_txs
    """