class PatternQueries:
    # Temporal pattern queries
    BURST_PATTERN_QUERY = """
        MATCH (a:Account)-[tx:SENT]->(b:Account)
        WITH a, tx.step AS step, COUNT(tx) AS tx_count,
             SUM(tx.amount) AS total_amount,
             COLLECT(tx.amount) AS amounts
        WHERE tx_count >= 3  // Ít nhất 3 giao dịch trong cùng step
        
        WITH a,
             COUNT(step) AS burst_steps,
             AVG(tx_count) AS avg_tx_per_step,
             AVG(total_amount) AS avg_amount_per_step
        WHERE burst_steps >= 2  // Có ít nhất 2 step có burst pattern
        
        SET a.burst_pattern = true,
            a.burst_steps = burst_steps,
            a.avg_tx_per_burst = avg_tx_per_step,
            a.avg_amount_per_burst = avg_amount_per_step
        RETURN COUNT(DISTINCT a) AS accounts_with_bursts
    """

    TRANSACTION_CYCLE_QUERY = """
        MATCH (a:Account)-[tx:SENT]->()
        WITH a, tx.step AS step
        ORDER BY step
        WITH a, COLLECT(step) AS steps
        WHERE SIZE(steps) >= 3
        
        WITH a, steps,
             REDUCE(diffs = [], i IN RANGE(0, SIZE(steps)-2) |
                diffs + [steps[i+1] - steps[i]]
             ) AS step_differences
        
        WITH a, 
             AVG(step_differences) AS avg_interval,                 
             SQRT(REDUCE(variance = 0.0, diff IN step_differences |
                variance + (toFloat(diff) - AVG(step_differences)) ^ 2
             ) / SIZE(step_differences)) AS interval_stddev
        
        SET a.tx_interval = avg_interval,
            a.interval_regularity = 
                CASE 
                    WHEN interval_stddev = 0 THEN 1.0  // Hoàn toàn đều đặn
                    WHEN interval_stddev < 2 THEN 0.8  // Khá đều đặn
                    WHEN interval_stddev < 5 THEN 0.5  // Hơi đều đặn
                    ELSE 0.2                           // Không đều đặn
                END
        RETURN COUNT(a) AS accounts_analyzed
    """

    AMOUNT_CHAIN_QUERY = """
        MATCH p=(a:Account)-[tx1:SENT]->(b:Account)-[tx2:SENT]->(c:Account)
        WHERE tx2.step > tx1.step  // Giao dịch theo thứ tự thời gian
              AND tx2.step - tx1.step <= 24  // Trong vòng 24 giờ
        WITH a, b, c, tx1, tx2,
             CASE 
                WHEN tx2.amount > tx1.amount * 1.2 THEN 'INCREASE'
                WHEN tx2.amount < tx1.amount * 0.8 THEN 'DECREASE'
                ELSE 'STABLE'
             END AS amount_pattern
        
        WHERE amount_pattern = 'INCREASE'  // Tập trung vào chuỗi tăng dần
        
        WITH DISTINCT a, COUNT(b) AS chain_count
        WHERE chain_count >= 2  // Có ít nhất 2 chuỗi tăng dần
        
        SET a.increasing_chains = true,
            a.chain_count = chain_count
        RETURN COUNT(a) AS accounts_with_chains
    """

    HOUR_DISTRIBUTION_QUERY = """
        MATCH (a:Account)-[tx:SENT]->()
        WITH a, tx.step % 24 AS hour, COUNT(tx) AS tx_count
        
        WITH a,
             COLLECT({hour: hour, count: tx_count}) AS hourly_dist,
             MAX(tx_count) AS max_hour_count
        
        SET a.peak_hour_count = max_hour_count,
            a.hour_concentration = 
                CASE 
                    WHEN SIZE(hourly_dist) <= 4 THEN 0.9  // Rất tập trung
                    WHEN SIZE(hourly_dist) <= 8 THEN 0.7  // Khá tập trung
                    WHEN SIZE(hourly_dist) <= 12 THEN 0.5 // Trung bình
                    ELSE 0.3                              // Phân tán
                END
        RETURN COUNT(a) AS accounts_analyzed
    """

    UPDATE_TEMPORAL_RISK_QUERY = """
        MATCH (a:Account)
        WHERE a.burst_pattern = true OR 
              a.interval_regularity IS NOT NULL OR
              a.increasing_chains = true OR
              a.hour_concentration IS NOT NULL
        
        SET a.temporal_risk_score = 
            CASE
                // Combine multiple temporal patterns
                WHEN a.burst_pattern = true AND a.increasing_chains = true
                    THEN 0.9  // Very suspicious
                
                // Individual pattern scoring
                WHEN a.burst_pattern = true AND a.burst_steps >= 3
                    THEN 0.8  // Multiple burst periods
                WHEN a.interval_regularity >= 0.8 AND a.hour_concentration >= 0.7
                    THEN 0.75 // Very regular and concentrated
                WHEN a.increasing_chains = true AND a.chain_count >= 3
                    THEN 0.7  // Multiple increasing chains
                
                // Moderate risk patterns
                WHEN a.burst_pattern = true OR a.increasing_chains = true
                    THEN 0.6
                WHEN a.interval_regularity >= 0.8 OR a.hour_concentration >= 0.7
                    THEN 0.5
                
                // Low risk or normal patterns
                ELSE 0.3
            END
    """

    # Fixed Model scoring queries
    MODEL1_NETWORK_QUERY = """
        MATCH (a:Account)
        WITH a, 
            COALESCE(a.pagerank_score, 0) AS pagerank,
            COALESCE(a.degree_score, 0) AS degree,
            COALESCE(a.similarity_score, 0) AS similarity,
            COALESCE(a.tx_imbalance, 0) AS imbalance,
            COALESCE(a.amount_imbalance, 0) AS amount_imbalance,
            COALESCE(a.temporal_density, 0) AS temporal_density,
            COALESCE(a.step_variance, 0) AS step_variance,
            COALESCE(a.has_burst_pattern, false) AS has_burst,
            COALESCE(a.burst_count, 0) AS burst_count
        
        // First normalize pagerank, which tends to be very small or very large
        WITH a, 
            // Apply sigmoid-like transformation for unbounded metrics
            CASE 
                WHEN pagerank IS NULL THEN 0
                WHEN pagerank <= 0 THEN 0
                ELSE 1 / (1 + exp(-10 * (pagerank - 0.5)))  // Sigmoid transformation
            END AS norm_pagerank,
            
            // Normalize degree - significantly boost for higher degrees
            CASE
                WHEN degree IS NULL THEN 0
                WHEN degree <= 0 THEN 0
                // Square root transformation gives more weight to lower values
                ELSE sqrt(degree / 10)  
            END AS norm_degree,
            
            similarity, imbalance, amount_imbalance, temporal_density, 
            step_variance, has_burst, burst_count
        
        // Apply bounds to normalized values
        WITH a, 
            // Cap normalized values at 1.0
            CASE WHEN norm_pagerank > 1.0 THEN 1.0 ELSE norm_pagerank END AS pagerank_component,
            CASE WHEN norm_degree > 1.0 THEN 1.0 ELSE norm_degree END AS degree_component,
            CASE WHEN similarity > 1.0 THEN 1.0 ELSE similarity END AS similarity_component,
            CASE WHEN imbalance > 1.0 THEN 1.0 ELSE imbalance END AS imbalance_component,
            CASE WHEN amount_imbalance > 1.0 THEN 1.0 ELSE amount_imbalance END AS amount_imbalance_component,
            
            // Temporal features
            CASE WHEN temporal_density > 3.0 THEN 1.0 ELSE temporal_density / 3.0 END AS temporal_density_component,
            CASE WHEN step_variance < 1.0 THEN 1.0 - step_variance ELSE 0.0 END AS step_variance_component,
            has_burst, burst_count
        
        // Calculate network structure score - emphasize imbalance which is a strong fraud signal
        WITH a,
            pagerank_component, degree_component, similarity_component,
            imbalance_component, amount_imbalance_component,
            temporal_density_component, step_variance_component,
            has_burst, burst_count,
            
            // Network structure component (40%)
            (pagerank_component * 0.3 + 
            degree_component * 0.4 + 
            similarity_component * 0.3) * 0.4 AS network_component,
            
            // Transaction imbalance component (40%)
            (imbalance_component * 0.6 + 
            amount_imbalance_component * 0.4) * 0.4 AS transaction_component,
            
            // Temporal component (20%)
            (CASE WHEN has_burst THEN 0.4 ELSE 0.0 END + 
            temporal_density_component * 0.3 + 
            step_variance_component * 0.3) * 0.2 AS temporal_component
        
        // Final model1 score with additional boost for fraud indicators
        WITH a, 
            network_component, transaction_component, temporal_component,
            pagerank_component, degree_component, similarity_component,
            imbalance_component, amount_imbalance_component,
            temporal_density_component, step_variance_component,
            has_burst, burst_count,
            
            network_component + transaction_component + temporal_component AS base_score,
            
            // Apply additional fraud indicators boost
            CASE 
                WHEN imbalance_component > 0.7 AND temporal_density_component > 0.5 THEN 0.2
                WHEN imbalance_component > 0.8 THEN 0.15
                WHEN has_burst AND step_variance_component > 0.7 THEN 0.1
                ELSE 0.0
            END AS fraud_boost
        
        // Set final score with fraud boost, ensuring 0-1 bounds
        SET a.model1_score = 
            CASE
                WHEN base_score + fraud_boost > 1.0 THEN 1.0
                ELSE base_score + fraud_boost
            END,
            a.model1_network_component = network_component,
            a.model1_transaction_component = transaction_component,
            a.model1_temporal_component = temporal_component
                
        RETURN COUNT(a) AS processed_count
    """

    MODEL2_BEHAVIOR_QUERY = """
        MATCH (a:Account)
        // Get outgoing transactions
        OPTIONAL MATCH (a)-[out_tx:SENT]->(out_receiver:Account)
        WITH a, collect(out_tx) as out_txs
        
        // Get incoming transactions
        OPTIONAL MATCH (in_sender:Account)-[in_tx:SENT]->(a)
        WITH a, out_txs, collect(in_tx) as in_txs
        
        // Calculate transaction metrics
        WITH a, out_txs, in_txs,
            size(out_txs) as out_count,
            size(in_txs) as in_count,
            CASE WHEN size(out_txs) > 0 THEN 
                REDUCE(s = 0, t IN out_txs | s + t.amount) 
                ELSE 0 END as out_amount,
            CASE WHEN size(in_txs) > 0 THEN 
                REDUCE(s = 0, t IN in_txs | s + t.amount) 
                ELSE 0 END as in_amount
        
        // Calculate transaction imbalance with EXTREME selectivity
        WITH a, out_txs, in_txs, out_count, in_count, out_amount, in_amount,
            // Transaction imbalance - MUCH more selective
            CASE 
                // Only consider accounts with significant transaction volume
                WHEN in_count + out_count < 5 THEN 0.0
                
                // Extremely strict one-way flow conditions
                WHEN in_count = 0 AND out_count > 10 THEN 1.0  // Major one-way out
                WHEN out_count = 0 AND in_count > 10 THEN 1.0  // Major one-way in
                
                // Significant imbalance in count
                WHEN in_count + out_count >= 5 AND abs(in_count - out_count) / (in_count + out_count) > 0.8 THEN
                    abs(in_count - out_count) / (in_count + out_count)
                ELSE 0.3  // Default lower value
            END as tx_count_imbalance,
            
            // Amount imbalance - MUCH more selective
            CASE 
                // Only consider accounts with significant amounts
                WHEN in_amount + out_amount < 10000 THEN 0.0
                
                // Extreme one-way flow with significant amounts
                WHEN in_amount = 0 AND out_amount > 50000 THEN 1.0
                WHEN out_amount = 0 AND in_amount > 50000 THEN 1.0
                
                // Significant amount imbalance
                WHEN in_amount + out_amount >= 10000 AND abs(in_amount - out_amount) / (in_amount + out_amount) > 0.9 THEN
                    abs(in_amount - out_amount) / (in_amount + out_amount)
                ELSE 0.3
            END as amount_imbalance,
            
            // High value transactions - much higher threshold
            [t IN out_txs WHERE t.amount > 20000] as high_value_txs,
            
            // Round amount transactions - stronger criteria
            [t IN out_txs WHERE round(t.amount) = t.amount AND t.amount >= 10000 
                            AND t.amount % 5000 = 0] as round_txs,
            
            // Burst patterns - more selective
            CASE WHEN size(out_txs) >= 10 THEN
                SIZE(REDUCE(steps = [], t IN out_txs | 
                    CASE WHEN NOT t.step IN steps THEN steps + [t.step] ELSE steps END
                )) < size(out_txs) * 0.5  // Require even more concentrated (< 50% unique)
            ELSE false END as has_bursts
        
        // Calculate behavioral risk factors with much stricter criteria
        WITH a, tx_count_imbalance, amount_imbalance, out_amount, in_amount,
            size(high_value_txs) >= 3 as has_high_value,  // Must have 3+ high-value txs
            size(round_txs) >= 3 as has_round_amounts,    // Must have 3+ round amount txs
            has_bursts,
            out_count, in_count
            
        // CRITICAL: Make tx_anomaly definition extremely selective
        WITH a, tx_count_imbalance, amount_imbalance, has_high_value, has_round_amounts, has_bursts, 
            out_count, in_count, out_amount, in_amount,
            (tx_count_imbalance > 0.95 AND amount_imbalance > 0.95) OR  // Both imbalances must be extreme
            (tx_count_imbalance > 0.95 AND out_count + in_count >= 20) OR // Extreme imbalance with high volume
            (amount_imbalance > 0.95 AND out_amount + in_amount >= 100000) // Extreme amount imbalance with high value
            as is_anomaly
        
        // Calculate model2 score with much more selective criteria
        WITH a, tx_count_imbalance, amount_imbalance, has_high_value, has_round_amounts, has_bursts, 
            out_count, in_count, is_anomaly,
            // Transaction imbalance component (40%) - MUCH stricter
            CASE
                WHEN tx_count_imbalance > 0.95 AND amount_imbalance > 0.95 AND (out_count + in_count >= 10) THEN 0.95
                WHEN tx_count_imbalance > 0.95 OR amount_imbalance > 0.95 THEN 0.80
                WHEN tx_count_imbalance > 0.9 AND amount_imbalance > 0.9 THEN 0.75
                WHEN tx_count_imbalance > 0.9 OR amount_imbalance > 0.9 THEN 0.60
                WHEN tx_count_imbalance > 0.8 AND amount_imbalance > 0.8 THEN 0.50
                ELSE 0.0  // Default to zero for normal patterns
            END * 0.4 as imbalance_component,
            
            // Transaction pattern component (35%) - require stronger combinations
            CASE
                WHEN has_high_value AND has_round_amounts AND has_bursts THEN 0.95
                WHEN has_high_value AND has_round_amounts THEN 0.70
                WHEN has_high_value AND has_bursts THEN 0.60
                WHEN has_round_amounts AND has_bursts THEN 0.55
                WHEN has_high_value THEN 0.25
                WHEN has_round_amounts THEN 0.20
                WHEN has_bursts THEN 0.15
                ELSE 0.0
            END * 0.35 as pattern_component,
            
            // Volume component (25%) - much stricter
            CASE
                WHEN out_count > 30 OR in_count > 30 THEN 1.0
                WHEN out_count > 20 OR in_count > 20 THEN 0.8
                WHEN out_count > 10 OR in_count > 10 THEN 0.5
                ELSE 0.0
            END * 0.25 as volume_component
        
        // Calculate final model2 score
        WITH a, imbalance_component, pattern_component, volume_component,
            has_high_value, has_round_amounts, has_bursts,
            tx_count_imbalance, amount_imbalance, is_anomaly,
            imbalance_component + pattern_component + volume_component as raw_score
        
        // Set model2 score and pattern flags - EXTREMELY SELECTIVE on tx_anomaly
        SET a.model2_score = 
            CASE
                WHEN raw_score > 1.0 THEN 1.0
                ELSE raw_score
            END,
            a.high_value_tx = has_high_value,
            a.tx_anomaly = is_anomaly,  // Using the new highly selective definition
            a.round_pattern = has_round_amounts,
            a.burst_pattern = has_bursts
        
        RETURN COUNT(a) AS processed_count,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count,
            SUM(CASE WHEN raw_score > 0.6 THEN 1 ELSE 0 END) as high_risk_count,
            AVG(CASE WHEN raw_score > 1.0 THEN 1.0 ELSE raw_score END) as avg_score
    """

    MODEL3_INTEGRATION_QUERY = """
        MATCH (a:Account)
        WITH a,
            COALESCE(a.model1_score, 0) AS network_score,
            COALESCE(a.model2_score, 0) AS behavior_score,
            COALESCE(a.temporal_risk_score, 0) AS temporal_risk,
            COALESCE(a.temporal_density, 0) AS temporal_density,
            COALESCE(a.step_variance, 0) AS step_variance,
            COALESCE(a.has_burst_pattern, false) AS has_burst,
            COALESCE(a.high_confidence_pattern, false) AS high_confidence
        
        // Ensure values are within proper range
        WITH a, 
            CASE WHEN network_score > 1.0 THEN 1.0 ELSE network_score END AS norm_network_score,
            CASE WHEN behavior_score > 1.0 THEN 1.0 ELSE behavior_score END AS norm_behavior_score,
            CASE WHEN temporal_risk > 1.0 THEN 1.0 ELSE temporal_risk END AS norm_temporal_risk,
            CASE WHEN temporal_density > 5.0 THEN 1.0 ELSE temporal_density / 5.0 END AS norm_temporal_density,
            CASE WHEN step_variance > 5.0 THEN 1.0 ELSE step_variance / 5.0 END AS norm_step_variance,
            has_burst, high_confidence
        
        // Calculate model3 score with properly bounded components
        WITH a, norm_network_score, norm_behavior_score, norm_temporal_risk, 
            norm_temporal_density, norm_step_variance, has_burst, high_confidence,
            
            // Weighted combination of all signals (all weights add up to 1.0)
            CASE
                // High confidence cases (max 1.0)
                WHEN high_confidence AND norm_network_score > 0.8 AND norm_behavior_score > 0.8
                THEN CASE 
                    WHEN (norm_network_score * 0.35 + norm_behavior_score * 0.45 + norm_temporal_risk * 0.20) * 1.2 > 1.0
                    THEN 1.0
                    ELSE (norm_network_score * 0.35 + norm_behavior_score * 0.45 + norm_temporal_risk * 0.20) * 1.2
                    END
                
                // Strong pattern combinations (max 1.0)
                WHEN norm_network_score > 0.7 AND norm_behavior_score > 0.7 AND norm_temporal_risk > 0.7
                THEN CASE
                    WHEN (norm_network_score * 0.35 + norm_behavior_score * 0.40 + norm_temporal_risk * 0.25) * 1.1 > 1.0
                    THEN 1.0
                    ELSE (norm_network_score * 0.35 + norm_behavior_score * 0.40 + norm_temporal_risk * 0.25) * 1.1
                    END
                
                // Normal cases (max 1.0)
                ELSE norm_network_score * 0.30 + norm_behavior_score * 0.35 + norm_temporal_risk * 0.35
            END AS model3_score
        
        // Set final model3 score, ensuring it stays within 0-1 range
        SET a.model3_score = 
            CASE
                WHEN model3_score > 1.0 THEN 1.0
                WHEN model3_score < 0.0 THEN 0.0
                ELSE model3_score
            END
        
        RETURN COUNT(a) AS processed_count
    """

    # Specialized pattern queries
    CHAIN_PATTERN_QUERY = """
        // Find chain patterns (A->B->C->D where amounts are similar)
        MATCH path = (a:Account)-[tx1:SENT]->(b:Account)-[tx2:SENT]->(c:Account)
        WHERE 
            // Transaction happened within a short time window
            abs(tx1.step - tx2.step) <= 2 
            
            // Second amount is similar to first (allowing for small fee)
            AND abs(tx1.amount - tx2.amount) / tx1.amount < 0.1
            
            // Minimum amount to consider
            AND tx1.amount >= 5000
        
        WITH a, b, c, path, tx1, tx2
        
        // Look for longer chains (3+ links)
        OPTIONAL MATCH extended = (c)-[tx3:SENT]->(d:Account)
        WHERE 
            abs(tx2.step - tx3.step) <= 2
            AND abs(tx2.amount - tx3.amount) / tx2.amount < 0.1
        
        WITH a, b, c, path, tx1, tx2, extended, tx3, d
        
        // Set chain pattern on all accounts in the chain
        WITH CASE WHEN extended IS NOT NULL THEN [a, b, c, d] ELSE [a, b, c] END as chain_accounts,
            CASE WHEN extended IS NOT NULL THEN 4 ELSE 3 END as chain_length,
            CASE WHEN extended IS NOT NULL THEN tx1.amount ELSE tx1.amount END as chain_amount
        
        UNWIND chain_accounts as account
        WITH DISTINCT account, max(chain_length) as longest_chain, max(chain_amount) as highest_amount
        
        // Set pattern flags
        SET account.chain_pattern = true,
            account.pattern_risk_score = CASE
                                        WHEN longest_chain >= 4 THEN 0.85
                                        WHEN longest_chain = 3 AND highest_amount >= 10000 THEN 0.75
                                        ELSE 0.65
                                        END
        
        RETURN count(distinct account) as chain_pattern_accounts
    """

    FUNNEL_PATTERN_QUERY = """
        MATCH (src:Account)-[t:TRANSFER]->(a:Account)-[t2:TRANSFER]->(dst:Account)
        WITH a, count(DISTINCT src) as in_count, count(DISTINCT dst) as out_count,
            collect(DISTINCT src) as sources, collect(DISTINCT dst) as destinations
        WHERE in_count >= $funnelMinSources AND out_count <= 2

        // Tính tổng số tiền vào và ra
        MATCH (src:Account)-[t:TRANSFER]->(a:Account)
        WHERE src IN sources
        WITH a, in_count, out_count, sum(t.amount) as total_in

        MATCH (a:Account)-[t:TRANSFER]->(dst:Account)
        WHERE dst IN destinations
        WITH a, in_count, out_count, total_in, sum(t.amount) as total_out

        // Mẫu phễu hợp lệ khi giá trị đi ra gần bằng giá trị đi vào
        WHERE ABS(total_in - total_out) / total_in < 0.2

        SET a.funnel_pattern = true,
            a.risk_factors = CASE 
                WHEN a.risk_factors IS NULL THEN "Mẫu phễu (nhận nhiều, chuyển ít)"
                ELSE a.risk_factors + ", Mẫu phễu (nhận nhiều, chuyển ít)"
            END,
            a.model3_score = CASE
                WHEN a.model3_score IS NULL THEN 0.75
                ELSE (a.model3_score + 0.75) / 2
            END

        RETURN count(a) as funnel_accounts
    """

    ROUND_AMOUNT_QUERY = """
        // Find accounts with round amount transactions pattern
        MATCH (account:Account)-[tx:SENT]->()
        WITH account, tx
        WHERE 
            // Exact round amounts (multiples of significant values)
            (tx.amount % 10000 = 0 AND tx.amount >= 10000) OR
            (tx.amount % 5000 = 0 AND tx.amount >= 5000) OR
            (tx.amount % 1000 = 0 AND tx.amount >= 1000)
        
        // Group by account and count round transactions
        WITH account, count(tx) as round_tx_count, collect(tx) as round_txs
        WHERE round_tx_count >= 2  // Account must have at least 2 round transactions
        
        // Calculate percentage of round amount transactions
        MATCH (account)-[all_tx:SENT]->()
        WITH account, round_tx_count, round_txs, count(all_tx) as total_tx_count
        
        // Calculate risk score based on pattern strength
        WITH account, round_tx_count, total_tx_count,
            1.0 * round_tx_count / total_tx_count as round_tx_ratio,
            REDUCE(total = 0, tx IN round_txs | total + tx.amount) as round_amount_total
        
        // Only mark accounts with significant round transaction behavior
        WHERE round_tx_ratio >= 0.5 OR round_tx_count >= 3
        
        // Set pattern flags and score
        SET account.round_pattern = true,
            account.pattern_risk_score = CASE
                                        WHEN round_tx_ratio = 1.0 AND round_tx_count >= 3 THEN 0.8  // All transactions are round
                                        WHEN round_tx_ratio >= 0.75 THEN 0.7   // Mostly round
                                        WHEN round_tx_ratio >= 0.5 THEN 0.6    // Half round
                                        ELSE 0.5                              // Some round
                                        END
        
        RETURN count(account) as round_pattern_accounts
    """

    PATTERN_RISK_SCORE_QUERY = """
        MATCH (a:Account)
        WHERE a.chain_pattern = true OR 
              a.funnel_pattern = true OR
              a.round_pattern = true
        
        SET a.pattern_risk_score = 
            CASE
                // Combined patterns - highest risk
                WHEN a.chain_pattern = true AND a.funnel_pattern = true
                    THEN 0.9
                WHEN a.chain_pattern = true AND a.round_pattern = true
                    THEN 0.85
                WHEN a.funnel_pattern = true AND a.round_pattern = true
                    THEN 0.8
                
                // Single patterns - moderate to high risk
                WHEN a.chain_pattern = true AND a.chain_count >= $minChainLength * 2
                    THEN 0.75
                WHEN a.funnel_pattern = true AND a.funnel_sources >= $funnelMinSources * 2
                    THEN 0.7
                WHEN a.round_pattern = true AND a.round_tx_count >= 5
                    THEN 0.65
                
                // Base pattern risks
                WHEN a.chain_pattern = true
                    THEN 0.6
                WHEN a.funnel_pattern = true
                    THEN 0.55
                WHEN a.round_pattern = true
                    THEN 0.5
                
                ELSE 0.0
            END
    """

    # Transaction velocity queries
    CHECK_TIMESTAMPS_QUERY = """
        MATCH ()-[r:SENT]->()
        WHERE r.timestamp IS NOT NULL
        RETURN COUNT(r) > 0 AS has_timestamps
    """

    VELOCITY_ANALYSIS_QUERY = """
        MATCH (a:Account)-[tx:SENT]->()
        WITH a, tx ORDER BY a, tx.step
        WITH a, COLLECT(tx) AS transactions
        WHERE SIZE(transactions) > 1
        
        WITH a, transactions,
             // Calculate time differences between consecutive transactions
             REDUCE(diffs = [], i IN RANGE(0, SIZE(transactions)-2) |
                 diffs + [transactions[i+1].step - transactions[i].step]
             ) AS time_diffs
             
        WITH a, transactions, time_diffs,
             // Calculate average time between transactions
             toFloat(REDUCE(total = 0, t IN time_diffs | total + t)) / SIZE(time_diffs) AS avg_time_diff
        
        WITH a, transactions, time_diffs, avg_time_diff,
             // Count rapid transactions (significantly faster than average)
             SIZE([x IN time_diffs WHERE x < avg_time_diff * 0.5]) AS rapid_tx_count
             
        // Calculate transaction velocity score
        WITH a, SIZE(transactions) AS tx_count, rapid_tx_count,
             toFloat(rapid_tx_count) / SIZE(transactions) AS velocity_ratio
        
        // Update account properties with velocity metrics (ensure score is within bounds)
        SET a.high_velocity = true,
            a.velocity_ratio = velocity_ratio,
            a.velocity_score = 
                CASE 
                    WHEN velocity_ratio > 1.0 THEN 1.0
                    ELSE velocity_ratio
                END
        
        RETURN COUNT(a) AS accounts_analyzed,
               AVG(velocity_ratio) AS avg_velocity_ratio,
               SUM(CASE WHEN velocity_ratio > 0.5 THEN 1 ELSE 0 END) AS high_velocity_accounts
    """

    # Fixed Final risk score queries
    FINAL_RISK_SCORE_QUERY = """
        MATCH (a:Account)
        WITH a,
            COALESCE(a.model1_score, 0.0) as model1_score,
            COALESCE(a.model2_score, 0.0) as model2_score,
            COALESCE(a.model3_score, 0.0) as model3_score,
            COALESCE(a.ensemble_score, 0.0) as ensemble_score,
            COALESCE(a.temporal_risk_score, 0.0) as temporal_score,
            COALESCE(a.velocity_score, 0.0) as velocity_score,
            COALESCE(a.pattern_risk_score, 0.0) as pattern_score,
            COALESCE(a.funnel_pattern, false) as has_funnel_pattern,
            COALESCE(a.chain_pattern, false) as has_chain_pattern,
            COALESCE(a.round_pattern, false) as has_round_pattern,
            COALESCE(a.similar_to_fraud, false) as similar_to_fraud,
            COALESCE(a.similarity_score, 0.0) as similarity_score,
            COALESCE(a.high_confidence_pattern, false) as high_confidence,
            COALESCE(a.tx_anomaly, false) as tx_anomaly
        
        // Base calculation - weight by effectiveness in identifying fraud
        WITH a, 
            model1_score, model2_score, model3_score, ensemble_score, 
            temporal_score, velocity_score, pattern_score,
            has_funnel_pattern, has_chain_pattern, has_round_pattern, 
            similar_to_fraud, similarity_score,
            high_confidence, tx_anomaly,
            
            // Calculate base score with differentiated weights
            model1_score * 0.55 +         // 55% - Network structure (highest weight)
            model2_score * 0.15 +         // 15% - Behavioral patterns
            ensemble_score * 0.15 +       // 15% - Ensemble score
            temporal_score * 0.05 +       // 5% - Temporal patterns
            pattern_score * 0.10          // 10% - Pattern risk
            as base_score
        
        // Create truly stratified risk scores for better differentiation
        WITH a, base_score, model1_score, high_confidence, tx_anomaly, 
            has_funnel_pattern, has_chain_pattern, has_round_pattern,
            similar_to_fraud, similarity_score,
        
            // Pattern boost calculation
            CASE
                // High-precision pattern combinations
                WHEN has_funnel_pattern AND has_chain_pattern THEN 0.30
                WHEN has_funnel_pattern AND has_round_pattern THEN 0.25
                WHEN has_chain_pattern AND has_round_pattern THEN 0.20
                
                // Individual strong patterns
                WHEN has_funnel_pattern THEN 0.15
                WHEN has_chain_pattern THEN 0.15
                WHEN has_round_pattern THEN 0.10
                ELSE 0.0
            END as pattern_boost,
            
            // Similarity boost if similar to known fraud
            CASE 
                WHEN similar_to_fraud THEN similarity_score * 0.25
                ELSE 0
            END as similarity_boost
        
        // Apply strict stratification rules based on risk signals
        WITH a, base_score, pattern_boost, similarity_boost,
            model1_score, high_confidence, tx_anomaly,
            
            // Generate a stratified score with better differentiation
            CASE
                // Very high risk - strict criteria with score >= 0.8
                WHEN high_confidence AND model1_score > 0.8 THEN 0.85
                WHEN model1_score > 0.85 THEN 0.82
                
                // High risk - criteria for score >= 0.7
                WHEN high_confidence AND model1_score > 0.7 THEN 0.76
                WHEN model1_score > 0.75 THEN 0.74
                WHEN model1_score > 0.7 AND pattern_boost > 0.2 THEN 0.72
                
                // Medium-high risk - criteria for score >= 0.6
                WHEN high_confidence THEN 0.68
                WHEN model1_score > 0.65 THEN 0.66
                WHEN model1_score > 0.6 AND pattern_boost > 0.15 THEN 0.64
                WHEN model1_score > 0.55 AND similarity_boost > 0.15 THEN 0.62
                
                // Medium risk - criteria for score >= 0.5
                WHEN model1_score > 0.6 THEN 0.58
                WHEN model1_score > 0.55 THEN 0.55
                WHEN model1_score > 0.5 AND pattern_boost > 0.1 THEN 0.53
                WHEN pattern_boost > 0.25 THEN 0.52
                
                // Use base score for all other accounts
                ELSE base_score
            END as stratified_score
        
        // Calculate final score with boosts and bounds
        WITH a, 
            CASE
                // Apply the stratified score
                WHEN stratified_score >= 0.5 THEN stratified_score
                
                // Or use base score with boosts for lower scores
                ELSE 
                    CASE 
                        WHEN base_score + pattern_boost + similarity_boost > 1.0 THEN 1.0
                        ELSE base_score + pattern_boost + similarity_boost
                    END
            END AS final_score
        
        // Set final fraud score and risk level
        SET a.fraud_score = final_score,
            a.risk_level = 
                CASE
                    WHEN final_score >= $veryHighRisk THEN 'VERY_HIGH_RISK'
                    WHEN final_score >= $highRisk THEN 'HIGH_RISK'
                    WHEN final_score >= $suspicious THEN 'SUSPICIOUS'
                    ELSE 'NORMAL'
                END
        
        RETURN COUNT(a) as updated_accounts,
            SUM(CASE WHEN final_score >= $fraudThreshold THEN 1 ELSE 0 END) as fraud_accounts,
            SUM(CASE WHEN final_score >= 0.7 THEN 1 ELSE 0 END) as accounts_above_07,
            SUM(CASE WHEN final_score >= 0.6 THEN 1 ELSE 0 END) as accounts_above_06,
            SUM(CASE WHEN final_score >= 0.5 THEN 1 ELSE 0 END) as accounts_above_05,
            AVG(final_score) as avg_score,
            MIN(final_score) as min_score,
            MAX(final_score) as max_score
    """

    # Additional helpful query - can be used before final scoring
    NORMALIZE_SCORES_QUERY = """
        // First get statistics for Model 1 scores
        MATCH (a:Account)
        WHERE a.model1_score IS NOT NULL
        WITH 
            AVG(a.model1_score) AS avg_model1,
            MAX(a.model1_score) AS max_model1,
            MIN(a.model1_score) AS min_model1
        WHERE max_model1 > min_model1  // Only normalize if there's a range
        
        // Now normalize all Model 1 scores to 0-1 range
        MATCH (a:Account)
        WHERE a.model1_score IS NOT NULL
        SET a.model1_score = 
            CASE 
                WHEN max_model1 = min_model1 THEN 0.5  // All same value
                ELSE (a.model1_score - min_model1) / (max_model1 - min_model1)
            END
        
        // Repeat for Model 3
        WITH true AS continue
        MATCH (a:Account)
        WHERE a.model3_score IS NOT NULL
        WITH 
            AVG(a.model3_score) AS avg_model3,
            MAX(a.model3_score) AS max_model3,
            MIN(a.model3_score) AS min_model3
        WHERE max_model3 > min_model3  // Only normalize if there's a range
        
        MATCH (a:Account)
        WHERE a.model3_score IS NOT NULL
        SET a.model3_score = 
            CASE 
                WHEN max_model3 = min_model3 THEN 0.5  // All same value
                ELSE (a.model3_score - min_model3) / (max_model3 - min_model3)
            END
        
        RETURN "Scores normalized successfully" AS result
    """

    # Fixed Ensemble score query
    ENSEMBLE_SCORE_QUERY = """
        MATCH (a:Account)
        WITH a,
            // Retrieve and normalize all model scores
            COALESCE(a.model1_score, 0) AS model1_score,
            COALESCE(a.model2_score, 0) AS model2_score,
            COALESCE(a.model3_score, 0) AS model3_score,
            COALESCE(a.funnel_pattern, false) AS has_funnel,
            COALESCE(a.chain_pattern, false) AS has_chain,
            COALESCE(a.round_pattern, false) AS has_round,
            COALESCE(a.high_velocity, false) AS has_velocity,
            COALESCE(a.high_value_tx, false) AS has_high_value,
            COALESCE(a.tx_anomaly, false) AS has_anomaly,
            COALESCE(a.burst_pattern, false) AS has_burst,
            COALESCE(a.high_confidence_pattern, false) AS high_confidence
        
        // Drastically reduce the base score to ensure better precision
        WITH a, model1_score, model2_score, model3_score,
            has_funnel, has_chain, has_round, has_velocity, has_high_value, has_anomaly, has_burst, high_confidence,
            
            // MUCH LOWER base score for most accounts
            CASE 
                // Only high confidence accounts get a decent baseline
                WHEN high_confidence THEN 0.45 + (model1_score * 0.15)
                
                // Regular scoring - make model1 the dominant factor
                ELSE model1_score * 0.35 + model2_score * 0.05 + model3_score * 0.05
            END AS base_score
        
        // Apply boosts only for significant indicators or combinations
        WITH a, base_score, model1_score, high_confidence,
            has_funnel, has_chain, has_round, has_velocity, has_high_value, has_anomaly, has_burst,
            
            // Pattern-specific boosts (adjusted for better selectivity)
            CASE
                // Very specific high-risk combinations only
                WHEN has_funnel AND has_high_value AND has_anomaly THEN 0.35
                WHEN has_funnel AND has_high_value THEN 0.25
                WHEN has_funnel AND has_anomaly THEN 0.25
                WHEN has_chain AND has_round THEN 0.20
                WHEN has_velocity AND has_burst THEN 0.15
                
                // Individual pattern boosts - minimal values 
                WHEN has_funnel THEN 0.10
                WHEN has_anomaly AND model1_score > 0.5 THEN 0.10
                WHEN has_high_value AND model1_score > 0.6 THEN 0.10
                ELSE 0.0
            END AS pattern_boost,
            
            // Bonus for very high model1 scores (better precision)
            CASE
                WHEN model1_score > 0.8 THEN 0.25   // Very high network risk
                WHEN model1_score > 0.7 THEN 0.20   // High network risk
                WHEN model1_score > 0.6 THEN 0.10   // Moderate network risk
                ELSE 0.0
            END AS model1_bonus
        
        // Calculate final ensemble score with controlled boosts
        WITH a, base_score, pattern_boost, model1_bonus, high_confidence, model1_score,
            base_score + pattern_boost + model1_bonus AS raw_score
        
        // Final score with strict stratification to ensure differentiation between thresholds
        SET a.ensemble_score = 
            CASE
                // High confidence accounts with very high model1 should be above 0.7
                WHEN high_confidence AND model1_score > 0.8 THEN 
                    CASE WHEN raw_score < 0.8 THEN 0.8 ELSE raw_score END
                    
                // High confidence accounts should be at least 0.6
                WHEN high_confidence THEN 
                    CASE WHEN raw_score < 0.6 THEN 0.6 ELSE raw_score END
                    
                // Very high model1 scores should be at least 0.5
                WHEN model1_score > 0.7 THEN
                    CASE WHEN raw_score < 0.5 THEN 0.5 ELSE raw_score END
                    
                // Normal accounts - enforce maximum to improve precision
                WHEN raw_score > 1.0 THEN 1.0
                ELSE raw_score
            END
        
        RETURN COUNT(a) AS scored_accounts,
            SUM(CASE WHEN high_confidence THEN 1 ELSE 0 END) AS high_confidence_accounts,
            SUM(CASE WHEN a.ensemble_score >= 0.7 THEN 1 ELSE 0 END) AS accounts_above_07,
            SUM(CASE WHEN a.ensemble_score >= 0.6 THEN 1 ELSE 0 END) AS accounts_above_06,
            SUM(CASE WHEN a.ensemble_score >= 0.5 THEN 1 ELSE 0 END) AS accounts_above_05,
            AVG(a.ensemble_score) AS avg_score,
            MAX(a.ensemble_score) AS max_score
    """

    HIGH_CONFIDENCE_PATTERN_QUERY = """
        MATCH (a:Account)
        WITH a, 
            COALESCE(a.model1_score, 0) as model1_score,
            COALESCE(a.model2_score, 0) as model2_score,
            COALESCE(a.funnel_pattern, false) as funnel_pattern,
            COALESCE(a.chain_pattern, false) as chain_pattern,
            COALESCE(a.round_pattern, false) as round_pattern,
            COALESCE(a.high_value_tx, false) as high_value_tx,
            COALESCE(a.tx_anomaly, false) as tx_anomaly,
            COALESCE(a.burst_pattern, false) as burst_pattern
            
        // First check if model scores are truly significant - this needs to be MUCH more selective
        WITH a, model1_score, model2_score,
            funnel_pattern, chain_pattern, round_pattern, high_value_tx, tx_anomaly, burst_pattern,
            (CASE
                // ONLY mark high confidence when model1 score is VERY high
                WHEN model1_score > 0.85 THEN true
                
                // OR when model1 is high AND model2 is also high
                WHEN model1_score > 0.75 AND model2_score > 0.65 THEN true
                
                // OR when model1 is high AND funnel pattern exists (key fraud pattern)
                WHEN model1_score > 0.7 AND funnel_pattern THEN true
                
                // OR when very specific pattern combinations exist
                WHEN funnel_pattern AND high_value_tx AND tx_anomaly THEN true
                
                // Otherwise, not high confidence
                ELSE false
            END) as high_confidence
        
        // Store high confidence status and pattern factors that led to it
        SET a.high_confidence_pattern = high_confidence,
            a.confidence_factors = 
                CASE WHEN high_confidence THEN 
                    REDUCE(patterns = [], i IN [
                        CASE WHEN model1_score > 0.75 THEN 'high_network_risk' ELSE NULL END,
                        CASE WHEN model2_score > 0.65 THEN 'high_behavior_risk' ELSE NULL END,
                        CASE WHEN funnel_pattern THEN 'funnel_pattern' ELSE NULL END,
                        CASE WHEN tx_anomaly THEN 'transaction_anomaly' ELSE NULL END,
                        CASE WHEN high_value_tx THEN 'high_value_tx' ELSE NULL END,
                        CASE WHEN round_pattern THEN 'round_amounts' ELSE NULL END
                    ] | 
                        CASE WHEN i IS NOT NULL THEN patterns + [i] ELSE patterns END
                    )
                ELSE []
                END
        
        RETURN COUNT(a) as accounts_analyzed,
            SUM(CASE WHEN high_confidence THEN 1 ELSE 0 END) as high_confidence_accounts
    """

    # Add this query at the end of your pattern_queries.py file
    PATTERN_STATS_QUERY = """
        MATCH (a:Account)
        
        // Count pattern occurrences
        WITH COUNT(a) as total,
            SUM(CASE WHEN a.model1_score > 0.5 THEN 1 ELSE 0 END) as model1_count,
            SUM(CASE WHEN a.model2_score > 0.5 THEN 1 ELSE 0 END) as model2_count,
            SUM(CASE WHEN a.model3_score > 0.5 THEN 1 ELSE 0 END) as model3_count,
            SUM(CASE WHEN a.high_confidence_pattern = true THEN 1 ELSE 0 END) as high_confidence_count,
            SUM(CASE WHEN a.funnel_pattern = true THEN 1 ELSE 0 END) as funnel_count,
            SUM(CASE WHEN a.round_pattern = true THEN 1 ELSE 0 END) as round_count,
            SUM(CASE WHEN a.chain_pattern = true THEN 1 ELSE 0 END) as chain_count,
            SUM(CASE WHEN a.similar_to_fraud = true THEN 1 ELSE 0 END) as similar_count,
            SUM(CASE WHEN a.high_velocity = true THEN 1 ELSE 0 END) as high_velocity_count
            
        // Calculate related transactions
        OPTIONAL MATCH (a:Account)-[tx:SENT]->()
        WHERE a.model1_score > 0.5
        WITH total, model1_count, model2_count, model3_count, high_confidence_count,
            funnel_count, round_count, chain_count, similar_count, high_velocity_count,
            COUNT(DISTINCT tx) as model1_txs
            
        OPTIONAL MATCH (a:Account)-[tx:SENT]->()
        WHERE a.model2_score > 0.5
        WITH total, model1_count, model2_count, model3_count, high_confidence_count,
            funnel_count, round_count, chain_count, similar_count, high_velocity_count,
            model1_txs, COUNT(DISTINCT tx) as model2_txs
            
        OPTIONAL MATCH (a:Account)-[tx:SENT]->()
        WHERE a.model3_score > 0.5
        WITH total, model1_count, model2_count, model3_count, high_confidence_count,
            funnel_count, round_count, chain_count, similar_count, high_velocity_count,
            model1_txs, model2_txs, COUNT(DISTINCT tx) as model3_txs
            
        OPTIONAL MATCH (a:Account)-[tx:SENT]->()
        WHERE a.high_confidence_pattern = true
        WITH total, model1_count, model2_count, model3_count, high_confidence_count,
            funnel_count, round_count, chain_count, similar_count, high_velocity_count,
            model1_txs, model2_txs, model3_txs, COUNT(DISTINCT tx) as high_confidence_txs
            
        OPTIONAL MATCH (a:Account)-[tx:SENT]->()
        WHERE a.funnel_pattern = true
        WITH total, model1_count, model2_count, model3_count, high_confidence_count,
            funnel_count, round_count, chain_count, similar_count, high_velocity_count,
            model1_txs, model2_txs, model3_txs, high_confidence_txs, COUNT(DISTINCT tx) as funnel_txs
            
        OPTIONAL MATCH (a:Account)-[tx:SENT]->()
        WHERE a.round_pattern = true  
        WITH total, model1_count, model2_count, model3_count, high_confidence_count,
            funnel_count, round_count, chain_count, similar_count, high_velocity_count,
            model1_txs, model2_txs, model3_txs, high_confidence_txs, funnel_txs, COUNT(DISTINCT tx) as round_txs
            
        OPTIONAL MATCH (a:Account)-[tx:SENT]->()
        WHERE a.chain_pattern = true
        WITH total, model1_count, model2_count, model3_count, high_confidence_count,
            funnel_count, round_count, chain_count, similar_count, high_velocity_count,
            model1_txs, model2_txs, model3_txs, high_confidence_txs, funnel_txs, round_txs, COUNT(DISTINCT tx) as chain_txs
            
        OPTIONAL MATCH (a:Account)-[tx:SENT]->()
        WHERE a.similar_to_fraud = true
        WITH total, model1_count, model2_count, model3_count, high_confidence_count,
            funnel_count, round_count, chain_count, similar_count, high_velocity_count,
            model1_txs, model2_txs, model3_txs, high_confidence_txs, funnel_txs, round_txs, chain_txs, COUNT(DISTINCT tx) as similar_txs
            
        OPTIONAL MATCH (a:Account)-[tx:SENT]->()
        WHERE a.high_velocity = true
        WITH total, model1_count, model2_count, model3_count, high_confidence_count,
            funnel_count, round_count, chain_count, similar_count, high_velocity_count,
            model1_txs, model2_txs, model3_txs, high_confidence_txs, funnel_txs, round_txs, chain_txs, similar_txs, COUNT(DISTINCT tx) as velocity_txs
            
        // Return all statistics as individual fields instead of a nested map
        RETURN total as total_accounts,
            model1_count as model1_count,
            model1_txs as model1_txs,
            model2_count as model2_count,
            model2_txs as model2_txs,
            model3_count as model3_count,
            model3_txs as model3_txs,
            high_confidence_count as high_confidence_count,
            high_confidence_txs as high_confidence_txs,
            funnel_count as funnel_count,
            funnel_txs as funnel_txs,
            round_count as round_count,
            round_txs as round_txs,
            chain_count as chain_count,
            chain_txs as chain_txs,
            similar_count as similar_count,
            similar_txs as similar_txs,
            high_velocity_count as velocity_count,
            velocity_txs as velocity_txs
    """

    SIMILAR_TO_FRAUD_QUERY = """
        // First identify high-confidence fraud accounts
        MATCH (a:Account)
        WHERE a.high_confidence_pattern = true AND a.model1_score > 0.7
        WITH COLLECT(a) as known_fraud_accounts
        WHERE size(known_fraud_accounts) > 0
        
        // Find accounts with similar transaction patterns to known fraud
        UNWIND known_fraud_accounts as fraud_account
        
        // Find accounts with transactions connected to the same entities as fraud accounts
        MATCH (fraud_account)-[:SENT]->(common_receiver:Account)
        MATCH (suspect:Account)-[:SENT]->(common_receiver)
        WHERE suspect <> fraud_account
        
        // Calculate similarity score based on common connections and transaction patterns
        WITH suspect, fraud_account, count(distinct common_receiver) as common_receivers
        
        // Get fraud account patterns
        WITH suspect, fraud_account, common_receivers,
            fraud_account.model1_score as fraud_model1,
            fraud_account.model2_score as fraud_model2,
            COALESCE(fraud_account.funnel_pattern, false) as fraud_funnel,
            COALESCE(fraud_account.round_pattern, false) as fraud_round,
            COALESCE(fraud_account.chain_pattern, false) as fraud_chain
            
        // Check suspect for similar patterns
        WITH suspect, fraud_account, common_receivers, fraud_model1, fraud_model2, 
            fraud_funnel, fraud_round, fraud_chain,
            COALESCE(suspect.model1_score, 0) as suspect_model1,
            COALESCE(suspect.model2_score, 0) as suspect_model2,
            COALESCE(suspect.funnel_pattern, false) as suspect_funnel,
            COALESCE(suspect.round_pattern, false) as suspect_round,
            COALESCE(suspect.chain_pattern, false) as suspect_chain
        
        // Calculate similarity score
        WITH suspect, common_receivers,
            (CASE WHEN suspect_model1 > 0.5 AND fraud_model1 > 0.5 THEN 1 ELSE 0 END +
            CASE WHEN suspect_model2 > 0.5 AND fraud_model2 > 0.5 THEN 1 ELSE 0 END +
            CASE WHEN suspect_funnel AND fraud_funnel THEN 1 ELSE 0 END +
            CASE WHEN suspect_round AND fraud_round THEN 1 ELSE 0 END +
            CASE WHEN suspect_chain AND fraud_chain THEN 1 ELSE 0 END) as pattern_similarity,
            
            // Connection similarity based on common receivers
            CASE
                WHEN common_receivers >= 3 THEN 1.0
                WHEN common_receivers = 2 THEN 0.7
                WHEN common_receivers = 1 THEN 0.4
                ELSE 0
            END as connection_similarity
        
        // Calculate overall similarity 
        WITH suspect, 
            pattern_similarity * 0.6 + connection_similarity * 0.4 as similarity_score
        WHERE similarity_score >= 0.6
        
        // Mark accounts that are similar to known fraud
        SET suspect.similar_to_fraud = true,
            suspect.similarity_score = similarity_score
        
        RETURN count(distinct suspect) as similar_accounts
    """