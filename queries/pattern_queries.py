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
        
        // Calculate additional behavior patterns
        WITH a, out_txs, in_txs, out_count, in_count, out_amount, in_amount,
            // Transaction imbalance ratio
            CASE 
                WHEN in_count + out_count = 0 THEN 0
                WHEN in_count = 0 AND out_count > 0 THEN 1.0
                WHEN out_count = 0 AND in_count > 0 THEN 1.0
                ELSE abs(in_count - out_count) / (in_count + out_count)
            END as tx_count_imbalance,
            
            // Amount imbalance ratio
            CASE 
                WHEN in_amount + out_amount = 0 THEN 0
                WHEN in_amount = 0 AND out_amount > 0 THEN 1.0
                WHEN out_amount = 0 AND in_amount > 0 THEN 1.0
                ELSE abs(in_amount - out_amount) / (in_amount + out_amount)
            END as amount_imbalance,
            
            // High value transactions (>5000)
            [t IN out_txs WHERE t.amount > 5000] as high_value_txs,
            
            // Round amount transactions
            [t IN out_txs WHERE round(t.amount) = t.amount AND t.amount >= 1000] as round_txs,
            
            // Detect transaction bursts
            CASE WHEN size(out_txs) >= 3 THEN
                // Count transactions with same step (timestamp)
                SIZE(REDUCE(steps = [], t IN out_txs | 
                    CASE WHEN NOT t.step IN steps THEN steps + [t.step] ELSE steps END
                )) < size(out_txs) * 0.7  // If < 70% unique timestamps, bursts exist
            ELSE false END as has_bursts
        
        // Calculate behavioral risk factors
        WITH a, tx_count_imbalance, amount_imbalance, 
            size(high_value_txs) > 0 as has_high_value,
            size(round_txs) >= 2 as has_round_amounts,
            has_bursts,
            out_count, in_count
            
        // Calculate model2 score with behavioral factors
        WITH a, tx_count_imbalance, amount_imbalance, has_high_value, has_round_amounts, has_bursts, out_count, in_count,
            // Transaction imbalance component (40%)
            CASE
                WHEN tx_count_imbalance > 0.8 AND amount_imbalance > 0.8 THEN 0.9
                WHEN tx_count_imbalance > 0.8 OR amount_imbalance > 0.8 THEN 0.8
                WHEN tx_count_imbalance > 0.6 AND amount_imbalance > 0.6 THEN 0.7
                WHEN tx_count_imbalance > 0.6 OR amount_imbalance > 0.6 THEN 0.6
                WHEN tx_count_imbalance > 0.4 OR amount_imbalance > 0.4 THEN 0.4
                ELSE 0.2
            END * 0.4 as imbalance_component,
            
            // Transaction pattern component (35%)
            CASE
                WHEN has_high_value AND has_round_amounts AND has_bursts THEN 0.9
                WHEN has_high_value AND (has_round_amounts OR has_bursts) THEN 0.8
                WHEN has_round_amounts AND has_bursts THEN 0.7
                WHEN has_high_value THEN 0.6
                WHEN has_round_amounts THEN 0.5
                WHEN has_bursts THEN 0.4
                ELSE 0.1
            END * 0.35 as pattern_component,
            
            // Volume component (25%)
            CASE
                WHEN out_count > 10 OR in_count > 10 THEN 1.0
                WHEN out_count > 5 OR in_count > 5 THEN 0.8
                WHEN out_count > 0 OR in_count > 0 THEN 0.5
                ELSE 0.0
            END * 0.25 as volume_component
        
        // Calculate final model2 score
        WITH a, imbalance_component, pattern_component, volume_component,
            has_high_value, has_round_amounts, has_bursts,
            tx_count_imbalance, amount_imbalance,
            imbalance_component + pattern_component + volume_component as raw_score
        
        // Set model2 score and pattern flags
        SET a.model2_score = 
            CASE
                WHEN raw_score > 1.0 THEN 1.0
                ELSE raw_score
            END,
            a.high_value_tx = has_high_value,
            a.tx_anomaly = tx_count_imbalance > 0.8 OR amount_imbalance > 0.8,
            a.round_pattern = has_round_amounts,
            a.burst_pattern = has_bursts
        
        RETURN COUNT(a) AS processed_count,
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
        MATCH path=(src:Account)-[tx1:SENT]->(mid:Account)-[tx2:SENT]->(dst:Account)
        WHERE tx2.step > tx1.step 
        AND tx2.step - tx1.step <= $chainTimeWindow
        WITH src, COLLECT(DISTINCT path) as paths
        WHERE size(paths) >= $minChainLength
        SET src.chain_pattern = true,
            src.chain_count = size(paths)
        RETURN COUNT(DISTINCT src) as accounts_with_chains
    """

    FUNNEL_PATTERN_QUERY = """
        MATCH (src:Account)-[tx:SENT]->(dst:Account)
        WITH dst, COUNT(DISTINCT src) as source_count,
             COLLECT(DISTINCT tx) as transactions
        WHERE source_count >= $funnelMinSources
        SET dst.funnel_pattern = true,
            dst.funnel_sources = source_count,
            dst.funnel_amount = REDUCE(s = 0, t IN transactions | s + t.amount)
        RETURN COUNT(DISTINCT dst) as accounts_with_funnels
    """

    ROUND_AMOUNT_QUERY = """
        MATCH (src:Account)-[tx:SENT]->()
        WHERE tx.amount >= $roundAmountMin
        AND round(tx.amount) = tx.amount
        WITH src, COUNT(tx) as round_count
        WHERE round_count >= 2
        SET src.round_pattern = true,
            src.round_tx_count = round_count
        RETURN COUNT(DISTINCT src) as accounts_with_rounds
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
            COALESCE(a.temporal_risk_score, 0.0) as temporal_score,
            COALESCE(a.velocity_score, 0.0) as velocity_score,
            COALESCE(a.pattern_risk_score, 0.0) as pattern_score,
            COALESCE(a.funnel_pattern, false) as has_funnel_pattern,
            COALESCE(a.chain_pattern, false) as has_chain_pattern,
            COALESCE(a.round_pattern, false) as has_round_pattern
        
        // Simple bound checks
        WITH a,
            CASE WHEN model1_score > 1.0 THEN 1.0 ELSE model1_score END AS norm_model1,
            CASE WHEN model2_score > 1.0 THEN 1.0 ELSE model2_score END AS norm_model2,
            CASE WHEN model3_score > 1.0 THEN 1.0 ELSE model3_score END AS norm_model3,
            CASE WHEN temporal_score > 1.0 THEN 1.0 ELSE temporal_score END AS norm_temporal,
            CASE WHEN velocity_score > 1.0 THEN 1.0 ELSE velocity_score END AS norm_velocity,
            CASE WHEN pattern_score > 1.0 THEN 1.0 ELSE pattern_score END AS norm_pattern,
            has_funnel_pattern, has_chain_pattern, has_round_pattern
            
        // Apply boosting for known patterns (important for fraud detection)
        WITH a, norm_model1, norm_model2, norm_model3, norm_temporal, norm_velocity, norm_pattern,
            has_funnel_pattern, has_chain_pattern, has_round_pattern,
            
            // Boost scores for accounts with funnel patterns (known fraud indicator)
            CASE
                WHEN has_funnel_pattern THEN 0.2  // Significant boost for funnel pattern
                ELSE 0.0
            END AS funnel_boost,
            
            // Boost for chain patterns
            CASE
                WHEN has_chain_pattern THEN 0.15
                ELSE 0.0
            END AS chain_boost,
            
            // Boost for round amount patterns
            CASE
                WHEN has_round_pattern THEN 0.1
                ELSE 0.0
            END AS round_boost
            
        // Calculate base and boosted scores
        WITH a, norm_model1, norm_model2, norm_model3, norm_temporal, norm_velocity, norm_pattern,
            funnel_boost, chain_boost, round_boost,
            
            // Base score using weighted models
            (norm_model1 * 0.30 +      // Network structure model (30%)
            norm_model2 * 0.25 +      // Behavioral patterns model (25%)
            norm_model3 * 0.25 +      // Complex pattern model (25%)
            norm_temporal * 0.10 +    // Temporal patterns (10%)
            norm_velocity * 0.05 +    // Velocity score (5%)
            norm_pattern * 0.05       // Pattern score (5%)
            ) AS base_score,
            
            // Pattern boosts help identify fraud better
            funnel_boost + chain_boost + round_boost AS pattern_boost
        
        // Calculate final score with pattern boost
        WITH a, norm_model1, norm_model2, 
            base_score,
            pattern_boost,
            // Adjust minimum threshold for model1/model2 scores
            CASE
                WHEN norm_model1 > 0.6 OR norm_model2 > 0.6 THEN 0.35
                ELSE 0.0
            END AS min_score
        
        // Combine all factors with bound check
        WITH a, 
            CASE
                WHEN base_score + pattern_boost > min_score THEN base_score + pattern_boost
                ELSE min_score
            END AS adjusted_score
        
        // Final bounds check
        WITH a, 
            CASE
                WHEN adjusted_score > 1.0 THEN 1.0
                ELSE adjusted_score
            END AS final_score
        
        // Set final fraud score and risk level with ADJUSTED THRESHOLDS
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
        
        // Calculate base ensemble score - weight high confidence patterns heavily
        WITH a, 
            // If high confidence, use a higher baseline score
            CASE 
                WHEN high_confidence THEN 0.6 +
                    (model1_score * 0.2 + model2_score * 0.2 + model3_score * 0.0)
                // Otherwise use weighted average with heavier model1 weight
                ELSE model1_score * 0.55 + model2_score * 0.4 + model3_score * 0.05
            END AS base_score,
            
            // Count pattern indicators
            (CASE WHEN has_funnel THEN 1 ELSE 0 END +
            CASE WHEN has_chain THEN 1 ELSE 0 END +
            CASE WHEN has_round THEN 1 ELSE 0 END +
            CASE WHEN has_velocity THEN 1 ELSE 0 END +
            CASE WHEN has_high_value THEN 1 ELSE 0 END +
            CASE WHEN has_anomaly THEN 1 ELSE 0 END +
            CASE WHEN has_burst THEN 1 ELSE 0 END) AS pattern_count,
            
            // Individual pattern flags
            has_funnel, has_chain, has_round, has_velocity, has_high_value, has_anomaly, has_burst, high_confidence
        
        // Apply pattern-based boosts based on precision analysis
        WITH a, base_score, pattern_count,
            has_funnel, has_chain, has_round, has_velocity, has_high_value, has_anomaly, has_burst, high_confidence,
            
            // Pattern count boost
            CASE
                WHEN pattern_count >= 3 THEN 0.35  // 3+ patterns is very suspicious
                WHEN pattern_count = 2 THEN 0.25   // 2 patterns is suspicious
                WHEN pattern_count = 1 THEN 0.15   // 1 pattern is somewhat suspicious
                ELSE 0.0
            END AS pattern_boost,
            
            // Special combination boosts for known high-precision patterns
            CASE
                WHEN has_funnel AND has_high_value THEN 0.35  // Very suspicious combination
                WHEN has_chain AND has_round THEN 0.30         // Suspicious combination
                WHEN has_velocity AND has_burst THEN 0.25      // Suspicious combination
                WHEN has_anomaly AND has_high_value THEN 0.35  // Very suspicious combination
                ELSE 0.0
            END AS combo_boost
        
        // Calculate final ensemble score with both boosts
        WITH a, base_score, pattern_boost, combo_boost, high_confidence,
            // If high confidence, ensure minimum score of 0.6 plus boosts
            CASE 
                WHEN high_confidence THEN 
                    CASE 
                        WHEN base_score > 0.6 THEN base_score
                        ELSE 0.6
                    END + pattern_boost + combo_boost
                // Otherwise, normal scoring
                ELSE base_score + pattern_boost + combo_boost
            END AS boosted_score
        
        // Apply bounds and set final score
        SET a.ensemble_score = 
            CASE
                WHEN boosted_score > 1.0 THEN 1.0
                ELSE boosted_score
            END
        
        RETURN COUNT(a) AS scored_accounts,
            COUNT(CASE WHEN boosted_score >= $fraudThreshold THEN 1 END) AS fraud_accounts,
            AVG(CASE WHEN boosted_score > 1.0 THEN 1.0 ELSE boosted_score END) AS avg_score,
            MAX(CASE WHEN boosted_score > 1.0 THEN 1.0 ELSE boosted_score END) AS max_score
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
        
        // Count significant risk factors
        WITH a, model1_score, model2_score,
            (CASE WHEN funnel_pattern THEN 1 ELSE 0 END +
            CASE WHEN chain_pattern THEN 1 ELSE 0 END +
            CASE WHEN round_pattern THEN 1 ELSE 0 END +
            CASE WHEN high_value_tx THEN 1 ELSE 0 END +
            CASE WHEN tx_anomaly THEN 1 ELSE 0 END +
            CASE WHEN burst_pattern THEN 1 ELSE 0 END) as risk_factor_count
        
        // Set high_confidence_pattern where there are multiple strong indicators
        WITH a,
            // Combination of model scores indicates higher confidence
            (model1_score > 0.7 AND model2_score > 0.5) OR
            // Multiple risk factors indicate higher confidence
            (risk_factor_count >= 3 AND (model1_score > 0.5 OR model2_score > 0.5)) OR
            // Extremely high risk in one model with some risk factors
            (model1_score > 0.8 AND risk_factor_count >= 1) OR
            (model2_score > 0.8 AND risk_factor_count >= 1)
            as high_confidence
        
        SET a.high_confidence_pattern = high_confidence
        
        RETURN COUNT(a) as accounts_analyzed,
            SUM(CASE WHEN high_confidence THEN 1 ELSE 0 END) as high_confidence_accounts
    """