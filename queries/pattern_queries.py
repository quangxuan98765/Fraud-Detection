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

    # Fraud scoring model queries
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
        
        WITH a, pagerank, degree, similarity, imbalance, amount_imbalance,
             temporal_density, step_variance, has_burst, burst_count,
             
             // Network metrics (50% of model weight)
             (pagerank * 0.20 +
              degree * 0.15 + 
              similarity * 0.15) * 0.50 +
             
             // Transaction metrics (30% of model weight)
             (CASE 
                 WHEN imbalance > 0.95 THEN 0.30
                 WHEN imbalance > 0.90 THEN 0.25
                 WHEN imbalance > 0.85 THEN 0.20
                 WHEN imbalance > 0.80 THEN 0.15
                 ELSE imbalance * 0.10
              END +
              CASE 
                 WHEN amount_imbalance > 0.95 THEN 0.20
                 WHEN amount_imbalance > 0.90 THEN 0.15
                 WHEN amount_imbalance > 0.85 THEN 0.10
                 ELSE amount_imbalance * 0.05
              END) * 0.30 +
              
             // Temporal metrics (20% of model weight)
             (CASE
                 WHEN has_burst AND burst_count >= 5 THEN 0.30
                 WHEN has_burst THEN 0.20
                 ELSE 0
              END +
              CASE
                 WHEN temporal_density > 3 THEN 0.20
                 WHEN temporal_density > 2 THEN 0.15
                 WHEN temporal_density > 1 THEN 0.10
                 ELSE temporal_density * 0.05
              END +
              CASE
                 WHEN step_variance < 0.5 THEN 0.20  // Low variance = suspicious
                 WHEN step_variance < 1.0 THEN 0.15
                 ELSE 0.05
              END) * 0.20
        AS model1_score
        
        SET a.model1_score = model1_score
        RETURN COUNT(a) AS processed_count
    """

    MODEL2_BEHAVIOR_QUERY = """
        MATCH (a:Account)
        // Look for transactions, but don't fail if none exist
        OPTIONAL MATCH (a)-[tx:SENT]->(b:Account)
        WITH a, COLLECT(tx) as txs
          // Default values in case of no transactions
        WITH a, txs,
             SIZE(txs) as tx_count,
             CASE WHEN SIZE(txs) > 0 THEN REDUCE(s = 0, t IN txs | s + t.amount) ELSE 0 END as total_amount,
             CASE WHEN SIZE(txs) > 0 THEN 
                  CASE WHEN SIZE(txs) = 0 THEN 0 
                  ELSE REDUCE(s = 0, t IN txs | s + t.amount) / SIZE(txs) 
                  END
             ELSE 0 END as avg_amount
        
        // Calculate pattern flags with defensive coding
        WITH a, txs, tx_count, total_amount, avg_amount,
             CASE WHEN avg_amount > 10000 THEN true ELSE false END as high_value,
             CASE WHEN tx_count > 10 AND total_amount > 0 AND 
                  total_amount/tx_count > 5000 THEN true ELSE false END as tx_anomaly,
               // Other pattern detections
             CASE WHEN SIZE([t IN txs WHERE round(t.amount) = t.amount 
                  AND t.amount >= 1000]) > 1 THEN true ELSE false END as round_pattern,
             CASE WHEN a.burst_pattern IS NOT NULL AND a.burst_pattern = true THEN true ELSE false END as burst_pattern,
             CASE WHEN a.chain_pattern IS NOT NULL AND a.chain_pattern = true THEN true ELSE false END as chain_pattern,
             CASE WHEN a.funnel_pattern IS NOT NULL AND a.funnel_pattern = true THEN true ELSE false END as funnel_pattern
        
        // Calculate model2 score with pattern weights
        WITH a, high_value, tx_anomaly, round_pattern, burst_pattern, chain_pattern, funnel_pattern,
             (CASE 
                WHEN chain_pattern AND funnel_pattern THEN 0.80
                WHEN chain_pattern AND round_pattern THEN 0.75
                WHEN funnel_pattern AND round_pattern THEN 0.70
                WHEN chain_pattern THEN 0.65
                WHEN funnel_pattern THEN 0.60
                WHEN round_pattern THEN 0.55
                ELSE 0.0
              END) * 0.60 +
             
             // Behavioral indicators
             (CASE
                WHEN tx_anomaly AND high_value THEN 0.85
                WHEN tx_anomaly THEN 0.65
                WHEN high_value THEN 0.55
                WHEN burst_pattern THEN 0.50
                ELSE 0.0
              END) * 0.40
        AS model2_score
        
        SET a.model2_score = model2_score,
            a.high_value_tx = high_value,
            a.tx_anomaly = tx_anomaly
        
        RETURN COUNT(a) AS processed_count,
               SUM(CASE WHEN model2_score > 0 THEN 1 ELSE 0 END) as nonzero_count,
               AVG(model2_score) as avg_score
    """

    MODEL3_INTEGRATION_QUERY = """
        MATCH (a:Account)
        WITH a,
            COALESCE(a.model1_score, 0) AS network_score,
            COALESCE(a.model2_score, 0) AS behavior_score,
            COALESCE(a.temporal_density, 0) AS temporal_density,
            COALESCE(a.step_variance, 0) AS step_variance,
            COALESCE(a.has_burst_pattern, false) AS has_burst,
            COALESCE(a.high_confidence_pattern, false) AS high_confidence
        
        WITH a, network_score, behavior_score, temporal_density, 
             step_variance, has_burst, high_confidence,
             
            // Calculate temporal risk score
            CASE
                WHEN has_burst AND temporal_density > 2 AND step_variance < 1.0 
                THEN 0.90  // Strong temporal pattern
                WHEN has_burst AND temporal_density > 1.5 
                THEN 0.75  // Moderate temporal pattern
                WHEN temporal_density > 2 OR step_variance < 0.5
                THEN 0.60  // Weak temporal pattern
                ELSE 0.30
            END AS temporal_risk
        
        WITH a, 
            // Weighted combination of all signals
            CASE
                // High confidence cases
                WHEN high_confidence AND network_score > 0.8 AND behavior_score > 0.8
                THEN (network_score * 0.35 + behavior_score * 0.45 + temporal_risk * 0.20) * 1.2
                
                // Strong pattern combinations
                WHEN network_score > 0.7 AND behavior_score > 0.7 AND temporal_risk > 0.7
                THEN (network_score * 0.35 + behavior_score * 0.40 + temporal_risk * 0.25) * 1.1
                
                // Normal cases
                ELSE (network_score * 0.30 + behavior_score * 0.35 + temporal_risk * 0.35)
            END AS model3_score
        
        SET a.model3_score = 
            CASE
                WHEN model3_score > 0.95 THEN 0.95  // Cap maximum score
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
        
        // Update account properties with velocity metrics
        SET a.high_velocity = true,
            a.velocity_ratio = velocity_ratio,
            a.fraud_score =
                CASE
                    WHEN a.fraud_score IS NULL 
                        THEN 0.35 + (velocity_ratio * 0.3)
                    WHEN a.fraud_score < 0.9 
                        THEN a.fraud_score + (velocity_ratio * 0.15)
                    ELSE a.fraud_score
                END
        
        RETURN COUNT(a) AS accounts_analyzed,
               AVG(velocity_ratio) AS avg_velocity_ratio,
               SUM(CASE WHEN velocity_ratio > 0.5 THEN 1 ELSE 0 END) AS high_velocity_accounts
    """

    # Final risk score queries
    FINAL_RISK_SCORE_QUERY = """
        MATCH (a:Account)
        WITH a,
             COALESCE(a.model1_score, 0.0) as model1_score,
             COALESCE(a.model2_score, 0.0) as model2_score,
             COALESCE(a.model3_score, 0.0) as model3_score,
             COALESCE(a.temporal_risk_score, 0.0) as temporal_score,
             COALESCE(a.velocity_risk_score, 0.0) as velocity_score
             
        // Calculate final fraud score with combined model weights
        WITH a,
             model1_score * 0.30 +     // Network structure model (30%)
             model2_score * 0.35 +     // Behavioral patterns model (35%)
             model3_score * 0.35       // Complex pattern model (35%)
             as ensemble_score
        
        // Apply minimum score for those with high model1 or model2 score
        WITH a, ensemble_score,
            CASE
                WHEN ensemble_score < 0.4 AND (
                    COALESCE(a.model1_score, 0) > 0.7 OR 
                    COALESCE(a.model2_score, 0) > 0.7
                ) THEN 0.4
                ELSE ensemble_score
            END as adjusted_score
        
        // Set final fraud score and risk level
        SET a.fraud_score = adjusted_score,
            a.risk_level = 
                CASE
                    WHEN adjusted_score >= $veryHighRisk THEN 'VERY_HIGH_RISK'
                    WHEN adjusted_score >= $highRisk THEN 'HIGH_RISK'
                    WHEN adjusted_score >= $suspicious THEN 'SUSPICIOUS'
                    ELSE 'NORMAL'
                END
        
        RETURN COUNT(a) as updated_accounts,
               SUM(CASE WHEN adjusted_score >= $fraudThreshold THEN 1 ELSE 0 END) as fraud_accounts,
               AVG(adjusted_score) as avg_score,
               MIN(adjusted_score) as min_score,
               MAX(adjusted_score) as max_score
    """

    ENSEMBLE_SCORE_QUERY = """
        MATCH (a:Account)
        WITH a,
             // Directly use model scores from previous steps
             COALESCE(a.model1_score, 0) * 0.3 +
             COALESCE(a.model2_score, 0) * 0.35 +
             COALESCE(a.model3_score, 0) * 0.35 as base_score,
               // Add bonus for accounts with multiple flags
             CASE WHEN (
                 (CASE WHEN a.high_value_tx IS NOT NULL AND a.high_value_tx = true THEN 1 ELSE 0 END) +
                 (CASE WHEN a.tx_anomaly IS NOT NULL AND a.tx_anomaly = true THEN 1 ELSE 0 END) +
                 (CASE WHEN a.round_pattern IS NOT NULL AND a.round_pattern = true THEN 1 ELSE 0 END) +
                 (CASE WHEN a.chain_pattern IS NOT NULL AND a.chain_pattern = true THEN 1 ELSE 0 END) +
                 (CASE WHEN a.funnel_pattern IS NOT NULL AND a.funnel_pattern = true THEN 1 ELSE 0 END) +
                 (CASE WHEN a.burst_pattern IS NOT NULL AND a.burst_pattern = true THEN 1 ELSE 0 END)
             ) >= 2 THEN 0.2 ELSE 0 END as pattern_bonus
        
        WITH a, base_score, pattern_bonus,
             // Apply base floor for accounts with at least one high risk flag
             CASE WHEN (a.isFraud IS NOT NULL AND a.isFraud = 1) OR 
                      (a.high_value_tx IS NOT NULL AND a.high_value_tx = true) OR
                      (a.chain_pattern IS NOT NULL AND a.chain_pattern = true)
                  THEN 0.4
                  ELSE 0
             END as minimum_score
        
        // Calculate final ensemble score
        WITH a, 
             CASE WHEN base_score + pattern_bonus > minimum_score
                  THEN base_score + pattern_bonus
                  ELSE minimum_score
             END as ensemble_score
        
        SET a.ensemble_score = ensemble_score
        
        RETURN COUNT(a) as scored_accounts,
               COUNT(CASE WHEN ensemble_score >= $fraudThreshold THEN 1 END) as fraud_accounts,
               AVG(ensemble_score) as avg_score,
               MAX(ensemble_score) as max_score
    """