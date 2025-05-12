from config import FRAUD_SCORE_THRESHOLD, SUSPICIOUS_THRESHOLD, HIGH_RISK_THRESHOLD

class PatternDetector:
    def __init__(self, driver):
        self.driver = driver
        
    def calculate_fraud_scores(self):
        """Calculate optimized fraud scores using ensemble methods and improved weighting"""
        with self.driver.session() as session:
            print("Calculating optimized fraud scores with multi-model approach...")
            
            # 1. Clear old optimized scores
            session.run("""
                MATCH (a:Account)
                REMOVE a.optimized_score, a.confidence_level, a.feature_importance,
                      a.model1_score, a.model2_score, a.model3_score, a.ensemble_score
            """)
            
            # 2. Identify highest-confidence fraud patterns
            session.run("""
                MATCH (s:Account)-[r:SENT]->(t:Account)
                WHERE r.amount > 150000 AND 
                    (
                        // Strong structural indicators
                        (s.pagerank_score > 0.6 AND t.pagerank_score > 0.6) OR
                        // Strong behavioral indicators  
                        (s.tx_anomaly = true AND t.tx_anomaly = true AND r.amount > 180000) OR
                        // Strong imbalance indicators
                        (s.tx_imbalance > 0.8 AND t.tx_imbalance > 0.8)
                    )
                SET s.high_confidence_pattern = true,
                    t.high_confidence_pattern = true
            """)

            # 3. MODEL 1: Feature-weighted scoring - favors network metrics
            model1_query = """
                MATCH (a:Account)
                
                WITH a, 
                    COALESCE(a.pagerank_score, 0) AS pagerank,
                    COALESCE(a.degree_score, 0) AS degree,
                    COALESCE(a.similarity_score, 0) AS similarity,
                    COALESCE(a.tx_imbalance, 0) AS imbalance,
                    COALESCE(a.amount_imbalance, 0) AS amount_imbalance,
                    COALESCE(a.high_tx_volume, false) AS high_volume,
                    COALESCE(a.tx_anomaly, false) AS anomaly,
                    COALESCE(a.high_value_tx, false) AS high_value,
                    COALESCE(a.potential_fraud, false) AS potential_fraud,
                    COALESCE(a.potential_mule, false) AS potential_mule,
                    COALESCE(a.suspected_fraud, false) AS suspected_fraud,
                    COALESCE(a.cycle_boost, 0) AS cycle_boost,
                    COALESCE(a.relation_boost, 0) AS relation_boost,
                    COALESCE(a.high_confidence_pattern, false) AS high_confidence
                
                // Model 1: Weight network structure heavily
                WITH a, 
                    // Network metrics (45% weight)
                    (pagerank * 0.20 +
                     degree * 0.10 + 
                     similarity * 0.15) * 0.45 +
                    
                    // Transaction patterns (35% weight)
                    (CASE 
                        WHEN imbalance > 0.85 THEN 0.25
                        WHEN imbalance > 0.75 THEN 0.20
                        WHEN imbalance > 0.65 THEN 0.15
                        WHEN imbalance > 0.55 THEN 0.10
                        ELSE imbalance * 0.08
                     END +
                     CASE 
                        WHEN amount_imbalance > 0.85 THEN 0.10
                        WHEN amount_imbalance > 0.75 THEN 0.08
                        WHEN amount_imbalance > 0.65 THEN 0.06
                        WHEN amount_imbalance > 0.55 THEN 0.04
                        ELSE amount_imbalance * 0.03
                     END) * 0.35 +
                    
                    // Behavioral flags (20% weight)
                    (CASE WHEN anomaly THEN 0.10 ELSE 0 END +
                     CASE WHEN high_volume THEN 0.05 ELSE 0 END +
                     CASE WHEN high_value THEN 0.06 ELSE 0 END +
                     CASE WHEN potential_fraud THEN 0.40 ELSE 0 END +
                     CASE WHEN potential_mule THEN 0.30 ELSE 0 END +
                     CASE WHEN suspected_fraud THEN 0.35 ELSE 0 END +
                     CASE WHEN high_confidence THEN 0.50 ELSE 0 END +
                     cycle_boost * 0.50 +
                     relation_boost * 0.40) * 0.20 AS model1_score
                
                SET a.model1_score = model1_score
                RETURN count(a) AS processed_count
            """
            
            # 4. MODEL 2: Feature-weighted scoring - favors behavioral patterns
            model2_query = """
                MATCH (a:Account)
                
                WITH a, 
                    COALESCE(a.pagerank_score, 0) AS pagerank,
                    COALESCE(a.degree_score, 0) AS degree,
                    COALESCE(a.similarity_score, 0) AS similarity,
                    COALESCE(a.tx_imbalance, 0) AS imbalance,
                    COALESCE(a.amount_imbalance, 0) AS amount_imbalance,
                    COALESCE(a.high_tx_volume, false) AS high_volume,
                    COALESCE(a.tx_anomaly, false) AS anomaly,
                    COALESCE(a.high_value_tx, false) AS high_value,
                    COALESCE(a.potential_fraud, false) AS potential_fraud,
                    COALESCE(a.potential_mule, false) AS potential_mule,
                    COALESCE(a.suspected_fraud, false) AS suspected_fraud,
                    COALESCE(a.cycle_boost, 0) AS cycle_boost,
                    COALESCE(a.relation_boost, 0) AS relation_boost,
                    COALESCE(a.high_confidence_pattern, false) AS high_confidence,
                    COALESCE(a.concentrated_out, false) AS concentrated_out,
                    COALESCE(a.funnel_pattern, false) AS funnel_pattern,
                    COALESCE(a.fan_out_pattern, false) AS fan_out_pattern
                
                // Model 2: Weight behavioral patterns heavily
                WITH a, 
                    // Network metrics (25% weight)
                    (pagerank * 0.08 +
                     degree * 0.07 + 
                     similarity * 0.10) * 0.25 +
                    
                    // Transaction patterns (20% weight)
                    (CASE 
                        WHEN imbalance > 0.80 THEN 0.18
                        WHEN imbalance > 0.70 THEN 0.15
                        WHEN imbalance > 0.60 THEN 0.12
                        WHEN imbalance > 0.50 THEN 0.08
                        ELSE imbalance * 0.07
                     END +
                     CASE 
                        WHEN amount_imbalance > 0.80 THEN 0.08
                        WHEN amount_imbalance > 0.70 THEN 0.06
                        WHEN amount_imbalance > 0.60 THEN 0.04
                        WHEN amount_imbalance > 0.50 THEN 0.02
                        ELSE amount_imbalance * 0.02
                     END) * 0.20 +
                    
                    // Behavioral flags (55% weight)
                    (CASE WHEN anomaly THEN 0.15 ELSE 0 END +
                     CASE WHEN high_volume THEN 0.08 ELSE 0 END +
                     CASE WHEN high_value THEN 0.12 ELSE 0 END +
                     CASE WHEN potential_fraud THEN 0.35 ELSE 0 END +
                     CASE WHEN potential_mule THEN 0.32 ELSE 0 END +
                     CASE WHEN suspected_fraud THEN 0.30 ELSE 0 END +
                     CASE WHEN high_confidence THEN 0.45 ELSE 0 END +
                     CASE WHEN concentrated_out THEN 0.18 ELSE 0 END +
                     CASE WHEN funnel_pattern THEN 0.22 ELSE 0 END +
                     CASE WHEN fan_out_pattern THEN 0.20 ELSE 0 END +
                     cycle_boost * 0.38 +
                     relation_boost * 0.35) * 0.55 AS model2_score
                
                SET a.model2_score = model2_score
                RETURN count(a) AS processed_count
            """
            
            # 5. MODEL 3: Graph pattern-based scoring
            model3_query = """
                MATCH (a:Account)
                
                WITH a, 
                    COALESCE(a.pagerank_score, 0) AS pagerank,
                    COALESCE(a.degree_score, 0) AS degree,
                    COALESCE(a.similarity_score, 0) AS similarity,
                    COALESCE(a.tx_imbalance, 0) AS imbalance,
                    COALESCE(a.amount_imbalance, 0) AS amount_imbalance,
                    COALESCE(a.high_tx_volume, false) AS high_volume,
                    COALESCE(a.tx_anomaly, false) AS anomaly,
                    COALESCE(a.high_value_tx, false) AS high_value,
                    COALESCE(a.potential_fraud, false) AS potential_fraud,
                    COALESCE(a.potential_mule, false) AS potential_mule,
                    COALESCE(a.suspected_fraud, false) AS suspected_fraud,
                    COALESCE(a.cycle_boost, 0) AS cycle_boost,
                    COALESCE(a.relation_boost, 0) AS relation_boost,
                    COALESCE(a.high_confidence_pattern, false) AS high_confidence,
                    COALESCE(a.layering_pattern, false) AS layering_pattern,
                    COALESCE(a.suspicious_community, false) AS suspicious_community
                
                // Model 3: Focus on complex graph patterns
                OPTIONAL MATCH path1 = (a)-[:SENT*1..2]->(:Account)
                WITH a, 
                     COUNT(DISTINCT path1) AS out_paths,
                     pagerank, degree, similarity, imbalance, amount_imbalance,
                     high_volume, anomaly, high_value, potential_fraud, potential_mule,
                     suspected_fraud, cycle_boost, relation_boost, high_confidence,
                     layering_pattern, suspicious_community
                
                OPTIONAL MATCH path2 = (:Account)-[:SENT*1..2]->(a)
                WITH a, 
                     out_paths,
                     COUNT(DISTINCT path2) AS in_paths,
                     pagerank, degree, similarity, imbalance, amount_imbalance,
                     high_volume, anomaly, high_value, potential_fraud, potential_mule,
                     suspected_fraud, cycle_boost, relation_boost, high_confidence,
                     layering_pattern, suspicious_community
                
                // Calculate path asymmetry (useful for detecting certain fraud patterns)
                WITH a, 
                     CASE 
                         WHEN out_paths + in_paths > 0 
                         THEN abs(out_paths - in_paths) / (out_paths + in_paths) 
                         ELSE 0 
                     END AS path_asymmetry,
                     pagerank, degree, similarity, imbalance, amount_imbalance,
                     high_volume, anomaly, high_value, potential_fraud, potential_mule,
                     suspected_fraud, cycle_boost, relation_boost, high_confidence,
                     layering_pattern, suspicious_community
                
                // Model 3: Weight complex patterns heavily
                WITH a, 
                    // Network structure (30% weight)
                    (pagerank * 0.15 +
                     degree * 0.10 + 
                     similarity * 0.10 +
                     path_asymmetry * 0.10) * 0.30 +
                    
                    // Transaction anomalies (15% weight)
                    (CASE 
                        WHEN imbalance > 0.80 THEN 0.20
                        WHEN imbalance > 0.70 THEN 0.16
                        WHEN imbalance > 0.60 THEN 0.12
                        WHEN imbalance > 0.50 THEN 0.08
                        ELSE imbalance * 0.08
                     END +
                     CASE WHEN anomaly THEN 0.15 ELSE 0 END) * 0.15 +
                    
                    // Complex patterns (55% weight)
                    (CASE WHEN high_confidence THEN 0.30 ELSE 0 END +
                     CASE WHEN layering_pattern THEN 0.25 ELSE 0 END +
                     CASE WHEN suspicious_community THEN 0.20 ELSE 0 END +
                     CASE WHEN potential_mule THEN 0.28 ELSE 0 END +
                     CASE WHEN suspected_fraud THEN 0.30 ELSE 0 END +
                     cycle_boost * 0.40 +
                     relation_boost * 0.30) * 0.55 AS model3_score
                
                SET a.model3_score = model3_score
                RETURN count(a) AS processed_count
            """

            # 6. Ensemble scoring - combine all models with confidence weighting
            ensemble_query = """
                MATCH (a:Account)
                WITH a,
                     COALESCE(a.model1_score, 0) AS m1_score,
                     COALESCE(a.model2_score, 0) AS m2_score,
                     COALESCE(a.model3_score, 0) AS m3_score
                  // Calculate variance between models as a confidence measure
                // Higher variance = lower confidence
                WITH a, m1_score, m2_score, m3_score,
                     (m1_score + m2_score + m3_score) / 3 AS avg_score,
                     SQRT(
                         ((m1_score - (m1_score + m2_score + m3_score) / 3) * (m1_score - (m1_score + m2_score + m3_score) / 3) +
                          (m2_score - (m1_score + m2_score + m3_score) / 3) * (m2_score - (m1_score + m2_score + m3_score) / 3) +
                          (m3_score - (m1_score + m2_score + m3_score) / 3) * (m3_score - (m1_score + m2_score + m3_score) / 3)) / 3
                     ) AS model_variance
                
                // Calculate confidence level (inverse of variance, normalized)
                WITH a, m1_score, m2_score, m3_score, avg_score, model_variance,
                     CASE 
                         WHEN model_variance > 0.1 THEN 0.3  // High variance = low confidence
                         WHEN model_variance > 0.05 THEN 0.6  // Medium variance = medium confidence
                         WHEN model_variance > 0.01 THEN 0.8  // Low variance = high confidence
                         ELSE 1.0  // Very low variance = very high confidence
                     END AS confidence_level
                
                // Identify most predictive model (closest to average)
                WITH a, m1_score, m2_score, m3_score, avg_score, model_variance, confidence_level,
                     CASE 
                         WHEN ABS(m1_score - avg_score) <= ABS(m2_score - avg_score) AND 
                              ABS(m1_score - avg_score) <= ABS(m3_score - avg_score) 
                         THEN 'Network Structure'
                         WHEN ABS(m2_score - avg_score) <= ABS(m1_score - avg_score) AND 
                              ABS(m2_score - avg_score) <= ABS(m3_score - avg_score) 
                         THEN 'Behavioral Patterns'
                         ELSE 'Complex Patterns'
                     END AS feature_importance
                
                // Final ensemble score: weighted average of models
                // If confidence is low, be more conservative (reduce score)
                WITH a, m1_score, m2_score, m3_score, confidence_level, feature_importance,
                     (m1_score * 0.30 + m2_score * 0.35 + m3_score * 0.35) * 
                     CASE 
                         WHEN confidence_level < 0.5 THEN confidence_level * 1.3  // Scale down more for low confidence
                         ELSE confidence_level
                     END AS ensemble_score
                
                // Set final scores and metadata
                SET a.optimized_score = ensemble_score,
                    a.confidence_level = confidence_level,
                    a.feature_importance = feature_importance,
                    a.ensemble_score = ensemble_score
                    
                RETURN 
                    count(a) AS processed_count,
                    sum(CASE WHEN ensemble_score > 0.7 THEN 1 ELSE 0 END) AS high_risk_count,
                    sum(CASE WHEN ensemble_score > 0.6 THEN 1 ELSE 0 END) AS medium_high_risk_count,
                    sum(CASE WHEN ensemble_score > 0.5 THEN 1 ELSE 0 END) AS medium_risk_count
            """
            
            # Execute all the model queries
            print("  Running Model 1 (Network Structure)...")
            result1 = session.run(model1_query).single()
            processed1 = result1["processed_count"] if result1 else 0
            print(f"  - Scored {processed1} accounts with Model 1")
            
            print("  Running Model 2 (Behavioral Patterns)...")
            result2 = session.run(model2_query).single()
            processed2 = result2["processed_count"] if result2 else 0
            print(f"  - Scored {processed2} accounts with Model 2")
            
            print("  Running Model 3 (Complex Patterns)...")
            result3 = session.run(model3_query).single()
            processed3 = result3["processed_count"] if result3 else 0
            print(f"  - Scored {processed3} accounts with Model 3")
            
            print("  Calculating ensemble scores...")
            ensemble_result = session.run(ensemble_query).single()
            if ensemble_result:
                total = ensemble_result["processed_count"]
                high = ensemble_result["high_risk_count"]
                medium_high = ensemble_result["medium_high_risk_count"]
                medium = ensemble_result["medium_risk_count"]
                
                print(f"  ✅ Ensemble scoring complete for {total} accounts")
                print(f"    - High risk (>0.7): {high}")
                print(f"    - Medium-high risk (>0.6): {medium_high}")
                print(f"    - Medium risk (>0.5): {medium}")
            
            # Update fraud_score to use optimized_score
            session.run("""
                MATCH (a:Account)
                WHERE a.optimized_score IS NOT NULL
                SET a.fraud_score = a.optimized_score
            """)
            
            return True
            
    def detect_specialized_patterns(self, session):
        """Detect patterns that are specifically designed to improve the precision of fraud detection"""
        print("Detecting specialized fraud patterns...")
        
        # 1. Check for round-amount transactions with high frequency
        round_amount_query = """
            MATCH (sender:Account)-[tx:SENT]->(receiver:Account)
            WHERE tx.amount % 10000 = 0 AND tx.amount >= 30000
            WITH sender, receiver, count(tx) AS round_count
            WHERE round_count >= 2
            
            WITH COLLECT(DISTINCT sender) + COLLECT(DISTINCT receiver) AS accounts
            UNWIND accounts AS account
            
            WITH DISTINCT account
            SET account.round_tx_pattern = true,
                account.fraud_score = CASE 
                    WHEN account.fraud_score IS NULL THEN 0.40
                    WHEN account.fraud_score < 0.85 THEN account.fraud_score + 0.10
                    ELSE account.fraud_score
                END
            
            RETURN count(account) AS accounts_with_round_tx
        """
        
        # 2. Detect "funnel and disperse" patterns (collect and distribute funds)
        funnel_disperse_query = """
            // Find accounts that receive from many sources and send to many destinations
            MATCH (account:Account)
            WHERE EXISTS((account)-[:SENT]->()) AND EXISTS((account)<-[:SENT]-())
            
            WITH account
            OPTIONAL MATCH (account)<-[in_tx:SENT]-(in_neighbor)
            WITH account, count(DISTINCT in_neighbor) AS in_count, sum(in_tx.amount) AS in_amount
            WHERE in_count >= 3
            
            OPTIONAL MATCH (account)-[out_tx:SENT]->(out_neighbor)
            WITH account, in_count, in_amount, count(DISTINCT out_neighbor) AS out_count, sum(out_tx.amount) AS out_amount
            WHERE out_count >= 3
            
            // Check if total amounts are similar (money passing through)
            WITH account, in_count, out_count,
                 ABS(in_amount - out_amount) / CASE WHEN in_amount > 0 THEN in_amount ELSE 1 END AS amount_diff_ratio
            WHERE amount_diff_ratio < 0.2  // Less than 20% difference
            
            SET account.funnel_disperse_pattern = true,
                account.fraud_score = CASE 
                    WHEN account.fraud_score IS NULL THEN 0.55
                    WHEN account.fraud_score < 0.85 THEN account.fraud_score + 0.15
                    ELSE account.fraud_score
                END
                
            RETURN count(account) AS accounts_with_funnel_disperse
        """
        
        # 3. Find chains with incrementally increasing amounts (layering pattern variation)
        increasing_chain_query = """
            MATCH path = (a:Account)-[tx1:SENT]->(b:Account)-[tx2:SENT]->(c:Account)
            WHERE tx2.amount > tx1.amount * 1.05 AND tx2.amount < tx1.amount * 1.5
                  AND tx1.amount > 20000
            
            WITH a, b, c
            SET b.increasing_chain = true,
                a.fraud_score = CASE 
                    WHEN a.fraud_score IS NULL THEN 0.35
                    WHEN a.fraud_score < 0.9 THEN a.fraud_score + 0.08
                    ELSE a.fraud_score
                END,
                b.fraud_score = CASE 
                    WHEN b.fraud_score IS NULL THEN 0.45
                    WHEN b.fraud_score < 0.9 THEN b.fraud_score + 0.12
                    ELSE b.fraud_score
                END,
                c.fraud_score = CASE 
                    WHEN c.fraud_score IS NULL THEN 0.40
                    WHEN c.fraud_score < 0.9 THEN c.fraud_score + 0.10
                    ELSE c.fraud_score
                END
                
            RETURN count(DISTINCT b) AS accounts_in_increasing_chains
        """
        
        # 4. Find accounts with strong similarity to confirmed fraud accounts
        fraud_similarity_query = """
            MATCH (a:Account)-[sim:SIMILAR_TO]->(b:Account)
            WHERE b.is_fraud = true AND sim.similarity > 0.7
            
            SET a.similar_to_fraud = true,
                a.fraud_score = CASE 
                    WHEN a.fraud_score IS NULL THEN 0.65
                    WHEN a.fraud_score < 0.9 THEN a.fraud_score + 0.20
                    ELSE a.fraud_score
                END
                
            RETURN count(a) AS accounts_similar_to_fraud
        """
        
        # Execute specialized pattern detection
        print("  Detecting round transaction patterns...")
        round_result = session.run(round_amount_query).single()
        round_accounts = round_result["accounts_with_round_tx"] if round_result else 0
        print(f"  - Found {round_accounts} accounts with round transaction patterns")
        
        print("  Detecting funnel and disperse patterns...")
        funnel_result = session.run(funnel_disperse_query).single()
        funnel_accounts = funnel_result["accounts_with_funnel_disperse"] if funnel_result else 0
        print(f"  - Found {funnel_accounts} accounts with funnel and disperse patterns")
        
        print("  Detecting increasing chain patterns...")
        chain_result = session.run(increasing_chain_query).single()
        chain_accounts = chain_result["accounts_in_increasing_chains"] if chain_result else 0
        print(f"  - Found {chain_accounts} accounts in increasing amount chains")
        
        print("  Detecting similarity to known fraud accounts...")
        sim_result = session.run(fraud_similarity_query).single()
        sim_accounts = sim_result["accounts_similar_to_fraud"] if sim_result else 0
        print(f"  - Found {sim_accounts} accounts similar to known fraud")
        
        return round_accounts + funnel_accounts + chain_accounts + sim_accounts
        
    def analyze_transaction_velocity(self, session):
        """Analyze the velocity of transactions to identify burst patterns"""
        print("Analyzing transaction velocity patterns...")
        
        # Check if we have timestamp data
        has_timestamps = session.run("""
            MATCH ()-[r:SENT]->() 
            WHERE r.timestamp IS NOT NULL 
            RETURN count(r) > 0 AS has_timestamps
        """).single()
        
        if not has_timestamps or not has_timestamps.get("has_timestamps", False):
            print("  No timestamp data available for velocity analysis")
            return 0
            
        # Analyze transaction velocity
        velocity_query = """
            MATCH (a:Account)-[tx:SENT]->()
            WHERE tx.timestamp IS NOT NULL
            
            WITH a, tx ORDER BY a, tx.timestamp
            WITH a, collect(tx) AS transactions
            WHERE size(transactions) >= 3
            
            // Calculate time differences between consecutive transactions
            WITH a, transactions,
                 [i IN range(0, size(transactions)-2) | 
                  duration.between(transactions[i].timestamp, 
                                  transactions[i+1].timestamp).hours] AS time_diffs
            
            // Calculate average time between transactions
            WITH a, transactions, time_diffs,
                 reduce(total = 0, t IN time_diffs | total + t) / size(time_diffs) AS avg_time_diff
            
            // Count rapid transactions (significantly faster than average)
            WITH a, transactions, time_diffs, avg_time_diff,
                 size([t IN time_diffs WHERE t < avg_time_diff * 0.5]) AS rapid_tx_count
            
            // Calculate transaction velocity score
            WITH a, size(transactions) AS tx_count, rapid_tx_count,
                 1.0 * rapid_tx_count / size(time_diffs) AS velocity_ratio
            
            WHERE velocity_ratio > 0.4  // At least 40% of transactions are rapid
            
            SET a.high_velocity = true,
                a.velocity_ratio = velocity_ratio,
                a.fraud_score = CASE 
                    WHEN a.fraud_score IS NULL THEN 0.35 + (velocity_ratio * 0.3)
                    WHEN a.fraud_score < 0.9 THEN a.fraud_score + (velocity_ratio * 0.15)
                    ELSE a.fraud_score
                END
                
            RETURN count(a) AS high_velocity_accounts
        """
        
        # Execute velocity analysis
        velocity_result = session.run(velocity_query).single()
        velocity_accounts = velocity_result["high_velocity_accounts"] if velocity_result else 0
        print(f"  Found {velocity_accounts} accounts with high transaction velocity")
        
        return velocity_accounts
        
    def calculate_final_risk_score(self, session):
        """Calculate final risk score using calibrated thresholds to improve precision"""
        print("Calculating final calibrated risk scores...")
        
        # Get basic statistics
        basic_stats = session.run("""
            MATCH (a:Account)
            WHERE a.fraud_score IS NOT NULL
            RETURN 
                min(a.fraud_score) AS min_score,
                max(a.fraud_score) AS max_score,
                avg(a.fraud_score) AS avg_score
        """).single()
        
        # Get percentiles separately
        percentiles = session.run("""
            MATCH (a:Account)
            WHERE a.fraud_score IS NOT NULL
            WITH a.fraud_score AS score
            ORDER BY score
            WITH COLLECT(score) AS scores
            WITH size(scores) AS count, scores
            RETURN
                scores[toInteger(count * 0.9) - 1] AS percentile_90,
                scores[toInteger(count * 0.95) - 1] AS percentile_95,
                scores[toInteger(count * 0.99) - 1] AS percentile_99
        """).single()
        
        # Combine results
        min_score = basic_stats.get("min_score", 0) if basic_stats else 0
        max_score = basic_stats.get("max_score", 1) if basic_stats else 1
        avg_score = basic_stats.get("avg_score", 0.5) if basic_stats else 0.5
        p90 = percentiles.get("percentile_90", 0.7) if percentiles else 0.7
        p95 = percentiles.get("percentile_95", 0.8) if percentiles else 0.8
        p99 = percentiles.get("percentile_99", 0.9) if percentiles else 0.9
        
        print(f"  Score distribution - Min: {min_score:.3f}, Avg: {avg_score:.3f}, Max: {max_score:.3f}")
        print(f"  Percentiles - P90: {p90:.3f}, P95: {p95:.3f}, P99: {p99:.3f}")
        
        # Calculate high-confidence thresholds based on distribution
        high_confidence_threshold = max(0.6, p90)
        very_high_confidence_threshold = max(0.7, p95)
        
        # Apply calibrated thresholds
        calibration_query = f"""
            MATCH (a:Account)
            WHERE a.fraud_score IS NOT NULL
            
            // Apply calibration
            SET a.calibrated_score = CASE
                // Upweight accounts with high confidence in multiple models
                WHEN a.confidence_level > 0.8 AND a.fraud_score > {p90} THEN
                    a.fraud_score * 1.15
                
                // Upweight accounts with specific high-risk patterns 
                WHEN (a.similar_to_fraud = true OR a.high_confidence_pattern = true OR
                      a.funnel_disperse_pattern = true) AND a.fraud_score > {p90} * 0.9 THEN
                    a.fraud_score * 1.10
                    
                // Downweight low-confidence high scores
                WHEN a.confidence_level < 0.5 AND a.fraud_score > {avg_score} THEN
                    a.fraud_score * 0.85
                    
                // Apply confidence-based adjustment to other scores
                ELSE a.fraud_score * (0.9 + (a.confidence_level * 0.2))
            END,
            
            // Set calibrated risk level
            a.risk_level = CASE
                WHEN a.calibrated_score > {very_high_confidence_threshold} THEN 'very_high'
                WHEN a.calibrated_score > {high_confidence_threshold} THEN 'high'
                WHEN a.calibrated_score > {avg_score} THEN 'medium'
                ELSE 'low'
            END
            
            RETURN
                count(a) AS processed,
                count(CASE WHEN a.risk_level = 'very_high' THEN a END) AS very_high,
                count(CASE WHEN a.risk_level = 'high' THEN a END) AS high,
                count(CASE WHEN a.risk_level = 'medium' THEN a END) AS medium,
                count(CASE WHEN a.risk_level = 'low' THEN a END) AS low
        """
        
        # Execute calibration
        cal_result = session.run(calibration_query).single()
        if cal_result:
            processed = cal_result.get("processed", 0)
            very_high = cal_result.get("very_high", 0)
            high = cal_result.get("high", 0)
            medium = cal_result.get("medium", 0)
            low = cal_result.get("low", 0)
            
            print(f"  ✅ Calibrated {processed} accounts:")
            print(f"    - Very high risk: {very_high} ({very_high/processed*100:.1f}%)")
            print(f"    - High risk: {high} ({high/processed*100:.1f}%)")
            print(f"    - Medium risk: {medium} ({medium/processed*100:.1f}%)")
            print(f"    - Low risk: {low} ({low/processed*100:.1f}%)")
        
        # Update fraud_score to use calibrated score
        session.run("""
            MATCH (a:Account)
            WHERE a.calibrated_score IS NOT NULL
            SET a.fraud_score = a.calibrated_score
        """)
    
        return True
