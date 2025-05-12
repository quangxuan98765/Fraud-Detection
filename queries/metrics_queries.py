class MetricsQueries:
    CALCULATE_METRICS = """
        MATCH (a:Account)
        WITH
            count(a) AS total_accounts,
            count(CASE WHEN a.is_fraud = true THEN a END) AS actual_fraud,
            count(CASE WHEN a.fraud_score > 0.5 THEN a END) AS detected_high_risk,
            count(CASE WHEN a.is_fraud = true AND a.fraud_score > 0.5 THEN a END) AS true_positives,
            count(CASE WHEN a.is_fraud = false AND a.fraud_score > 0.5 THEN a END) AS false_positives,
            count(CASE WHEN a.is_fraud = true AND a.fraud_score <= 0.5 THEN a END) AS false_negatives
        
        RETURN 
            total_accounts,
            detected_high_risk,
            actual_fraud,
            true_positives,
            false_positives,
            false_negatives,
            
            // Calculate precision, recall and F1
            CASE WHEN detected_high_risk > 0 
                 THEN 1.0 * true_positives / detected_high_risk 
                 ELSE 0 END AS precision,
                 
            CASE WHEN actual_fraud > 0 
                 THEN 1.0 * true_positives / actual_fraud 
                 ELSE 0 END AS recall,
                 
            CASE WHEN (1.0 * true_positives / detected_high_risk + 1.0 * true_positives / actual_fraud) > 0
                 THEN 2.0 * (1.0 * true_positives / detected_high_risk) * (1.0 * true_positives / actual_fraud) / 
                      ((1.0 * true_positives / detected_high_risk) + (1.0 * true_positives / actual_fraud))
                 ELSE 0 END AS f1_score
    """
    
    THRESHOLD_METRICS = """
        MATCH (a:Account)
        WITH
            count(a) AS total_accounts,
            count(CASE WHEN a.is_fraud = true THEN a END) AS actual_fraud,
            count(CASE WHEN a.fraud_score > $threshold THEN a END) AS detected_high_risk,
            count(CASE WHEN a.is_fraud = true AND a.fraud_score > $threshold THEN a END) AS true_positives
        
        RETURN 
            detected_high_risk,
            1.0 * true_positives / detected_high_risk AS precision,
            1.0 * true_positives / actual_fraud AS recall,
            CASE WHEN (true_positives / detected_high_risk + true_positives / actual_fraud) > 0
                THEN 2.0 * (true_positives / detected_high_risk) * (true_positives / actual_fraud) / 
                     ((true_positives / detected_high_risk) + (true_positives / actual_fraud))
                ELSE 0 END AS f1_score
    """
    
    COMMUNITY_METRICS = """
        MATCH (a:Account)
        WHERE a.community IS NOT NULL
        WITH a.community AS community,
             count(a) AS community_size,
             count(CASE WHEN a.is_fraud = true THEN a END) AS fraud_count,
             count(CASE WHEN a.fraud_score > 0.5 THEN a END) AS flagged_count
        WHERE community_size > 2  // Focus on communities with at least 3 members
        
        RETURN count(community) AS community_count,
               sum(CASE WHEN 1.0 * fraud_count / community_size > 0.3 THEN 1 ELSE 0 END) AS high_fraud_communities,
               sum(CASE WHEN 1.0 * flagged_count / community_size > 0.3 THEN 1 ELSE 0 END) AS high_risk_communities,
               avg(community_size) AS avg_community_size,
               avg(1.0 * fraud_count / community_size) AS avg_fraud_ratio,
               avg(1.0 * flagged_count / community_size) AS avg_flagged_ratio
    """