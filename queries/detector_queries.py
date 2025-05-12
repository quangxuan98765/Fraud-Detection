class FraudDetectorQueries:
    # High-risk account marking query
    def mark_high_risk_accounts(self, threshold):
        return f"""
            MATCH (a:Account)
            WHERE 
                (a.fraud_score > {threshold}) OR  // Use configured threshold
                (a.risk_level = 'very_high') OR
                (a.risk_level = 'high')
            SET a.high_risk = true,
                a.risk_factors = CASE WHEN a.fraud_score > {threshold} THEN ['high_fraud_score'] ELSE [] END
                    + CASE WHEN a.model1_score > 0.6 THEN ['network_structure'] ELSE [] END
                    + CASE WHEN a.model2_score > 0.6 THEN ['behavior_patterns'] ELSE [] END
                    + CASE WHEN a.model3_score > 0.6 THEN ['complex_patterns'] ELSE [] END
                    + CASE WHEN a.tx_anomaly = true THEN ['transaction_anomaly'] ELSE [] END
                    + CASE WHEN a.cycle_boost > 0.2 THEN ['suspicious_cycle'] ELSE [] END
                    + CASE WHEN a.potential_mule = true THEN ['money_mule'] ELSE [] END
                    + CASE WHEN a.high_confidence_pattern = true THEN ['high_confidence_pattern'] ELSE [] END
                    + CASE WHEN a.similar_to_fraud = true THEN ['similar_to_fraud'] ELSE [] END
                    + CASE WHEN a.funnel_disperse_pattern = true THEN ['funnel_disperse'] ELSE [] END
                    + CASE WHEN a.round_tx_pattern = true THEN ['round_transactions'] ELSE [] END
                    + CASE WHEN a.increasing_chain = true THEN ['increasing_chain'] ELSE [] END
                    + CASE WHEN a.high_velocity = true THEN ['high_velocity'] ELSE [] END
        """
    
    # Detection effectiveness validation query
    VALIDATION_QUERY = """
        MATCH (a:Account)
        WHERE a.high_risk = true
        
        WITH collect(DISTINCT a.risk_factors) AS all_factors,
             count(DISTINCT a) AS flagged_accounts,
             count(DISTINCT CASE WHEN size(a.risk_factors) > 1 THEN a END) AS multi_factor,
             count(DISTINCT CASE WHEN 'high_fraud_score' IN a.risk_factors THEN a END) AS fraud_score,
             count(DISTINCT CASE WHEN 'transaction_anomaly' IN a.risk_factors THEN a END) AS tx_anomaly,
             count(DISTINCT CASE WHEN 'suspicious_cycle' IN a.risk_factors THEN a END) AS cycles,
             count(DISTINCT CASE WHEN 'network_structure' IN a.risk_factors THEN a END) AS network,
             count(DISTINCT CASE WHEN 'behavior_patterns' IN a.risk_factors THEN a END) AS behavior,
             count(DISTINCT CASE WHEN 'complex_patterns' IN a.risk_factors THEN a END) AS complex_patterns,
             count(DISTINCT CASE WHEN 'money_mule' IN a.risk_factors THEN a END) AS mules,
             count(DISTINCT CASE WHEN 'high_confidence_pattern' IN a.risk_factors THEN a END) AS high_conf,
             count(DISTINCT CASE WHEN 'similar_to_fraud' IN a.risk_factors THEN a END) AS similar,
             count(DISTINCT CASE WHEN 'funnel_disperse' IN a.risk_factors THEN a END) AS funnel,
             count(DISTINCT CASE WHEN 'round_transactions' IN a.risk_factors THEN a END) AS round_tx,
             count(DISTINCT CASE WHEN 'increasing_chain' IN a.risk_factors THEN a END) AS inc_chain,
             count(DISTINCT CASE WHEN 'high_velocity' IN a.risk_factors THEN a END) AS velocity
        RETURN 
            flagged_accounts,
            multi_factor,
            1.0 * multi_factor / flagged_accounts AS multi_factor_ratio,
            fraud_score, tx_anomaly, cycles, network, behavior, complex_patterns,
            mules, high_conf, similar, funnel, round_tx, inc_chain, velocity
    """
    
    # Performance metrics query
    PERFORMANCE_METRICS_QUERY = """
        MATCH (a:Account)
        WITH
            count(a) AS total_accounts,
            count(CASE WHEN a.is_fraud = true THEN a END) AS actual_fraud,
            count(CASE WHEN a.fraud_score > 0.5 THEN a END) AS detected_05,
            count(CASE WHEN a.fraud_score > 0.6 THEN a END) AS detected_06,
            count(CASE WHEN a.fraud_score > 0.7 THEN a END) AS detected_07,
            count(CASE WHEN a.is_fraud = true AND a.fraud_score > 0.5 THEN a END) AS true_pos_05,
            count(CASE WHEN a.is_fraud = true AND a.fraud_score > 0.6 THEN a END) AS true_pos_06,
            count(CASE WHEN a.is_fraud = true AND a.fraud_score > 0.7 THEN a END) AS true_pos_07
        
        RETURN 
            total_accounts,
            actual_fraud,
            detected_05, detected_06, detected_07,
            true_pos_05, true_pos_06, true_pos_07,
            
            // Calculate precision, recall, F1 at different thresholds
            CASE WHEN detected_05 > 0 THEN 1.0 * true_pos_05 / detected_05 ELSE 0 END AS precision_05,
            CASE WHEN detected_06 > 0 THEN 1.0 * true_pos_06 / detected_06 ELSE 0 END AS precision_06,
            CASE WHEN detected_07 > 0 THEN 1.0 * true_pos_07 / detected_07 ELSE 0 END AS precision_07,
            
            CASE WHEN actual_fraud > 0 THEN 1.0 * true_pos_05 / actual_fraud ELSE 0 END AS recall_05,
            CASE WHEN actual_fraud > 0 THEN 1.0 * true_pos_06 / actual_fraud ELSE 0 END AS recall_06,
            CASE WHEN actual_fraud > 0 THEN 1.0 * true_pos_07 / actual_fraud ELSE 0 END AS recall_07
    """
    
    # Clearing old analysis data
    CLEAR_ANALYSIS_DATA_QUERY = """
        MATCH (a:Account) 
        REMOVE a.fraud_score, a.community, a.pagerank_score, 
            a.degree_score, a.similarity_score, a.path_score, a.suspected_fraud,
            a.base_score, a.tx_anomaly, a.high_tx_volume, a.only_sender,
            a.potential_mule, a.mule_boost, a.temporal_boost, a.rapid_transfer,
            a.model1_score, a.model2_score, a.model3_score, a.ensemble_score,
            a.optimized_score, a.confidence_level, a.feature_importance,
            a.high_confidence_pattern, a.round_tx_pattern, a.funnel_disperse_pattern,
            a.increasing_chain, a.similar_to_fraud, a.high_velocity, a.velocity_ratio,
            a.calibrated_score, a.risk_level
    """
    
    # Clearing old relationships
    CLEAR_RELATIONSHIPS_QUERY = "MATCH ()-[r:SIMILAR_TO]->() DELETE r"
    
    # Create index for Account
    CREATE_INDEX_QUERY = "CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.id)"
    
    # Check for existing projected graph
    CHECK_PROJECTED_GRAPH_QUERY = """
        CALL gds.graph.list()
        YIELD graphName
        WHERE graphName = 'fraud_graph'
        RETURN count(*) > 0 AS exists
    """
    
    # Drop projected graph
    DROP_PROJECTED_GRAPH_QUERY = "CALL gds.graph.drop('fraud_graph', false)"
    
    # Create optimized projected graph
    CREATE_PROJECTED_GRAPH_QUERY = """
        CALL gds.graph.project(
            'fraud_graph',
            'Account',
            {
                SENT: {
                    type: 'SENT',
                    orientation: 'UNDIRECTED',
                    properties: {
                        amount: {property: 'amount', defaultValue: 0.0}
                    }
                }
            }
        )
    """
    
    # Degree Centrality with weights
    DEGREE_CENTRALITY_QUERY = """
        CALL gds.degree.write('fraud_graph', {
            relationshipWeightProperty: 'amount',
            writeProperty: 'degree_score'
        })
    """
    
    # PageRank with optimized parameters
    PAGERANK_QUERY = """
        CALL gds.pageRank.write('fraud_graph', {
            maxIterations: 30,
            dampingFactor: 0.85,
            relationshipWeightProperty: 'amount',
            writeProperty: 'pagerank_score'
        })
    """
    
    # Optimized Community Detection
    COMMUNITY_DETECTION_QUERY = """
        CALL gds.louvain.write('fraud_graph', {
            relationshipWeightProperty: 'amount',
            maxLevels: 10,
            maxIterations: 20,
            tolerance: 0.0001,
            writeProperty: 'community'
        })
    """
    
    # Node Similarity optimization
    NODE_SIMILARITY_QUERY = """
        CALL gds.nodeSimilarity.write('fraud_graph', {
            writeRelationshipType: 'SIMILAR_TO',
            writeRelationshipProperty: 'similarity',
            topK: 15,
            similarityCutoff: 0.5
        })
    """