class AccountQueries:
    # Account behavior queries
    GET_ACCOUNT_COUNT = "MATCH (a:Account) RETURN count(a) AS count"
    
    ACCOUNT_BEHAVIOR_QUERY = """
        MATCH (a:Account)
        WITH a ORDER BY id(a)
        SKIP $skip LIMIT $limit
        
        WITH a, 
            COALESCE(a.out_count, 0) AS outCount, 
            COALESCE(a.in_count, 0) AS inCount,
            COALESCE(a.out_amount, 0) AS outAmount,
            COALESCE(a.in_amount, 0) AS inAmount
            
        // Calculate transaction count and amount differences
        WITH a, outCount, inCount, outAmount, inAmount,
             CASE 
                 WHEN outCount >= inCount THEN outCount - inCount
                 ELSE inCount - outCount
             END AS txDiff,
             CASE 
                 WHEN outAmount >= inAmount THEN outAmount - inAmount
                 ELSE inAmount - outAmount
             END AS amountDiff,
             CASE 
                 WHEN outAmount + inAmount > 0 THEN
                     CASE 
                         WHEN outAmount >= inAmount THEN outAmount / (outAmount + inAmount)
                         ELSE inAmount / (outAmount + inAmount)
                     END
                 ELSE 0 
             END AS dominance_ratio
        
        // Set imbalance metrics with stricter thresholds and ratio analysis
        SET a.tx_imbalance = CASE 
                WHEN outCount + inCount < 5 THEN 0  // Increased minimum transaction requirement
                WHEN outCount + inCount = 0 THEN 0
                WHEN dominance_ratio > 0.85 THEN txDiff / (outCount + inCount) * 1.2  // Boost score for dominant direction
                ELSE txDiff / (outCount + inCount)
            END,
            a.amount_imbalance = CASE
                WHEN outAmount + inAmount = 0 THEN 0
                WHEN outAmount + inAmount < 100000 THEN 0  // Increased threshold
                WHEN dominance_ratio > 0.85 THEN amountDiff / (outAmount + inAmount) * 1.2  // Boost score for dominant direction
                ELSE amountDiff / (outAmount + inAmount)
            END,
            a.only_sender = CASE 
                WHEN outCount >= 3 AND inCount = 0 THEN true  // Lowered threshold from 4
                ELSE false 
            END,
            a.high_tx_volume = CASE 
                WHEN outCount + inCount > 8 THEN true  // Lowered threshold from 10
                ELSE false 
            END,
            // New metric: Direction bias - indicates strong directional preference
            a.direction_bias = CASE
                WHEN outCount + inCount >= 5 THEN
                    CASE 
                        WHEN outCount > inCount * 3 THEN 'outgoing'
                        WHEN inCount > outCount * 3 THEN 'incoming'
                        ELSE null
                    END
                ELSE null
            END,
            // New metric: Flow ratio - relationship between in and out flow
            a.flow_ratio = CASE
                WHEN inAmount > 0 THEN outAmount / inAmount
                ELSE 
                    CASE WHEN outAmount > 0 THEN 99.0 ELSE 0 END
            END
            
        RETURN count(*) as processed
    """
    
    RAPID_TURNOVER_QUERY = """
        MATCH (a:Account)
        WHERE a.in_amount > 0 AND a.out_amount > 0
        WITH a,
             a.out_amount AS outAmount,
             a.in_amount AS inAmount,
             a.out_count AS outCount,
             a.in_count AS inCount
        WITH a, outAmount, inAmount, outCount, inCount,
             CASE 
                 WHEN outCount >= inCount THEN outCount - inCount
                 ELSE inCount - outCount
             END AS txDiff
        WHERE 
            // High proportion of funds moving through account
            outAmount > inAmount * 0.8 AND 
            inAmount > 50000 AND
            // Activity on both sides
            outCount >= 2 AND inCount >= 2
        SET a.rapid_turnover = true,
            a.turnover_ratio = outAmount / inAmount,
            a.transaction_diff = txDiff
    """
    
    STRUCTURING_PATTERNS_QUERY = """
        MATCH (a:Account)-[tx:SENT]->(b:Account)
        WITH a, b, count(tx) AS txCount, sum(tx.amount) AS totalAmount, avg(tx.amount) AS avgAmount
        WHERE txCount >= 5 AND  // Multiple transactions
              avgAmount < 10000 AND  // Small average amount
              totalAmount > 30000    // But significant total
        WITH a, collect(DISTINCT b) AS recipients, count(DISTINCT b) AS recipientCount
        WHERE recipientCount <= 3  // Small number of recipients (concentrated)
        SET a.potential_structuring = true
    """
    
    # Transaction anomaly queries
    CLEAR_TX_ANOMALY_FLAGS = "MATCH (a:Account) REMOVE a.tx_anomaly"
    
    TRANSACTION_ANOMALIES_QUERY = """
        MATCH (a:Account)
        WITH a ORDER BY id(a)
        SKIP $skip LIMIT $limit
        
        WITH a
        OPTIONAL MATCH (a)-[tx:SENT]->()
        WITH a, avg(tx.amount) AS avgAmount, max(tx.amount) AS maxAmount, count(tx) AS txCount,
            collect(tx.amount) as amounts
        WHERE txCount >= 2 AND (  // Lowered minimum transaction count from 3
            // Lower threshold for suspicious transaction detection
            (maxAmount > avgAmount * 7 AND maxAmount > 70000 AND txCount >= 3) OR
            // Lower threshold for high-value transactions
            (maxAmount > 180000 AND avgAmount < 35000 AND txCount >= 2) OR
            // Requiring multiple anomalous transactions but with lower thresholds
            (size([x in amounts WHERE x > avgAmount * 4]) >= 2 AND  // Lowered from 5x and 3 occurrences
            txCount >= 3 AND  // Lowered from 4
            maxAmount > avgAmount * 5) OR  // Lowered from 6
            // More transactions with high value with lower thresholds
            (size([x in amounts WHERE x > 70000]) >= 2 AND  // Lowered from 80000 and 3 occurrences
            avgAmount < 22000 AND  // Lowered from 25000
            txCount >= 2)  // Lowered from 3
        )                    
        SET a.tx_anomaly = true
        RETURN count(*) as processed
    """
    
    HIGH_VALUE_TRANSACTIONS_QUERY = """
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
            // Lower threshold for transaction value
            max_amount > 90000 AND
            // Lower imbalance threshold
            max_amount > avg_amount * 3.5 AND
            // Require fewer high-value transactions
            size([x IN amounts WHERE x > 70000]) >= 2 AND
            // Require fewer transactions overall
            num_transactions >= 2
        SET a.high_value_tx = true
    """
    
    ROUND_NUMBER_TRANSACTIONS_QUERY = """
        MATCH (a:Account)-[tx:SENT]->()
        WHERE tx.amount % 10000 = 0 AND tx.amount >= 50000
        WITH a, count(tx) AS roundTxCount
        WHERE roundTxCount >= 2
        SET a.round_number_tx = true,
            a.round_tx_count = roundTxCount
    """
    
    HIGH_RISK_TARGETS_QUERY = """
        MATCH (sender:Account)-[:SENT]->(receiver:Account)
        WHERE receiver.high_risk = true OR receiver.tx_anomaly = true
        WITH sender, count(DISTINCT receiver) AS highRiskTargets
        WHERE highRiskTargets >= 2
        SET sender.targets_high_risk = true,
            sender.high_risk_target_count = highRiskTargets
    """
    
    # Account neighborhood queries
    CONCENTRATED_TRANSFERS_QUERY = """
        MATCH (a:Account)-[:SENT]->(b:Account)
        WITH a, count(DISTINCT b) AS outNeighbors
        MATCH (a)-[out:SENT]->()
        WITH a, outNeighbors, count(out) AS totalOut
        WHERE totalOut > 5 AND outNeighbors <= 2  // Many transfers to few accounts
        SET a.concentrated_out = true,
            a.concentration_ratio = 1.0 * totalOut / outNeighbors
    """
    
    FUNNEL_PATTERN_QUERY = """
        MATCH (a:Account)
        WHERE EXISTS((a)-[:SENT]->()) AND EXISTS((a)<-[:SENT]-())
        
        MATCH (a)<-[:SENT]-(inNeighbor)
        WITH a, count(DISTINCT inNeighbor) AS inNeighborCount
        
        MATCH (a)-[:SENT]->(outNeighbor)
        WITH a, inNeighborCount, count(DISTINCT outNeighbor) AS outNeighborCount
        
        WHERE inNeighborCount >= 4 AND outNeighborCount <= 2
        AND inNeighborCount > outNeighborCount * 2
        
        SET a.funnel_pattern = true,
            a.funnel_ratio = 1.0 * inNeighborCount / 
                             CASE WHEN outNeighborCount = 0 THEN 1 ELSE outNeighborCount END
    """
    
    FAN_OUT_PATTERN_QUERY = """
        MATCH (a:Account)
        WHERE EXISTS((a)-[:SENT]->()) AND EXISTS((a)<-[:SENT]-())
        
        MATCH (a)<-[:SENT]-(inNeighbor)
        WITH a, count(DISTINCT inNeighbor) AS inNeighborCount
        
        MATCH (a)-[:SENT]->(outNeighbor)
        WITH a, inNeighborCount, count(DISTINCT outNeighbor) AS outNeighborCount
        
        WHERE outNeighborCount >= 4 AND inNeighborCount <= 2
        AND outNeighborCount > inNeighborCount * 2
        
        SET a.fan_out_pattern = true,
            a.fan_out_ratio = 1.0 * outNeighborCount / 
                             CASE WHEN inNeighborCount = 0 THEN 1 ELSE inNeighborCount END
    """
    
    # Temporal pattern queries
    CHECK_TIMESTAMPS_QUERY = """
        MATCH ()-[r:SENT]->() 
        WHERE r.timestamp IS NOT NULL 
        RETURN count(r) > 0 AS has_timestamps
    """
    
    BURST_ACTIVITY_QUERY = """
        MATCH (a:Account)-[tx:SENT]->()
        WHERE tx.timestamp IS NOT NULL
        WITH a, tx.timestamp AS timestamp
        ORDER BY a, timestamp
        WITH a, collect(timestamp) AS timestamps
        WHERE size(timestamps) >= 3
        
        // Calculate time differences between consecutive transactions
        WITH a, 
             [i IN range(0, size(timestamps)-2) | 
              duration.between(timestamps[i], timestamps[i+1]).hours] AS timeDiffs
              
        // Check if there are multiple transactions within 24 hours
        WHERE size([x IN timeDiffs WHERE x <= 24]) >= 2
        
        SET a.burst_activity = true
    """
    
    UNUSUAL_HOURS_QUERY = """
        MATCH (a:Account)-[tx:SENT]->()
        WHERE tx.timestamp IS NOT NULL
        
        // Extract hour of day from timestamp
        WITH a, tx, datetime({epochmillis: datetime.epochmillis(tx.timestamp)}).hour AS hour
        
        // Count transactions by hour
        WITH a, hour, count(tx) AS txCount
        
        // Flag accounts with high night activity (12AM-5AM)
        WITH a, collect({hour: hour, count: txCount}) AS hourCounts,
             sum(CASE WHEN hour >= 0 AND hour < 5 THEN txCount ELSE 0 END) AS nightCount,
             sum(txCount) AS totalCount
             
        WHERE totalCount >= 4 AND 1.0 * nightCount / totalCount > 0.4
        
        SET a.unusual_hours = true,
            a.night_tx_ratio = 1.0 * nightCount / totalCount
    """
    
    # Complex pattern queries
    LAYERING_PATTERN_QUERY = """
        MATCH p=(origin:Account)-[:SENT*2..4]->(destination:Account)
        WHERE 
            // Ensure total path has meaningful volume 
            reduce(total = 0, r in relationships(p) | total + r.amount) > 80000 AND
            
            // Check for close timing if timestamps available
            all(i in range(0, size(relationships(p))-1) WHERE 
                NOT EXISTS(relationships(p)[i].timestamp) OR
                NOT EXISTS(relationships(p)[i+1].timestamp) OR
                duration.between(relationships(p)[i].timestamp, 
                             relationships(p)[i+1].timestamp).hours <= 48)
        
        WITH p, nodes(p) AS accounts
        
        // Apply to all accounts in the path
        UNWIND accounts AS account
        WITH DISTINCT account
        
        SET account.layering_pattern = true
    """
    
    COMBINED_RISK_INDICATORS_QUERY = """
        MATCH (a:Account)
        WHERE (a.tx_anomaly = true OR a.high_value_tx = true) AND
              (a.tx_imbalance > 0.5 OR a.amount_imbalance > 0.5) AND
              (a.fan_out_pattern = true OR a.funnel_pattern = true OR 
               a.concentrated_out = true OR a.rapid_turnover = true)
        
        SET a.multi_indicator_risk = true,
            a.risk_level = CASE
                WHEN a.tx_anomaly = true AND a.tx_imbalance > 0.7 AND 
                     (a.funnel_pattern = true OR a.rapid_turnover = true) THEN 'high'
                ELSE 'medium'
            END
    """
    
    SUSPICIOUS_COMMUNITIES_QUERY = """
        MATCH (a:Account)
        WHERE a.community IS NOT NULL
        WITH a.community AS community, 
             count(a) AS communitySize,
             count(CASE WHEN a.tx_anomaly = true THEN a END) AS anomalies,
             count(CASE WHEN a.high_value_tx = true THEN a END) AS highValueTx,
             count(CASE WHEN a.potential_structuring = true THEN a END) AS structuring,
             count(CASE WHEN a.layering_pattern = true THEN a END) AS layering
        WHERE 
            // Small to medium sized communities with high proportion of suspicious activity
            communitySize >= 3 AND communitySize <= 20 AND
            (1.0 * (anomalies + highValueTx + structuring + layering) / communitySize >= 0.3)
            
        // Mark all accounts in suspicious communities
        MATCH (a:Account)
        WHERE a.community = community
        SET a.suspicious_community = true,
            a.community_risk_factors = 
                CASE WHEN anomalies > 0 THEN ['anomalies'] ELSE [] END +
                CASE WHEN highValueTx > 0 THEN ['high_value'] ELSE [] END +
                CASE WHEN structuring > 0 THEN ['structuring'] ELSE [] END +
                CASE WHEN layering > 0 THEN ['layering'] ELSE [] END
    """