from config import (FRAUD_SCORE_THRESHOLD, SUSPICIOUS_THRESHOLD, HIGH_RISK_THRESHOLD,
                 VERY_HIGH_RISK_THRESHOLD, ROUND_AMOUNT_MIN, 
                 CHAIN_TIME_WINDOW, MIN_CHAIN_LENGTH, FUNNEL_MIN_SOURCES)
import time
from queries.pattern_queries import PatternQueries

class PatternDetector:
    def __init__(self, driver):
        self.driver = driver
        self.queries = PatternQueries()

    def analyze_fraud(self):
        with self.driver.session() as session:
            print("🔍 Detecting complex fraud patterns...")
            start_time = time.time()
            
            # First detect temporal patterns and bursts
            self.detect_temporal_patterns(session)
            
            # Then detect specialized transaction patterns
            self.detect_specialized_patterns(session)

            # Analyze transaction velocity and bursts
            self.analyze_transaction_velocity(session)

            # Calculate initial scores using improved ensemble
            self.calculate_fraud_scores()

            # Apply final calibration with improved thresholds
            self.calculate_final_risk_score(session)
            
            print(f"✅ Pattern detection completed in {time.time() - start_time:.2f} seconds")
            return True

    def detect_temporal_patterns(self, session):
        """Phân tích các mẫu thời gian dựa trên step"""
        print("Analyzing temporal patterns in transactions...")
        
        temporal_queries = [
            self.queries.BURST_PATTERN_QUERY,
            self.queries.TRANSACTION_CYCLE_QUERY,
            self.queries.AMOUNT_CHAIN_QUERY,
            self.queries.HOUR_DISTRIBUTION_QUERY
        ]
        
        print("\nAnalyzing temporal patterns:")
        for i, query in enumerate(temporal_queries, 1):
            result = session.run(query).single()
            if result:
                count = list(result.values())[0]
                pattern_types = ["Burst patterns", "Transaction cycles", 
                               "Amount chains", "Hour distribution"]
                print(f"  Pattern {i} ({pattern_types[i-1]}): {count} accounts")

        # Update risk scores based on temporal patterns
        session.run(self.queries.UPDATE_TEMPORAL_RISK_QUERY)
        
        return True

    def calculate_fraud_scores(self):
        """Calculate optimized fraud scores with temporal patterns and improved weighting"""
        with self.driver.session() as session:
            print("Calculating optimized fraud scores with temporal analysis...")
            
            # Execute all models
            print("  Running Model 1 (Network Structure)...")
            result1 = session.run(self.queries.MODEL1_NETWORK_QUERY).single()
            processed1 = result1["processed_count"] if result1 else 0
            print(f"  ✓ Scored {processed1} accounts with Model 1")
            
            print("  Running Model 2 (Behavioral Patterns)...")
            result2 = session.run(self.queries.MODEL2_BEHAVIOR_QUERY).single()
            processed2 = result2["processed_count"] if result2 else 0
            print(f"  ✓ Scored {processed2} accounts with Model 2")
            
            print("  Running Model 3 (Complex Integration)...")
            result3 = session.run(self.queries.MODEL3_INTEGRATION_QUERY).single()
            processed3 = result3["processed_count"] if result3 else 0
            print(f"  ✓ Scored {processed3} accounts with Model 3")

            # Final ensemble combination
            print("  Calculating final ensemble scores...")
            self._calculate_ensemble_scores(session)

            print("  Detecting high-confidence fraud patterns...")
            high_confidence_result = session.run(self.queries.HIGH_CONFIDENCE_PATTERN_QUERY).single()
            high_confidence_count = high_confidence_result.get("high_confidence_accounts", 0) if high_confidence_result else 0
            print(f"  ✓ Found {high_confidence_count} high-confidence fraud patterns")
                    
            return True

    def detect_specialized_patterns(self, session):
        """Detect complex transaction patterns indicating potential fraud"""
        print("Detecting specialized transaction patterns...")
        
        pattern_queries = [
            self.queries.CHAIN_PATTERN_QUERY,
            self.queries.FUNNEL_PATTERN_QUERY,
            self.queries.ROUND_AMOUNT_QUERY
        ]

        params = {
            "chainTimeWindow": CHAIN_TIME_WINDOW,
            "minChainLength": MIN_CHAIN_LENGTH,
            "funnelMinSources": FUNNEL_MIN_SOURCES,
            "roundAmountMin": ROUND_AMOUNT_MIN
        }

        print("\nAnalyzing specialized patterns:")
        pattern_types = ["Chain patterns", "Funnel patterns", "Round amounts"]
        
        for i, query in enumerate(pattern_queries):
            try:
                result = session.run(query, params).single()
                if result:
                    count = list(result.values())[0]
                    print(f"  {pattern_types[i]}: {count} accounts")
            except Exception as e:
                print(f"Error detecting {pattern_types[i]}: {str(e)}")
                continue

        # Update risk scores based on detected patterns
        self._calculate_pattern_risk_scores(session)
        return True

    def _calculate_pattern_risk_scores(self, session):
        """Calculate risk scores based on detected patterns"""
        session.run(self.queries.PATTERN_RISK_SCORE_QUERY, {
            "minChainLength": MIN_CHAIN_LENGTH,
            "funnelMinSources": FUNNEL_MIN_SOURCES
        })

    def analyze_transaction_velocity(self, session):
        """Analyze the velocity of transactions to identify burst patterns"""
        print("Analyzing transaction velocity patterns...")
        
        # Check if we have timestamp data
        has_timestamps = session.run(self.queries.CHECK_TIMESTAMPS_QUERY).single()

        # Execute velocity analysis
        velocity_result = session.run(self.queries.VELOCITY_ANALYSIS_QUERY).single()
        velocity_accounts = velocity_result["high_velocity_accounts"] if velocity_result else 0
        print(f"  Found {velocity_accounts} accounts with high transaction velocity")
        
        return velocity_accounts

    def calculate_final_risk_score(self, session):
        """Calculate final risk scores using ensemble approach"""
        print("Calculating final risk scores...")

        result = session.run(self.queries.FINAL_RISK_SCORE_QUERY, {
            "veryHighRisk": VERY_HIGH_RISK_THRESHOLD,
            "highRisk": HIGH_RISK_THRESHOLD,
            "suspicious": SUSPICIOUS_THRESHOLD,
            "fraudThreshold": FRAUD_SCORE_THRESHOLD
        }).single()
        
        if result:
            print(f"\nRisk Score Summary:")
            print(f"  Total accounts analyzed: {result['updated_accounts']}")
            print(f"  Accounts above fraud threshold: {result['fraud_accounts']}")
            print(f"  Score range: {result['min_score']:.3f} - {result['max_score']:.3f}")
            print(f"  Average risk score: {result['avg_score']:.3f}")
        
        return True

    def _calculate_ensemble_scores(self, session):
        """Calculate ensemble scores using the ensemble model"""
        try:
            result = session.run(self.queries.ENSEMBLE_SCORE_QUERY, {
                "fraudThreshold": FRAUD_SCORE_THRESHOLD
            }).single()
            
            print("\nEnsemble Score Summary:")
            if result and result['scored_accounts'] > 0:
                print(f"  Total accounts processed: {result['scored_accounts']}")
                print(f"  Accounts above fraud threshold: {result['fraud_accounts']}")
                if result['avg_score'] is not None:
                    print(f"  Average score: {result['avg_score']:.3f}")
                    print(f"  Maximum score: {result['max_score']:.3f}")
                else:
                    print("  Average score: N/A")
            else:
                print("  No accounts were scored")
                
            return True
            
        except Exception as e:
            print(f"Error calculating ensemble scores: {str(e)}")
            return False