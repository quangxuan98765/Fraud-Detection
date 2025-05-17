#!/usr/bin/env python3
"""
Feature importance analysis for fraud detection.
This script analyzes the importance of different features in identifying fraudulent transactions.
"""

import os
import sys
import time
import json
import argparse
import matplotlib.pyplot as plt
import numpy as np

# Add root directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from detector.database_manager import DatabaseManager
from detector.utils.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class FeatureImportanceAnalyzer:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.features = [
            'degScore', 'prScore', 'simScore', 'btwScore', 'hubScore', 
            'authScore', 'coreScore', 'triCount', 'cycleCount', 
            'tempBurst', 'txVelocity', 'amountVolatility', 
            'maxAmountRatio', 'stdTimeBetweenTx', 'normCommunitySize'
        ]
        
    def analyze_feature_importance(self):
        """Analyze the importance of features in fraud detection."""
        print("🔄 Analyzing feature importance...")
          # Calculate feature importance based on correlation with fraud
        # The features are stored on Account nodes, not on SENT relationships
        query = """
        MATCH (sender:Account)-[tx:SENT]->()
        WHERE tx.ground_truth_fraud IS NOT NULL
        RETURN 
            tx.ground_truth_fraud AS is_fraud,
            tx.anomaly_score AS anomaly_score,
            sender.degScore AS degScore,
            sender.prScore AS prScore,
            sender.simScore AS simScore,
            sender.btwScore AS btwScore,
            sender.hubScore AS hubScore,
            sender.authScore AS authScore,
            sender.coreScore AS coreScore,
            sender.triCount AS triCount,
            sender.cycleCount AS cycleCount,
            sender.tempBurst AS tempBurst,
            sender.txVelocity AS txVelocity,
            sender.amountVolatility AS amountVolatility,
            sender.maxAmountRatio AS maxAmountRatio,
            sender.stdTimeBetweenTx AS stdTimeBetweenTx,
            sender.normCommunitySize AS normCommunitySize
        """
        
        # Execute query
        try:
            with self.db_manager.driver.session() as session:
                result = session.run(query).data()
                
            if not result:
                print("⚠️ No data returned for feature importance analysis.")
                return None
                  # Separate fraud and non-fraud records
            fraud_records = [r for r in result if r["is_fraud"] == True]
            non_fraud_records = [r for r in result if r["is_fraud"] == False]
            
            if not fraud_records or not non_fraud_records:
                print("⚠️ Not enough data for comparison (need both fraud and non-fraud records).")
                print(f"Found {len(fraud_records)} fraud records and {len(non_fraud_records)} non-fraud records.")
                return None
            
            print(f"✅ Found {len(fraud_records)} fraud records and {len(non_fraud_records)} non-fraud records for analysis.")
                  # Calculate mean values for each feature in fraud and non-fraud groups
            # Add debugging to check feature values
            print("\nChecking feature availability:")
            for feature in self.features:
                fraud_values = [r.get(feature, None) for r in fraud_records]
                non_fraud_values = [r.get(feature, None) for r in non_fraud_records]
                fraud_valid = [v for v in fraud_values if v is not None]
                non_fraud_valid = [v for v in non_fraud_values if v is not None]
                print(f"  • {feature}: {len(fraud_valid)}/{len(fraud_records)} fraud records, {len(non_fraud_valid)}/{len(non_fraud_records)} non-fraud records have valid values")
            
            fraud_means = {}
            non_fraud_means = {}
            for feature in self.features:
                # Get non-None values for calculation
                fraud_values = [r.get(feature, 0) for r in fraud_records if r.get(feature) is not None]
                non_fraud_values = [r.get(feature, 0) for r in non_fraud_records if r.get(feature) is not None]
                
                if fraud_values:
                    fraud_means[feature] = np.mean(fraud_values)
                else:
                    fraud_means[feature] = 0
                    
                if non_fraud_values:
                    non_fraud_means[feature] = np.mean(non_fraud_values)
                else:
                    non_fraud_means[feature] = 0
            
            # Calculate importance as the absolute difference between means
            importance = {}
            for feature in self.features:
                fraud_mean = fraud_means.get(feature, 0)
                non_fraud_mean = non_fraud_means.get(feature, 0)
                
                # Calculate various difference metrics
                abs_diff = abs(fraud_mean - non_fraud_mean)
                
                # Avoid division by zero
                if non_fraud_mean != 0:
                    relative_diff = abs(fraud_mean / non_fraud_mean - 1)
                else:
                    relative_diff = 0 if fraud_mean == 0 else 1
                
                # Combined score gives weight to both absolute and relative differences
                importance[feature] = (abs_diff * 0.3) + (relative_diff * 0.7)
            
            # Normalize importance scores
            max_importance = max(importance.values()) if importance else 1.0
            normalized_importance = {f: (v / max_importance) for f, v in importance.items()}
            
            # Sort features by importance
            sorted_features = sorted(normalized_importance.items(), key=lambda x: x[1], reverse=True)
            
            # Print results
            print("\n📊 Feature Importance Analysis:")
            for feature, score in sorted_features:
                print(f"  • {feature}: {score:.4f}")
                
            # Save results
            result = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "feature_importance": dict(sorted_features),
                "fraud_means": fraud_means,
                "non_fraud_means": non_fraud_means
            }
            
            with open('feature_importance_analysis.json', 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
                
            print("\n✅ Saved feature importance analysis to feature_importance_analysis.json")
            
            # Plot feature importance
            self._plot_feature_importance(dict(sorted_features))
            
            return result
            
        except Exception as e:
            print(f"❌ Error analyzing feature importance: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
            
    def _plot_feature_importance(self, importance):
        """Plot feature importance."""
        try:
            # Sort features by importance
            sorted_items = sorted(importance.items(), key=lambda x: x[1])
            features = [item[0] for item in sorted_items]
            scores = [item[1] for item in sorted_items]
            
            # Create horizontal bar chart
            plt.figure(figsize=(12, 8))
            y_pos = np.arange(len(features))
            plt.barh(y_pos, scores, align='center', alpha=0.8)
            plt.yticks(y_pos, features)
            plt.xlabel('Normalized Importance')
            plt.title('Feature Importance for Fraud Detection')
            plt.tight_layout()
            
            # Save the figure
            plt.savefig('feature_importance.png')
            print("✅ Saved feature importance chart to feature_importance.png")
            
        except Exception as e:
            print(f"⚠️ Could not generate feature importance plot: {str(e)}")
            
    def calculate_feature_weights(self):
        """Calculate optimized feature weights based on importance analysis."""
        print("🔄 Calculating optimized feature weights...")
        
        # First analyze feature importance
        analysis = self.analyze_feature_importance()
        if not analysis:
            print("⚠️ Could not calculate optimized weights due to missing feature importance.")
            return None
            
        # Get normalized importance values
        importance = analysis["feature_importance"]
        
        # Apply some domain expertise to adjust raw importance
        # Higher importance for known fraud indicators
        adjusted_importance = {}
        for feature, score in importance.items():
            if feature in ['tempBurst', 'maxAmountRatio', 'txVelocity', 'amountVolatility']:
                # Boost temporal and amount features that are typically important for fraud
                adjusted_importance[feature] = score * 1.5
            elif feature in ['degScore', 'hubScore', 'cycleCount']:
                # These network structure features are also important
                adjusted_importance[feature] = score * 1.2
            else:
                adjusted_importance[feature] = score
                
        # Normalize adjusted importance to sum to 1.0
        total = sum(adjusted_importance.values())
        weights = {feature: score/total for feature, score in adjusted_importance.items()} if total > 0 else adjusted_importance
        
        # Print optimized weights
        print("\n📊 Optimized Feature Weights:")
        for feature, weight in sorted(weights.items(), key=lambda x: x[1], reverse=True):
            print(f"  • {feature}: {weight:.4f}")
            
        # Save optimized weights
        result = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "optimized_weights": weights,
            "raw_importance": importance
        }
        
        with open('optimized_feature_weights.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
            
        print("\n✅ Saved optimized feature weights to optimized_feature_weights.json")
        
        # Generate Python code for updated weights
        code = "# Optimized feature weights based on data analysis\n"
        code += "OPTIMIZED_WEIGHTS = {\n"
        for feature, weight in sorted(weights.items(), key=lambda x: x[0]):
            code += f"    '{feature}': {weight:.4f},\n"
        code += "}\n"
        
        with open('optimized_weights.py', 'w', encoding='utf-8') as f:
            f.write(code)
            
        print("✅ Generated Python code with optimized weights in optimized_weights.py")
        
        return weights


def main():
    parser = argparse.ArgumentParser(description='Analyze feature importance for fraud detection')
    
    parser.add_argument('--calculate-weights', action='store_true',
                       help='Calculate optimized feature weights')
    
    args = parser.parse_args()
    
    # Neo4j connection
    try:
        db_manager = DatabaseManager(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        analyzer = FeatureImportanceAnalyzer(db_manager)
        
        if args.calculate_weights:
            analyzer.calculate_feature_weights()
        else:
            analyzer.analyze_feature_importance()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
