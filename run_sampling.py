import os
from data_processing import network_preserving_sampling

# Define input and output paths
input_path = r"D:\Fraud-Detection\uploads\paysim1.csv"  # Replace with your actual dataset path
output_folder = r"D:\Fraud-Detection\uploads"
output_filename = "filtered_paysim1.csv"

# Create output directory if it doesn't exist
os.makedirs(output_folder, exist_ok=True)
output_path = os.path.join(output_folder, output_filename)

# Run the sampling with the desired fraud rate (1.291%)
result_path = network_preserving_sampling(
    input_path=input_path,
    output_path=output_path,
    target_nodes=200000,
    target_edges=100000,  # Adjusted to get closer to your 96k relationships
    target_fraud_rate=0.01291  # 1.291% fraud rate
)

print(f"Sampling complete! Sampled dataset saved to: {result_path}")