import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.ticker import PercentFormatter

def plot_fraud_distribution(metrics, title='Phân phối tỷ lệ phát hiện gian lận'):
    """Vẽ biểu đồ phân phối tỷ lệ phát hiện gian lận."""
    # Chuẩn bị dữ liệu từ kết quả metrics
    labels = ['True Positives', 'False Positives', 'False Negatives', 'True Negatives']
    values = [
        metrics['true_positives'], 
        metrics['false_positives'],
        metrics['false_negatives'],
        metrics['true_negatives']
    ]
    
    # Tạo biểu đồ bánh
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7))
    
    # Biểu đồ bánh cho tổng quát
    colors = ['#ff9999','#ffcc99','#99ff99','#66b3ff']
    explode = (0.1, 0, 0, 0)  # Tách mảnh True Positives
    
    ax1.pie(values, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
    ax1.set_title('Phân phối dự đoán')
    
    # Biểu đồ cột cho các chỉ số hiệu suất
    performance = ['Precision', 'Recall', 'F1 Score', 'Accuracy']
    perf_values = [
        metrics['precision'],
        metrics['recall'],
        metrics['f1_score'],
        metrics['accuracy'] if 'accuracy' in metrics else (metrics['true_positives'] + metrics['true_negatives']) / 
        (metrics['true_positives'] + metrics['true_negatives'] + metrics['false_positives'] + metrics['false_negatives'])
    ]
    
    # Plot bar chart
    bars = ax2.bar(performance, perf_values, color=['#ff9999', '#ffcc99', '#99ff99', '#66b3ff'])
    ax2.set_ylim(0, 1.0)
    ax2.set_ylabel('Giá trị')
    ax2.set_title('Chỉ số hiệu suất')
    
    # Add values above bars
    for bar in bars:
        height = bar.get_height()
        ax2.annotate(f'{height:.3f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')
    
    fig.suptitle(title, fontsize=16)
    plt.tight_layout()
    
    return fig

def plot_feature_importance(feature_weights, correlations=None, title='Tầm quan trọng của các đặc trưng'):
    """Vẽ biểu đồ tầm quan trọng của các đặc trưng."""
    # Chuẩn bị dữ liệu
    features = list(feature_weights.keys())
    weights = list(feature_weights.values())
    
    # Sort by weight
    sorted_indices = np.argsort(weights)[::-1]
    sorted_features = [features[i] for i in sorted_indices]
    sorted_weights = [weights[i] for i in sorted_indices]
    
    # Màu sắc gradient theo weights
    colors = plt.cm.YlOrRd(np.array(sorted_weights) / max(sorted_weights))
    
    # Tạo figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Vẽ biểu đồ
    bars = ax.barh(sorted_features, sorted_weights, color=colors)
    
    # Thêm nhãn
    ax.set_xlabel('Trọng số')
    ax.set_title(title)
    
    # Thêm giá trị trên các thanh
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax.text(width + 0.005, bar.get_y() + bar.get_height()/2, f'{sorted_weights[i]:.3f}',
                ha='left', va='center')
    
    # Nếu có dữ liệu tương quan, thêm biểu đồ tương quan
    if correlations:
        # Convert to list of tuples for sorting
        corr_tuples = [(f, c) for f, c in correlations.items()]
        sorted_corrs = sorted(corr_tuples, key=lambda x: abs(x[1]), reverse=True)
        corr_features = [x[0] for x in sorted_corrs]
        corr_values = [x[1] for x in sorted_corrs]
        
        # Create second figure for correlations
        fig2, ax2 = plt.subplots(figsize=(12, 8))
        
        # Color gradient based on absolute correlation
        colors2 = plt.cm.RdBu(0.5 + np.array(corr_values) / 2)
        
        # Plot horizontal bars
        bars2 = ax2.barh(corr_features, corr_values, color=colors2)
        
        # Add labels
        ax2.set_xlabel('Correlation with Fraud')
        ax2.set_title('Tương quan giữa các đặc trưng và gian lận')
        
        # Add values on bars
        for i, bar in enumerate(bars2):
            width = bar.get_width()
            label_x = width + 0.01 if width >= 0 else width - 0.01
            align = 'left' if width >= 0 else 'right'
            ax2.text(label_x, bar.get_y() + bar.get_height()/2, f'{corr_values[i]:.3f}',
                    ha=align, va='center')
        
        # Add a vertical line at x=0
        ax2.axvline(x=0, color='gray', linestyle='-', linewidth=0.8)
        
        # Set limits
        ax2.set_xlim(-1, 1)
        
        return fig, fig2
    
    return fig