/**
 * Fixes for real-time metrics display in fraud detection system
 */

// Safety function to safely get DOM elements and handle null values
function safeGetElement(id) {
    const element = document.getElementById(id);
    if (!element) {
        console.warn(`Element with ID "${id}" not found in the DOM`);
    }
    return element;
}

// Safe update function for any element's text content
function safeUpdateText(id, value, defaultValue = "-") {
    const element = safeGetElement(id);
    if (element) {
        try {
            element.textContent = value || defaultValue;
        } catch (e) {
            console.error(`Error updating element ${id}:`, e);
        }
    }
}

// Safely update a progress bar element
function safeUpdateProgressBar(id, valuePercent) {
    const bar = safeGetElement(id);
    if (!bar) return;
    
    try {
        // Round to 1 decimal place
        const roundedValue = Math.round(valuePercent * 10) / 10;
        
        // Update bar properties
        bar.style.width = `${roundedValue}%`;
        bar.setAttribute('aria-valuenow', roundedValue);
        bar.textContent = `${roundedValue}%`;
        
        // Update color based on score
        bar.classList.remove('bg-success', 'bg-info', 'bg-warning', 'bg-danger');
        if (roundedValue >= 80) bar.classList.add('bg-success');
        else if (roundedValue >= 60) bar.classList.add('bg-info');
        else if (roundedValue >= 40) bar.classList.add('bg-warning');
        else bar.classList.add('bg-danger');
    } catch (e) {
        console.error(`Error updating progress bar ${id}:`, e);
    }
}

// Safely fetch metrics and update the UI
function fetchAndUpdateMetrics() {
    console.log("Fetching updated metrics from API...");
    fetch('/api/metrics')
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            console.log("Received metrics data:", data);
            if (data.has_data && data.metrics) {
                // Update basic metrics
                safeUpdateText('totalAccounts', data.metrics.accounts ? data.metrics.accounts.toLocaleString() : "0");
                safeUpdateText('totalTransactions', data.metrics.transactions ? data.metrics.transactions.toLocaleString() : "0");
                
                // Update fraud count and metrics
                const fraudCount = safeGetElement('fraudCount');
                if (fraudCount) {
                    const fraudCountValue = data.metrics.detected_fraud_transactions || 0;
                    const precision = data.metrics.precision || 0;
                    const recall = data.metrics.recall || 0;
                    const f1Score = data.metrics.f1_score || 0;
                    
                    try {
                        fraudCount.innerHTML = `
                            ${fraudCountValue.toLocaleString()} 
                            <div class="mt-2">
                                <small class="text-muted">Chính xác: ${(precision * 100).toFixed(1)}%</small><br>
                                <small class="text-muted">Recall: ${(recall * 100).toFixed(1)}%</small><br>
                                <small class="text-muted">F1-Score: ${(f1Score * 100).toFixed(1)}%</small>
                            </div>`;
                    } catch (e) {
                        console.error("Error updating fraud count:", e);
                        fraudCount.textContent = fraudCountValue.toLocaleString();
                    }
                }
                
                // Update high risk communities count
                safeUpdateText('highRiskCommunityCount', data.metrics.high_risk_communities || "0");
                
                // Update progress bars
                if (data.metrics.precision !== undefined) {
                    safeUpdateProgressBar('precision-bar', data.metrics.precision * 100);
                }
                if (data.metrics.recall !== undefined) {
                    safeUpdateProgressBar('recall-bar', data.metrics.recall * 100);
                }
                if (data.metrics.f1_score !== undefined) {
                    safeUpdateProgressBar('f1-bar', data.metrics.f1_score * 100);
                }
                  console.log("Metrics updated successfully");
                
                // Hide any error messages on successful load
                const errorAlert = document.querySelector('.metrics-error');
                if (errorAlert) {
                    errorAlert.classList.add('d-none');
                }
            } else {
                console.warn("No data available in metrics response");
                // Show no data available message
                safeUpdateText('totalAccounts', "-");
                safeUpdateText('totalTransactions', "-");
                safeUpdateText('fraudCount', "-");
                safeUpdateText('highRiskCommunityCount', "-");
            }
        })        .catch(error => {
            console.error('Error fetching metrics:', error);
            // Show error message
            safeUpdateText('totalAccounts', "Lỗi");
            safeUpdateText('totalTransactions', "Lỗi");
            safeUpdateText('fraudCount', "Lỗi");
            safeUpdateText('highRiskCommunityCount', "Lỗi");
            
            // Show the error alert
            const errorAlert = document.querySelector('.metrics-error');
            if (errorAlert) {
                errorAlert.classList.remove('d-none');
                errorAlert.querySelector('span').textContent = 
                    `Không thể tải dữ liệu metrics: ${error.message}. Vui lòng làm mới trang.`;
            }
        });
}

// Handle DOM ready events
document.addEventListener('DOMContentLoaded', function() {
    console.log("DOM loaded, initializing metrics fixes");
    
    // Try to fetch metrics immediately
    setTimeout(fetchAndUpdateMetrics, 500);
    
    // Add a refresh button to manually update metrics
    const metricsContainer = document.querySelector('.row.mb-4');
    if (metricsContainer) {
        const refreshButton = document.createElement('button');
        refreshButton.className = 'btn btn-sm btn-outline-primary mt-3';
        refreshButton.innerHTML = '<i class="fas fa-sync-alt me-1"></i> Làm mới dữ liệu';
        refreshButton.onclick = function() {
            // Hide any error messages when manually refreshing
            const errorAlert = document.querySelector('.metrics-error');
            if (errorAlert) {
                errorAlert.classList.add('d-none');
            }
            fetchAndUpdateMetrics();
        };
        
        const buttonContainer = document.createElement('div');
        buttonContainer.className = 'col-12 text-center';
        buttonContainer.appendChild(refreshButton);
        
        metricsContainer.insertAdjacentElement('afterend', buttonContainer);
    }
});

// Add a retry mechanism to handle race conditions in page loading
window.addEventListener('load', function() {
    console.log("Window loaded, attempting final metrics update");
    setTimeout(fetchAndUpdateMetrics, 1000);
});
