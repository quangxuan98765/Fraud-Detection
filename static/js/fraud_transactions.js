/**
 * Xử lý các chức năng liên quan đến hiển thị chi tiết giao dịch gian lận
 */
document.addEventListener('DOMContentLoaded', function() {
    let fraudChart = null;
    let currentTransactions = []; // Store current transactions for filtering

    // Khởi tạo biểu đồ
    function initFraudChart(data) {
        const ctx = document.getElementById('fraudTypeChart').getContext('2d');
        
        if (fraudChart) {
            fraudChart.destroy();
        }
        
        fraudChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: data.map(item => item.type || 'Khác'),
                datasets: [{
                    data: data.map(item => item.count),
                    backgroundColor: [
                        '#ff4136',  // Đỏ
                        '#ff851b',  // Cam
                        '#ffdc00',  // Vàng
                        '#2ecc40',  // Xanh lá
                        '#0074d9',  // Xanh dương
                        '#b10dc9'   // Tím
                    ]
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'right'
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const value = context.raw;
                                const amount = data[context.dataIndex].amount;
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = ((value / total) * 100).toFixed(1);
                                return [
                                    `${value} giao dịch (${percentage}%)`,
                                    `Tổng: ${new Intl.NumberFormat('vi-VN', { 
                                        style: 'currency', 
                                        currency: 'VND' 
                                    }).format(amount)}`
                                ];
                            }
                        }
                    }
                }
            }
        });
    }

    // Hiển thị bảng giao dịch với sorting và filtering
    function renderTransactionsTable(transactions, filters = {}) {
        const tbody = document.getElementById('fraudTransactionsTable');
        tbody.innerHTML = '';

        // Apply filters
        let filteredTransactions = transactions.filter(tx => {
            if (filters.minAmount && tx.amount < filters.minAmount) return false;
            if (filters.maxAmount && tx.amount > filters.maxAmount) return false;
            if (filters.minScore) {
                const avgScore = ((tx.source_score || 0) + (tx.target_score || 0)) / 2;
                if (avgScore < filters.minScore) return false;
            }
            if (filters.type && filters.type !== 'all' && tx.type !== filters.type) return false;
            if (filters.search) {
                const search = filters.search.toLowerCase();
                return tx.source.toLowerCase().includes(search) || 
                       tx.target.toLowerCase().includes(search) ||
                       tx.type.toLowerCase().includes(search);
            }
            return true;
        });

        // Apply sorting
        if (filters.sort) {
            const [field, direction] = filters.sort.split(':');
            filteredTransactions.sort((a, b) => {
                let valA, valB;
                switch (field) {
                    case 'amount':
                        valA = a.amount;
                        valB = b.amount;
                        break;
                    case 'score':
                        valA = ((a.source_score || 0) + (a.target_score || 0)) / 2;
                        valB = ((b.source_score || 0) + (b.target_score || 0)) / 2;
                        break;
                    case 'timestamp':
                        valA = new Date(a.timestamp);
                        valB = new Date(b.timestamp);
                        break;
                    default:
                        valA = a[field];
                        valB = b[field];
                }
                return direction === 'asc' ? 
                    (valA > valB ? 1 : -1) : 
                    (valA < valB ? 1 : -1);
            });
        }

        // Update stats with filtered transactions
        updateStats(filteredTransactions);

        filteredTransactions.forEach(tx => {
            const row = document.createElement('tr');
            row.classList.add('fraud-transaction-row');
            
            // Tính toán điểm rủi ro trung bình
            const avgScore = ((tx.source_score || 0) + (tx.target_score || 0)) / 2;
            
            // Xác định màu cho điểm rủi ro
            let scoreClass = avgScore > 0.7 ? 'high' : 
                           avgScore > 0.5 ? 'medium' : 'low';
            
            // Format thời gian
            const timestamp = new Date(tx.timestamp);
            const formattedTime = timestamp.toLocaleString('vi-VN');
            
            row.innerHTML = `
                <td>
                    <strong>${tx.source}</strong>
                    <br>
                    <span class="fraud-risk-score ${getScoreClass(tx.source_score)}">
                        ${(tx.source_score || 0).toFixed(2)}
                    </span>
                </td>
                <td>
                    <strong>${tx.target}</strong>
                    <br>
                    <span class="fraud-risk-score ${getScoreClass(tx.target_score)}">
                        ${(tx.target_score || 0).toFixed(2)}
                    </span>
                </td>
                <td>
                    <strong class="fraud-amount">${new Intl.NumberFormat('vi-VN', { 
                        style: 'currency', 
                        currency: 'VND' 
                    }).format(tx.amount)}</strong>
                    ${tx.is_fraud ? 
                        '<span class="badge bg-danger ms-2">Đã xác nhận</span>' : ''}
                </td>
                <td><span class="badge bg-secondary fraud-transaction-badge">${tx.type || 'Khác'}</span></td>
                <td>
                    <span class="fraud-risk-score ${scoreClass}">
                        ${avgScore.toFixed(2)}
                    </span>
                </td>
                <td>
                    <a href="#" class="fraud-community-link" 
                       onclick="showCommunityDetails('${tx.source_community}'); return false;">
                        #${tx.source_community}
                    </a>
                    ${tx.source_community !== tx.target_community ? 
                        `<i class="fas fa-arrow-right mx-1"></i>
                         <a href="#" class="fraud-community-link"
                            onclick="showCommunityDetails('${tx.target_community}'); return false;">
                             #${tx.target_community}
                         </a>` 
                        : ''}
                </td>
                <td>
                    <span class="fraud-timestamp">${formattedTime}</span>
                </td>
            `;
            
            tbody.appendChild(row);
        });

        // Update UI with filtered count
        const totalEl = document.getElementById('fraudTotalTransactions');
        if (filteredTransactions.length !== transactions.length) {
            totalEl.innerHTML = `${filteredTransactions.length.toLocaleString()} <small class="text-muted">(filtered from ${transactions.length})</small>`;
        } else {
            totalEl.textContent = transactions.length.toLocaleString();
        }
    }

    // Helper function to get score class
    function getScoreClass(score) {
        return score > 0.7 ? 'high' :
               score > 0.5 ? 'medium' : 'low';
    }

    // Cập nhật thống kê
    function updateStats(transactions) {
        const total = transactions.length;
        const totalAmount = transactions.reduce((sum, tx) => sum + (tx.amount || 0), 0);
        const avgScore = transactions.reduce((sum, tx) => {
            const txScore = ((tx.source_score || 0) + (tx.target_score || 0)) / 2;
            return sum + txScore;
        }, 0) / (total || 1);

        const confirmedFraud = transactions.filter(tx => tx.is_fraud).length;
        const highRisk = transactions.filter(tx => {
            const avgScore = ((tx.source_score || 0) + (tx.target_score || 0)) / 2;
            return avgScore > 0.7;
        }).length;

        document.getElementById('fraudTotalTransactions').textContent = total.toLocaleString();
        document.getElementById('fraudTotalAmount').textContent = new Intl.NumberFormat('vi-VN', {
            style: 'currency',
            currency: 'VND'
        }).format(totalAmount);
        document.getElementById('fraudAvgScore').textContent = avgScore.toFixed(2);

        // Update additional stats
        const statsContainer = document.querySelector('.stats');
        if (confirmedFraud > 0 || highRisk > 0) {
            statsContainer.innerHTML += `
                <div class="d-flex justify-content-between mt-3 text-danger">
                    <span>Đã xác nhận gian lận:</span>
                    <strong>${confirmedFraud}</strong>
                </div>
                <div class="d-flex justify-content-between">
                    <span>Rủi ro cao:</span>
                    <strong>${highRisk}</strong>
                </div>
            `;
        }
    }

    // Tải dữ liệu giao dịch theo loại
    function loadTransactions(type = 'all', filters = {}) {
        const loadingEl = document.getElementById('fraud-transactions-loading');
        const contentEl = document.getElementById('fraud-transactions-content');
        
        loadingEl.classList.remove('d-none');
        contentEl.classList.add('d-none');

        fetch(`/api/fraud-transactions/${type}`)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                if (data.error) {
                    throw new Error(data.error);
                }

                loadingEl.classList.add('d-none');
                contentEl.classList.remove('d-none');

                // Store current transactions
                currentTransactions = data.transactions;

                // Cập nhật giao diện
                renderTransactionsTable(currentTransactions, filters);

                // Nhóm dữ liệu theo loại giao dịch để vẽ biểu đồ
                const typeStats = currentTransactions.reduce((acc, tx) => {
                    const type = tx.type || 'Khác';
                    if (!acc[type]) {
                        acc[type] = { count: 0, amount: 0 };
                    }
                    acc[type].count++;
                    acc[type].amount += tx.amount;
                    return acc;
                }, {});

                const chartData = Object.entries(typeStats).map(([type, stats]) => ({
                    type,
                    count: stats.count,
                    amount: stats.amount
                }));

                // Vẽ biểu đồ
                initFraudChart(chartData);
            })
            .catch(error => {
                console.error('Error loading fraud transactions:', error);
                loadingEl.classList.add('d-none');
                contentEl.classList.remove('d-none');
                contentEl.innerHTML = `
                    <div class="alert alert-danger d-flex align-items-center">
                        <i class="fas fa-exclamation-circle me-2"></i>
                        <div>
                            <strong>Error loading transactions:</strong><br>
                            ${error.message}
                        </div>
                    </div>
                `;
            });
    }

    // Event handlers
    let filterTimeout;
    const filterDelay = 300; // ms delay for debouncing filter updates

    function setupEventListeners() {
        // Transaction type filter
        document.getElementById('fraudTransactionType').addEventListener('change', function() {
            loadTransactions(this.value, getCurrentFilters());
        });

        // Amount range filter
        const amountFilterInputs = ['minAmount', 'maxAmount'].map(id => 
            document.getElementById('fraud' + id.charAt(0).toUpperCase() + id.slice(1)));
        
        amountFilterInputs.forEach(input => {
            input.addEventListener('input', debounceFilter);
        });

        // Score filter
        document.getElementById('fraudMinScore').addEventListener('input', debounceFilter);

        // Search filter
        document.getElementById('fraudSearch').addEventListener('input', debounceFilter);

        // Sort headers
        document.querySelectorAll('[data-sort]').forEach(header => {
            header.addEventListener('click', function() {
                const sort = this.dataset.sort;
                const currentSort = getCurrentFilters().sort;
                let newSort;

                if (currentSort && currentSort.startsWith(sort)) {
                    newSort = currentSort.endsWith(':asc') ? 
                        `${sort}:desc` : null;
                } else {
                    newSort = `${sort}:asc`;
                }

                // Update UI
                document.querySelectorAll('[data-sort]').forEach(h => {
                    h.classList.remove('sort-asc', 'sort-desc');
                });
                if (newSort) {
                    this.classList.add(newSort.endsWith(':asc') ? 'sort-asc' : 'sort-desc');
                }

                // Apply sort
                const filters = getCurrentFilters();
                filters.sort = newSort;
                renderTransactionsTable(currentTransactions, filters);
            });
        });
    }

    function debounceFilter() {
        clearTimeout(filterTimeout);
        filterTimeout = setTimeout(() => {
            renderTransactionsTable(currentTransactions, getCurrentFilters());
        }, filterDelay);
    }

    function getCurrentFilters() {
        return {
            type: document.getElementById('fraudTransactionType').value,
            minAmount: parseFloat(document.getElementById('fraudMinAmount').value) || null,
            maxAmount: parseFloat(document.getElementById('fraudMaxAmount').value) || null,
            minScore: parseFloat(document.getElementById('fraudMinScore').value) || null,
            search: document.getElementById('fraudSearch').value,
            sort: document.querySelector('[data-sort].sort-asc, [data-sort].sort-desc')?.dataset.sort
        };
    }

    // Export required functions
    window.showFraudTransactions = function() {
        const modal = new bootstrap.Modal(document.getElementById('fraudTransactionModal'));
        modal.show();
        loadTransactions('all');
    };

    // Initialize
    setupEventListeners();
});
