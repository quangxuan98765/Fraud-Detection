/**
 * Xử lý các chức năng liên quan đến hiển thị cộng đồng rủi ro
 */
document.addEventListener('DOMContentLoaded', function() {
    // Thêm modal HTML vào trang
    const modalHTML = `
    <div class="modal fade" id="communityModal" tabindex="-1" aria-labelledby="communityModalLabel" aria-hidden="true">
        <div class="modal-dialog modal-xl modal-dialog-scrollable">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="communityModalLabel">
                        <i class="fas fa-users-cog me-2"></i> Chi tiết cộng đồng rủi ro
                    </h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body p-0">
                    <!-- Phần loading -->
                    <div id="community-loading" class="text-center py-5">
                        <div class="spinner-border text-primary mb-3" role="status">
                            <span class="visually-hidden">Đang tải...</span>
                        </div>
                        <p class="lead mb-0">Đang tải dữ liệu cộng đồng...</p>
                    </div>
                    
                    <!-- Phần nội dung -->
                    <div id="community-content" class="d-none">
                        <!-- Tổng quan cộng đồng -->
                        <div class="community-header p-4 mb-0">
                            <div class="d-flex justify-content-between align-items-start">
                                <div>
                                    <h4 id="community-title" class="mb-1">Cộng đồng #<span id="community-id"></span></h4>
                                    <p id="community-subtitle" class="text-muted mb-0"></p>
                                </div>
                                <span id="community-risk-badge" class="badge bg-danger px-3 py-2 fs-6"></span>
                            </div>
                        </div>
                        
                        <!-- Thống kê nhanh -->
                        <div class="row g-0 text-center border-top border-bottom py-2 mb-3">
                            <div class="col-3 p-3 border-end">
                                <h3 id="stat-accounts" class="mb-0">--</h3>
                                <small class="text-muted">Tài khoản</small>
                            </div>
                            <div class="col-3 p-3 border-end">
                                <h3 id="stat-risk" class="mb-0">--</h3>
                                <small class="text-muted">Tài khoản rủi ro</small>
                            </div>
                            <div class="col-3 p-3 border-end">
                                <h3 id="stat-transactions" class="mb-0">--</h3>
                                <small class="text-muted">Giao dịch nội bộ</small>
                            </div>
                            <div class="col-3 p-3">
                                <h3 id="stat-avg-score" class="mb-0">--</h3>
                                <small class="text-muted">Điểm trung bình</small>
                            </div>
                        </div>
                        
                        <!-- Nội dung tab -->
                        <div class="px-4 pb-4">
                            <ul class="nav nav-tabs mb-4" id="communityTab" role="tablist">
                                <li class="nav-item" role="presentation">
                                    <button class="nav-link active" id="accounts-tab" data-bs-toggle="tab" data-bs-target="#accounts-tab-pane" type="button" role="tab" aria-controls="accounts-tab-pane" aria-selected="true">
                                        <i class="fas fa-users me-2"></i>Tài khoản
                                    </button>
                                </li>
                                <li class="nav-item" role="presentation">
                                    <button class="nav-link" id="network-tab" data-bs-toggle="tab" data-bs-target="#network-tab-pane" type="button" role="tab" aria-controls="network-tab-pane" aria-selected="false">
                                        <i class="fas fa-project-diagram me-2"></i>Mạng lưới
                                    </button>
                                </li>
                                <li class="nav-item" role="presentation">
                                    <button class="nav-link" id="related-tab" data-bs-toggle="tab" data-bs-target="#related-tab-pane" type="button" role="tab" aria-controls="related-tab-pane" aria-selected="false">
                                        <i class="fas fa-link me-2"></i>Liên kết
                                    </button>
                                </li>
                            </ul>
                            
                            <div class="tab-content" id="communityTabContent">
                                <!-- Tab tài khoản -->
                                <div class="tab-pane fade show active" id="accounts-tab-pane" role="tabpanel" aria-labelledby="accounts-tab" tabindex="0">
                                    <div class="table-responsive">
                                        <table class="table table-striped table-hover">
                                            <thead>
                                                <tr>
                                                    <th>ID Tài khoản</th>
                                                    <th>Điểm rủi ro</th>
                                                    <th>Đặc điểm</th>
                                                    <th>Giao dịch ra</th>
                                                    <th>Giao dịch vào</th>
                                                    <th>Chênh lệch</th>
                                                </tr>
                                            </thead>
                                            <tbody id="community-accounts-table">
                                                <!-- Dữ liệu tài khoản sẽ được thêm vào đây -->
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                                
                                <!-- Tab mạng lưới -->
                                <div class="tab-pane fade" id="network-tab-pane" role="tabpanel" aria-labelledby="network-tab" tabindex="0">
                                    <div class="card mb-3 shadow-sm">
                                        <div class="card-body p-0">
                                            <div id="community-network" style="height: 450px; background-color: #131c2c; border-radius: 4px;"></div>
                                        </div>
                                    </div>
                                    <div class="mt-3">
                                        <h6 class="mb-3">Giao dịch đáng chú ý:</h6>
                                        <div class="table-responsive">
                                            <table class="table table-sm">
                                                <thead>
                                                    <tr>
                                                        <th>Từ</th>
                                                        <th>Đến</th>
                                                        <th>Số tiền</th>
                                                        <th>Loại</th>
                                                        <th>Trạng thái</th>
                                                    </tr>
                                                </thead>
                                                <tbody id="community-transactions-table">
                                                    <!-- Dữ liệu giao dịch sẽ được thêm vào đây -->
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                </div>
                                
                                <!-- Tab liên kết -->
                                <div class="tab-pane fade" id="related-tab-pane" role="tabpanel" aria-labelledby="related-tab" tabindex="0">
                                    <div class="row mb-4" id="related-communities-container">
                                        <!-- Dữ liệu cộng đồng liên quan sẽ được thêm vào đây -->
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Đóng</button>
                </div>
            </div>
        </div>
    </div>
    `;
    
    // Thêm modal vào cuối body
    document.body.insertAdjacentHTML('beforeend', modalHTML);
    
    // Thêm CSS động cho hiệu ứng và giao diện
    const style = document.createElement('style');
    style.textContent = `
        .highlight-card {
            box-shadow: 0 0 20px rgba(255, 193, 7, 0.7);
            animation: pulse 1s infinite;
        }
        
        @keyframes pulse {
            0% { box-shadow: 0 0 20px rgba(255, 193, 7, 0.7); }
            50% { box-shadow: 0 0 30px rgba(255, 193, 7, 1); }
            100% { box-shadow: 0 0 20px rgba(255, 193, 7, 0.7); }
        }
        
        .community-header {
            background-color: #f8f9fa;
            border-bottom: 1px solid #e9ecef;
        }
        
        .community-card {
            cursor: pointer;
            transition: all 0.2s;
            border-radius: 8px;
            overflow: hidden;
            height: 100%;
        }
        
        .community-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 20px rgba(0,0,0,0.1);
        }
        
        .community-card .card-header {
            padding: 12px 15px;
        }
        
        .community-card .card-body {
            padding: 15px;
        }
        
        .risk-low { background-color: #20c997; }
        .risk-medium { background-color: #fd7e14; }
        .risk-high { background-color: #dc3545; }
        
        .size-small { border-left: 4px solid #20c997; }
        .size-medium { border-left: 4px solid #fd7e14; }
        .size-large { border-left: 4px solid #dc3545; }
        
        #communityListModal .modal-body {
            max-height: 70vh;
            overflow-y: auto;
        }
        
        .node {
            stroke: #fff;
            stroke-width: 1.5px;
            transition: all 0.2s;
        }
        
        .node:hover {
            stroke-width: 2.5px;
        }
        
        .link {
            stroke-opacity: 0.6;
        }
        
        .link.fraud {
            stroke: #ff4136;
            stroke-dasharray: 5, 5;
        }
    `;
    document.head.appendChild(style);
    
    // Tạo modal danh sách cộng đồng
    const communitiesListModal = `
    <div class="modal fade" id="communityListModal" tabindex="-1" aria-labelledby="communityListModalLabel" aria-hidden="true">
        <div class="modal-dialog modal-xl">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="communityListModalLabel">
                        <i class="fas fa-users me-2"></i> Danh sách cộng đồng rủi ro
                    </h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body p-3">
                    <!-- Bộ lọc -->
                    <div class="row mb-4">
                        <div class="col-md-3">
                            <select id="sizeFilter" class="form-select form-select-sm">
                                <option value="all">Tất cả kích thước</option>
                                <option value="small">Nhỏ (2-3 tài khoản)</option>
                                <option value="medium">Trung bình (4-10 tài khoản)</option>
                                <option value="large">Lớn (11+ tài khoản)</option>
                            </select>
                        </div>
                        <div class="col-md-3">
                            <select id="riskFilter" class="form-select form-select-sm">
                                <option value="all">Tất cả mức độ rủi ro</option>
                                <option value="high">Rủi ro cao (>0.7)</option>
                                <option value="medium">Rủi ro trung bình (>0.5)</option>
                                <option value="low">Rủi ro thấp (<0.5)</option>
                            </select>
                        </div>
                        <div class="col-md-4">
                            <input type="text" id="communitySearch" class="form-control form-control-sm" placeholder="Tìm kiếm ID cộng đồng...">
                        </div>
                        <div class="col-md-2">
                            <button id="resetFilters" class="btn btn-sm btn-outline-secondary w-100">
                                <i class="fas fa-redo-alt me-1"></i> Đặt lại
                            </button>
                        </div>
                    </div>
                    
                    <!-- Danh sách cộng đồng -->
                    <div id="communities-loading" class="text-center py-5">
                        <div class="spinner-border text-primary mb-3" role="status">
                            <span class="visually-hidden">Đang tải...</span>
                        </div>
                        <p class="lead mb-0">Đang tải danh sách cộng đồng...</p>
                    </div>
                    
                    <div id="communities-container" class="row g-3">
                        <!-- Danh sách cộng đồng sẽ được thêm vào đây -->
                    </div>
                    
                    <div id="no-communities-found" class="text-center py-5 d-none">
                        <i class="fas fa-search fa-3x text-muted mb-3"></i>
                        <p class="lead">Không tìm thấy cộng đồng nào phù hợp với điều kiện lọc</p>
                        <button id="resetFilters2" class="btn btn-outline-primary mt-2">
                            <i class="fas fa-redo-alt me-1"></i> Đặt lại bộ lọc
                        </button>
                    </div>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Đóng</button>
                </div>
            </div>
        </div>
    </div>
    `;
    document.body.insertAdjacentHTML('beforeend', communitiesListModal);
    
    // Nút xem chi tiết cộng đồng
    const showCommunitiesBtn = document.getElementById('showCommunitiesBtn');
    if (showCommunitiesBtn) {
        showCommunitiesBtn.addEventListener('click', function() {
            // Mở modal danh sách cộng đồng
            const communityListModal = new bootstrap.Modal(document.getElementById('communityListModal'));
            communityListModal.show();
            
            // Tải danh sách cộng đồng
            loadCommunities();
        });
    }
    
    // Hàm tải danh sách cộng đồng
    function loadCommunities() {
        const container = document.getElementById('communities-container');
        const loadingEl = document.getElementById('communities-loading');
        const notFoundEl = document.getElementById('no-communities-found');
        
        container.innerHTML = '';
        loadingEl.classList.remove('d-none');
        notFoundEl.classList.add('d-none');
        
        fetch('/api/communities')
            .then(response => response.json())
            .then(data => {
                loadingEl.classList.add('d-none');
                
                if (!data.communities || data.communities.length === 0) {
                    notFoundEl.classList.remove('d-none');
                    return;
                }
                
                // Lưu trữ dữ liệu để lọc
                window.allCommunities = data.communities;
                
                // Hiển thị cộng đồng
                renderCommunities(data.communities);
                
                // Thiết lập bộ lọc
                setupFilters();
            })
            .catch(error => {
                console.error('Error loading communities:', error);
                loadingEl.classList.add('d-none');
                container.innerHTML = `
                    <div class="col-12 text-center py-4">
                        <i class="fas fa-exclamation-triangle fa-3x text-warning mb-3"></i>
                        <p class="lead">Không thể tải danh sách cộng đồng</p>
                        <small class="text-muted">Lỗi: ${error.message}</small>
                    </div>
                `;
            });
    }
    
    // Hàm hiển thị danh sách cộng đồng
    function renderCommunities(communities) {
        const container = document.getElementById('communities-container');
        container.innerHTML = '';
          communities.forEach(community => {
            const avgScore = parseFloat(community.avg_score);
            const size = parseInt(community.size);
            
            // Xác định màu sắc dựa trên mức độ rủi ro
            let riskClass = 'risk-low';
            if (avgScore > 0.7) riskClass = 'risk-high';
            else if (avgScore > 0.5) riskClass = 'risk-medium';
            
            // Xác định kích thước
            let sizeClass = 'size-small';
            if (size > 10) sizeClass = 'size-large';
            else if (size > 3) sizeClass = 'size-medium';
            
            const cardHTML = `
                <div class="col-md-4 col-lg-3">
                    <div class="card community-card shadow-sm ${sizeClass}" data-community-id="${community.id}" data-community-size="${community.size_category}" data-community-risk="${community.risk_level}">
                        <div class="card-header ${riskClass} text-white d-flex justify-content-between align-items-center">
                            <div>Cộng đồng #${community.id}</div>
                            <span class="badge bg-light text-dark">${size} tài khoản</span>
                        </div>
                        <div class="card-body">
                            <div class="mb-3">
                                <div class="progress" style="height: 5px">
                                    <div class="progress-bar bg-danger" style="width: ${avgScore * 100}%" aria-valuenow="${avgScore * 100}" aria-valuemin="0" aria-valuemax="100"></div>
                                </div>
                                <div class="d-flex justify-content-between align-items-center mt-1">
                                    <small class="text-muted">Điểm rủi ro trung bình</small>
                                    <small class="fw-bold">${avgScore.toFixed(2)}</small>
                                </div>
                            </div>
                            <button class="btn btn-sm btn-primary w-100 view-community-btn">
                                <i class="fas fa-search me-1"></i> Xem chi tiết
                            </button>
                        </div>
                    </div>
                </div>
            `;
            container.insertAdjacentHTML('beforeend', cardHTML);
        });
        
        // Thêm sự kiện cho nút xem chi tiết
        document.querySelectorAll('.view-community-btn').forEach(button => {
            button.addEventListener('click', function() {
                const card = this.closest('.community-card');
                const communityId = card.dataset.communityId;
                
                // Đóng modal danh sách
                const listModal = bootstrap.Modal.getInstance(document.getElementById('communityListModal'));
                listModal.hide();
                
                // Mở modal chi tiết
                showCommunityDetails(communityId);
                
                // Log để debugging
                console.log("Đang mở chi tiết cộng đồng ID:", communityId);
            });
        });
    }
    
    // Hàm thiết lập bộ lọc
    function setupFilters() {
        const sizeFilter = document.getElementById('sizeFilter');
        const riskFilter = document.getElementById('riskFilter');
        const searchInput = document.getElementById('communitySearch');
        const resetBtn = document.getElementById('resetFilters');
        const resetBtn2 = document.getElementById('resetFilters2');
        
        // Hàm lọc
        function applyFilters() {
            const sizeValue = sizeFilter.value;
            const riskValue = riskFilter.value;
            const searchValue = searchInput.value.trim().toLowerCase();
            
            let filtered = window.allCommunities;
            
            // Lọc theo kích thước
            if (sizeValue !== 'all') {
                filtered = filtered.filter(c => c.size_category === sizeValue);
            }
            
            // Lọc theo mức độ rủi ro
            if (riskValue !== 'all') {
                filtered = filtered.filter(c => c.risk_level === riskValue);
            }
            
            // Lọc theo tìm kiếm
            if (searchValue) {
                filtered = filtered.filter(c => c.id.toString().includes(searchValue));
            }
            
            // Hiển thị kết quả
            renderCommunities(filtered);
            
            // Hiển thị thông báo nếu không có kết quả
            const notFoundEl = document.getElementById('no-communities-found');
            if (filtered.length === 0) {
                notFoundEl.classList.remove('d-none');
            } else {
                notFoundEl.classList.add('d-none');
            }
        }
        
        // Gắn sự kiện
        sizeFilter.addEventListener('change', applyFilters);
        riskFilter.addEventListener('change', applyFilters);
        searchInput.addEventListener('input', applyFilters);
        
        // Nút đặt lại
        resetBtn.addEventListener('click', function() {
            sizeFilter.value = 'all';
            riskFilter.value = 'all';
            searchInput.value = '';
            applyFilters();
        });
        
        resetBtn2.addEventListener('click', function() {
            sizeFilter.value = 'all';
            riskFilter.value = 'all';
            searchInput.value = '';
            applyFilters();
        });
    }    // Hàm hiển thị chi tiết cộng đồng
    function showCommunityDetails(communityId) {
        console.log(`Đang tải chi tiết cộng đồng ID: ${communityId}`);
        const modal = new bootstrap.Modal(document.getElementById('communityModal'));
        modal.show();
        
        // Hiển thị loading
        document.getElementById('community-loading').classList.remove('d-none');
        document.getElementById('community-content').classList.add('d-none');
        
        // Tải dữ liệu từ API
        fetch(`/api/community/${communityId}`)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error ${response.status}: ${response.statusText}`);
                }
                return response.json();
            })
            .then(data => {
                console.log("Dữ liệu chi tiết cộng đồng nhận được:", data);
                
                if (data.error) {
                    throw new Error(data.error);
                }
                
                // Ẩn loading, hiển thị nội dung
                document.getElementById('community-loading').classList.add('d-none');
                document.getElementById('community-content').classList.remove('d-none');
                
                // Hiển thị thông tin cộng đồng
                renderCommunityDetails(communityId, data);
                
                // Vẽ biểu đồ network khi tab được chọn
                document.getElementById('network-tab').addEventListener('shown.bs.tab', function() {
                    renderCommunityNetwork(data.accounts, data.transactions);
                });
            })
            .catch(error => {
                console.error('Error loading community details:', error);
                document.getElementById('community-loading').classList.add('d-none');
                document.getElementById('community-content').classList.remove('d-none');
                document.getElementById('community-content').innerHTML = `
                    <div class="text-center py-5">
                        <i class="fas fa-exclamation-triangle fa-3x text-warning mb-3"></i>
                        <p class="lead">Không thể tải thông tin chi tiết cộng đồng</p>
                        <small class="text-muted">Lỗi: ${error.message}</small>
                        <div class="mt-3">
                            <button class="btn btn-sm btn-secondary" data-bs-dismiss="modal">Đóng</button>
                            <button class="btn btn-sm btn-primary ms-2" onclick="showCommunityDetails('${communityId}')">Thử lại</button>
                        </div>
                    </div>
                `;
            });
    }
    
    // Expose the function globally
    window.showCommunityDetails = showCommunityDetails;
    
    // Hàm hiển thị chi tiết cộng đồng
    function renderCommunityDetails(communityId, data) {
        const overview = data.overview;
        const accounts = data.accounts;
        const transactions = data.transactions;
        const relatedCommunities = data.related_communities;
        
        // Hiển thị thông tin tổng quan
        document.getElementById('community-id').textContent = communityId;
        
        const totalAccounts = overview.total_accounts || 0;
        const highRiskAccounts = overview.high_risk_accounts || 0;
        const avgScore = parseFloat(overview.avg_score || 0);
        
        document.getElementById('community-subtitle').textContent = 
            `Cộng đồng ${totalAccounts <= 3 ? 'nhỏ' : totalAccounts <= 10 ? 'trung bình' : 'lớn'} với ${totalAccounts} tài khoản, trong đó có ${highRiskAccounts} tài khoản rủi ro cao.`;
        
        // Badge rủi ro
        const riskBadge = document.getElementById('community-risk-badge');
        if (avgScore > 0.7) {
            riskBadge.className = 'badge bg-danger px-3 py-2 fs-6';
            riskBadge.textContent = 'RỦI RO CAO';
        } else if (avgScore > 0.5) {
            riskBadge.className = 'badge bg-warning px-3 py-2 fs-6 text-dark';
            riskBadge.textContent = 'RỦI RO TRUNG BÌNH';
        } else {
            riskBadge.className = 'badge bg-success px-3 py-2 fs-6';
            riskBadge.textContent = 'RỦI RO THẤP';
        }
        
        // Thống kê nhanh
        document.getElementById('stat-accounts').textContent = totalAccounts;
        document.getElementById('stat-risk').textContent = highRiskAccounts;
        document.getElementById('stat-transactions').textContent = transactions.length;
        document.getElementById('stat-avg-score').textContent = avgScore.toFixed(2);
        
        // Hiển thị danh sách tài khoản
        renderAccountsList(accounts);
        
        // Hiển thị danh sách giao dịch
        renderTransactionsList(transactions);
        
        // Hiển thị cộng đồng liên quan
        renderRelatedCommunities(relatedCommunities);
    }
    
    // Hàm hiển thị danh sách tài khoản
    function renderAccountsList(accounts) {
        const container = document.getElementById('community-accounts-table');
        container.innerHTML = '';
        
        if (!accounts || accounts.length === 0) {
            container.innerHTML = `
                <tr>
                    <td colspan="6" class="text-center py-4">
                        Không có dữ liệu tài khoản trong cộng đồng này
                    </td>
                </tr>
            `;
            return;
        }
        
        accounts.forEach(account => {
            // Xác định đặc điểm nổi bật
            let feature = '';
            if (account.in_cycle === 1) feature = 'Vòng tròn giao dịch';
            else if (Math.abs(account.imbalance) > 0.5) feature = 'Mất cân bằng giao dịch';
            else if (account.pagerank > 1.5) feature = 'Trung tâm mạng lưới';
            else feature = 'Bình thường';
            
            // Class màu cho điểm
            const scoreClass = account.score > 0.7 ? 'danger' : account.score > 0.5 ? 'warning' : 'success';
            
            container.insertAdjacentHTML('beforeend', `
                <tr>
                    <td><strong>${account.id}</strong></td>
                    <td><span class="badge bg-${scoreClass}">${parseFloat(account.score).toFixed(2)}</span></td>
                    <td>${feature}</td>
                    <td>${account.sent_count} (${formatCurrency(account.sent_amount)})</td>
                    <td>${account.received_count} (${formatCurrency(account.received_amount)})</td>
                    <td class="text-${account.imbalance_amount > 10000 ? 'danger' : 'success'}">${formatCurrency(account.imbalance_amount)}</td>
                </tr>
            `);
        });
    }
    
    // Hàm hiển thị danh sách giao dịch
    function renderTransactionsList(transactions) {
        const container = document.getElementById('community-transactions-table');
        container.innerHTML = '';
        
        if (!transactions || transactions.length === 0) {
            container.innerHTML = `
                <tr>
                    <td colspan="5" class="text-center py-4">
                        Không có dữ liệu giao dịch nội bộ trong cộng đồng này
                    </td>
                </tr>
            `;
            return;
        }
          // Sắp xếp giao dịch theo giá trị giảm dần và ưu tiên giao dịch có điểm rủi ro cao
        transactions.sort((a, b) => {
            // Determine if transactions are high-risk based on their source and target scores
            const aIsFraud = a.source_score > 0.8 && a.target_score > 0.8;
            const bIsFraud = b.source_score > 0.8 && b.target_score > 0.8;
            
            if (aIsFraud !== bIsFraud) return bIsFraud - aIsFraud;
            return b.amount - a.amount;
        });
        
        // Hiển thị tối đa 10 giao dịch
        const displayTransactions = transactions.slice(0, 10);
        
        displayTransactions.forEach(tx => {
            container.insertAdjacentHTML('beforeend', `
                <tr>
                    <td>${tx.source}</td>
                    <td>${tx.target}</td>
                    <td>${formatCurrency(tx.amount)}</td>
                    <td>${tx.type}</td>                    <td>
                        ${(tx.source_score > 0.8 && tx.target_score > 0.8) ? 
                            '<span class="badge bg-danger">Gian lận</span>' : 
                            tx.high_risk ? 
                                '<span class="badge bg-warning text-dark">Rủi ro</span>' : 
                                '<span class="badge bg-success">Bình thường</span>'
                        }
                    </td>
                </tr>
            `);
        });
        
        // Hiển thị thông báo nếu còn nhiều giao dịch
        if (transactions.length > 10) {
            container.insertAdjacentHTML('beforeend', `
                <tr>
                    <td colspan="5" class="text-center text-muted">
                        <small>Còn ${transactions.length - 10} giao dịch khác không được hiển thị</small>
                    </td>
                </tr>
            `);
        }
    }
    
    // Hàm hiển thị cộng đồng liên quan
    function renderRelatedCommunities(relatedCommunities) {
        const container = document.getElementById('related-communities-container');
        container.innerHTML = '';
        
        if (!relatedCommunities || relatedCommunities.length === 0) {
            container.innerHTML = `
                <div class="col-12 text-center py-4">
                    <i class="fas fa-link-slash fa-3x text-muted mb-3"></i>
                    <p>Không tìm thấy mối liên hệ với các cộng đồng khác</p>
                </div>
            `;
            return;
        }
        
        relatedCommunities.forEach(community => {
            const avgScore = parseFloat(community.avg_score);
            
            // Xác định màu sắc dựa trên mức độ rủi ro
            let riskClass = 'success';
            if (avgScore > 0.7) riskClass = 'danger';
            else if (avgScore > 0.5) riskClass = 'warning';
            
            container.insertAdjacentHTML('beforeend', `
                <div class="col-md-6 mb-3">
                    <div class="card h-100">
                        <div class="card-header bg-light d-flex justify-content-between align-items-center">
                            <div>
                                <strong>Cộng đồng #${community.id}</strong>
                            </div>
                            <span class="badge bg-${riskClass}">${avgScore.toFixed(2)}</span>
                        </div>
                        <div class="card-body">
                            <div class="row text-center mb-3">
                                <div class="col-6">
                                    <h5 class="mb-0">${community.account_count}</h5>
                                    <small class="text-muted">Tài khoản</small>
                                </div>
                                <div class="col-6">
                                    <h5 class="mb-0">${community.transaction_count}</h5>
                                    <small class="text-muted">Giao dịch liên quan</small>
                                </div>
                            </div>
                            <div class="d-grid">
                                <button class="btn btn-sm btn-outline-primary view-related-community" data-community-id="${community.id}">
                                    <i class="fas fa-eye me-1"></i> Xem chi tiết
                                </button>
                            </div>
                        </div>
                        <div class="card-footer bg-white">
                            <small class="text-muted">
                                Tổng giá trị giao dịch: ${formatCurrency(community.total_amount)}
                            </small>
                        </div>
                    </div>
                </div>
            `);
        });
        
        // Thêm sự kiện cho nút xem chi tiết
        document.querySelectorAll('.view-related-community').forEach(button => {
            button.addEventListener('click', function() {
                const communityId = this.dataset.communityId;
                showCommunityDetails(communityId);
            });
        });
    }
    
    // Hàm vẽ biểu đồ mạng lưới cộng đồng
    function renderCommunityNetwork(accounts, transactions) {
        const container = document.getElementById('community-network');
        
        // Kiểm tra dữ liệu
        if (!accounts || !transactions || accounts.length === 0) {
            container.innerHTML = `
                <div class="d-flex justify-content-center align-items-center h-100">
                    <div class="text-center">
                        <i class="fas fa-project-diagram fa-3x text-muted mb-3"></i>
                        <p class="text-white">Không có đủ dữ liệu để hiển thị mạng lưới</p>
                    </div>
                </div>
            `;
            return;
        }
        
        // Chuẩn bị dữ liệu cho D3.js
        const nodes = accounts.map(acc => ({
            id: acc.id,
            score: acc.score,
            pagerank: acc.pagerank || 0,
            in_cycle: acc.in_cycle === 1,
            sent_count: acc.sent_count || 0,
            received_count: acc.received_count || 0
        }));
        
        const links = transactions.map(tx => ({            source: tx.source,
            target: tx.target,
            value: tx.amount,
            is_fraud: (tx.source_score > 0.8 && tx.target_score > 0.8)
        }));
        
        // Xóa nội dung container
        container.innerHTML = '';
        
        // Tạo SVG
        const width = container.clientWidth;
        const height = container.clientHeight;
        
        const svg = d3.select(container)
            .append('svg')
            .attr('width', width)
            .attr('height', height);
            
        // Thêm bộ lọc glow
        const defs = svg.append('defs');
        const filter = defs.append('filter')
            .attr('id', 'glow');
            
        filter.append('feGaussianBlur')
            .attr('stdDeviation', '2.5')
            .attr('result', 'coloredBlur');
            
        const feMerge = filter.append('feMerge');
        feMerge.append('feMergeNode')
            .attr('in', 'coloredBlur');
        feMerge.append('feMergeNode')
            .attr('in', 'SourceGraphic');
            
        // Thêm zoom
        const g = svg.append('g');
        const zoom = d3.zoom()
            .scaleExtent([0.1, 3])
            .on('zoom', (event) => {
                g.attr('transform', event.transform);
            });
            
        svg.call(zoom);
        
        // Áp dụng zoom ban đầu
        svg.call(zoom.transform, d3.zoomIdentity.scale(0.9).translate(width / 4, height / 4));
        
        // Tạo simulation
        const simulation = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(links).id(d => d.id).distance(80))
            .force('charge', d3.forceManyBody().strength(-200))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('x', d3.forceX(width / 2).strength(0.05))
            .force('y', d3.forceY(height / 2).strength(0.05))
            .force('collision', d3.forceCollide().radius(d => 5 + (d.score || 0) * 6).strength(0.7));
            
        // Thêm links
        const link = g.append('g')
            .selectAll('line')
            .data(links)
            .enter().append('line')
            .attr('class', d => 'link ' + (d.is_fraud ? 'fraud' : ''))
            .attr('stroke-width', d => Math.max(0.5, Math.min(Math.sqrt(d.value) / 100, 3)));
            
        // Thêm nodes
        const node = g.append('g')
            .selectAll('circle')
            .data(nodes)
            .enter().append('circle')
            .attr('class', 'node')
            .attr('r', d => 4 + (d.score || 0) * 6)
            .attr('fill', d => {
                if (d.score > 0.7) return '#ff4136';
                if (d.score > 0.5) return '#ff851b';
                return '#0074d9';
            })
            .attr('stroke', '#fff')
            .attr('stroke-width', 1.5)
            .style('filter', d => d.score > 0.7 ? 'url(#glow)' : null)
            .call(d3.drag()
                .on('start', dragstarted)
                .on('drag', dragged)
                .on('end', dragended));
                
        // Thêm tooltip
        node.append('title')
            .text(d => {
                return `ID: ${d.id}
Điểm gian lận: ${(d.score || 0).toFixed(2)}
Số GD gửi: ${d.sent_count}
Số GD nhận: ${d.received_count}
${d.in_cycle ? '⚠️ Tài khoản trong vòng tròn giao dịch' : ''}`;
            });
            
        // Thêm labels cho các nút quan trọng
        const labels = g.append('g')
            .selectAll('text')
            .data(nodes.filter(d => d.score > 0.7 || d.pagerank > 1.5))
            .enter()
            .append('text')
            .text(d => d.id)
            .attr('font-size', 8)
            .attr('fill', '#fff')
            .attr('text-anchor', 'middle')
            .attr('dy', d => -(6 + (d.score || 0) * 4))
            .attr('pointer-events', 'none');
            
        // Update positions
        simulation.on('tick', () => {
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);
                
            node
                .attr('cx', d => d.x)
                .attr('cy', d => d.y);
                
            labels
                .attr('x', d => d.x)
                .attr('y', d => d.y);
        });
        
        // Drag functions
        function dragstarted(event, d) {
            if (!event.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
        }
        
        function dragged(event, d) {
            d.fx = event.x;
            d.fy = event.y;
        }
        
        function dragended(event, d) {
            if (!event.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
        }
    }
    
    // Hàm định dạng tiền tệ
    function formatCurrency(value) {
        if (!value && value !== 0) return 'N/A';
        return new Intl.NumberFormat('vi-VN', {
            style: 'currency',
            currency: 'VND',
            maximumFractionDigits: 0
        }).format(value);
    }
});
