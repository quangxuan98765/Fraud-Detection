// Mã JavaScript chung cho trang web
document.addEventListener('DOMContentLoaded', function() {
    // Kích hoạt tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'))
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl)
    });
    
    // Đặt sự kiện cho các nút
    var buttons = document.querySelectorAll('.btn');
    buttons.forEach(function(button) {
        button.addEventListener('click', function() {
            if (!this.disabled && !this.classList.contains('no-effect')) {
                var icon = this.querySelector('i');
                if (icon) {
                    icon.classList.add('fa-spin');
                    setTimeout(function() {
                        icon.classList.remove('fa-spin');
                    }, 1000);
                }
            }
        });
    });

    // Tải dữ liệu tài khoản đáng ngờ khi trang đã tải
    if (document.getElementById('suspicious-accounts')) {
        loadSuspiciousAccounts();
    }
});

// Hàm tải dữ liệu tài khoản đáng ngờ từ API
function loadSuspiciousAccounts() {
    fetch('/api/suspicious')
        .then(response => response.json())
        .then(data => {
            renderSuspiciousAccounts(data.accounts || []);
        })
        .catch(error => {
            console.error('Lỗi khi tải dữ liệu tài khoản đáng ngờ:', error);
            document.getElementById('suspicious-accounts').innerHTML = `
                <tr>
                    <td colspan="4" class="text-center py-3 text-danger">
                        <i class="fas fa-exclamation-circle me-2"></i>
                        Không thể tải dữ liệu tài khoản đáng ngờ
                    </td>
                </tr>
            `;
        });
}

// Hàm hiển thị thông báo
function showNotification(message, type) {
    var alertDiv = document.createElement('div');
    alertDiv.classList.add('alert', 'alert-' + type, 'alert-dismissible', 'fade', 'show');
    alertDiv.setAttribute('role', 'alert');
    
    alertDiv.innerHTML = message + 
        '<button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>';
    
    document.querySelector('.container').prepend(alertDiv);
    
    // Tự động ẩn sau 5 giây
    setTimeout(function() {
        var bsAlert = new bootstrap.Alert(alertDiv);
        bsAlert.close();
    }, 5000);
}