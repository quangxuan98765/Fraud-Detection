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
});

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