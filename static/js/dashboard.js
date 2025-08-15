// Dashboard JavaScript functionality
// Global variables
let dashboardCharts = {};
let refreshInterval = null;
let isRefreshing = false;

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
    setupEventListeners();
    startAutoRefresh();
});

function initializeDashboard() {
    console.log('Initializing Email Guardian Dashboard...');
    
    // Initialize tooltips
    initializeTooltips();
    
    // Initialize data tables
    initializeDataTables();
    
    // Setup form validations
    setupFormValidations();
    
    // Initialize any existing charts (handled in templates)
    updateChartResponsiveness();
}

function setupEventListeners() {
    // Global click handlers for action buttons
    document.addEventListener('click', function(e) {
        // Handle flag sender buttons
        if (e.target.closest('[data-action="flag-sender"]')) {
            const sender = e.target.closest('[data-action="flag-sender"]').dataset.sender;
            handleFlagSender(sender);
        }
        
        // Handle create case buttons
        if (e.target.closest('[data-action="create-case"]')) {
            const emailId = e.target.closest('[data-action="create-case"]').dataset.emailId;
            handleCreateCase(emailId);
        }
        
        // Handle update case status buttons
        if (e.target.closest('[data-action="update-case"]')) {
            const caseId = e.target.closest('[data-action="update-case"]').dataset.caseId;
            const status = e.target.closest('[data-action="update-case"]').dataset.status;
            handleUpdateCaseStatus(caseId, status);
        }
    });
    
    // Handle form submissions
    document.addEventListener('submit', function(e) {
        if (e.target.classList.contains('ajax-form')) {
            e.preventDefault();
            handleAjaxFormSubmit(e.target);
        }
    });
    
    // Handle file upload drag and drop
    setupFileUploadHandlers();
    
    // Window resize handler for chart responsiveness
    window.addEventListener('resize', debounce(updateChartResponsiveness, 300));
}

function initializeTooltips() {
    // Initialize Bootstrap tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

function initializeDataTables() {
    // Initialize DataTables with common options
    const tables = document.querySelectorAll('.data-table');
    tables.forEach(function(table) {
        if ($.fn.DataTable.isDataTable(table)) {
            $(table).DataTable().destroy();
        }
        
        $(table).DataTable({
            pageLength: 25,
            responsive: true,
            order: [[0, 'desc']],
            language: {
                search: "Search records:",
                lengthMenu: "Show _MENU_ records per page",
                info: "Showing _START_ to _END_ of _TOTAL_ records",
                paginate: {
                    first: "First",
                    last: "Last",
                    next: "Next",
                    previous: "Previous"
                }
            },
            columnDefs: [
                {
                    targets: 'no-sort',
                    orderable: false
                }
            ]
        });
    });
}

function setupFormValidations() {
    // Custom form validation for Bootstrap
    const forms = document.querySelectorAll('.needs-validation');
    forms.forEach(function(form) {
        form.addEventListener('submit', function(event) {
            if (!form.checkValidity()) {
                event.preventDefault();
                event.stopPropagation();
            }
            form.classList.add('was-validated');
        });
    });
}

function setupFileUploadHandlers() {
    const uploadAreas = document.querySelectorAll('.upload-area');
    
    uploadAreas.forEach(function(area) {
        // Prevent default drag behaviors
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            area.addEventListener(eventName, preventDefaults, false);
        });
        
        // Highlight drop area
        ['dragenter', 'dragover'].forEach(eventName => {
            area.addEventListener(eventName, highlight, false);
        });
        
        ['dragleave', 'drop'].forEach(eventName => {
            area.addEventListener(eventName, unhighlight, false);
        });
        
        // Handle dropped files
        area.addEventListener('drop', handleDrop, false);
    });
    
    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }
    
    function highlight(e) {
        e.target.classList.add('drag-over');
    }
    
    function unhighlight(e) {
        e.target.classList.remove('drag-over');
    }
    
    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        
        if (files.length > 0) {
            const fileInput = area.querySelector('input[type="file"]');
            if (fileInput) {
                fileInput.files = files;
                // Trigger change event
                fileInput.dispatchEvent(new Event('change'));
            }
        }
    }
}

let refreshErrorCount = 0;
const MAX_REFRESH_ERRORS = 3;

function startAutoRefresh() {
    // Stop existing interval if any
    if (refreshInterval) {
        clearInterval(refreshInterval);
    }
    
    // Auto-refresh dashboard data every 30 seconds
    refreshInterval = setInterval(function() {
        if (!isRefreshing && document.visibilityState === 'visible' && refreshErrorCount < MAX_REFRESH_ERRORS) {
            refreshDashboardData();
        } else if (refreshErrorCount >= MAX_REFRESH_ERRORS) {
            clearInterval(refreshInterval);
            refreshInterval = null;
            console.warn('Auto-refresh stopped due to repeated errors');
        }
    }, 30000);
    
    // Pause refresh when page is not visible
    document.addEventListener('visibilitychange', function() {
        if (document.visibilityState === 'hidden') {
            clearInterval(refreshInterval);
            refreshInterval = null;
        } else if (refreshErrorCount < MAX_REFRESH_ERRORS) {
            startAutoRefresh();
        }
    });
}

// Reset error count on successful refresh
function resetRefreshErrors() {
    refreshErrorCount = 0;
}

function refreshDashboardData() {
    if (isRefreshing) return;
    isRefreshing = true;
    
    // Show loading indicator
    showLoadingIndicator();
    
    fetch('/api/dashboard-stats')
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            // Check if response is actually JSON
            const contentType = response.headers.get('content-type');
            if (!contentType || !contentType.includes('application/json')) {
                throw new Error('Response is not JSON format');
            }
            
            return response.json();
        })
        .then(data => {
            // Validate data structure before updating
            if (data && typeof data === 'object') {
                updateDashboardStats(data);
                updateDashboardCharts(data);
                resetRefreshErrors(); // Reset error count on successful refresh
            } else {
                throw new Error('Invalid data structure received');
            }
        })
        .catch(error => {
            console.error('Dashboard refresh error:', error);
            refreshErrorCount++;
            
            if (refreshErrorCount >= MAX_REFRESH_ERRORS) {
                // Stop auto-refresh on repeated errors to prevent endless loop
                if (refreshInterval) {
                    clearInterval(refreshInterval);
                    refreshInterval = null;
                }
                showNotification('Dashboard auto-refresh disabled due to repeated errors', 'warning');
            } else {
                showNotification(`Dashboard refresh error (${refreshErrorCount}/${MAX_REFRESH_ERRORS})`, 'error');
            }
        })
        .finally(() => {
            hideLoadingIndicator();
            isRefreshing = false;
        });
}

function updateDashboardStats(data) {
    // Update key metrics cards
    const statsSelectors = {
        'total_emails': '.stats-total-emails',
        'active_cases': '.stats-active-cases',
        'flagged_senders': '.stats-flagged-senders',
        'todays_emails': '.stats-todays-emails'
    };
    
    Object.keys(statsSelectors).forEach(key => {
        const element = document.querySelector(statsSelectors[key]);
        if (element && data[key] !== undefined) {
            animateNumberChange(element, data[key]);
        }
    });
}

function updateDashboardCharts(data) {
    // Update existing charts with new data
    if (window.timelineChart && data.timeline_data) {
        window.timelineChart.data.labels = data.timeline_data.map(item => item[0]);
        window.timelineChart.data.datasets[0].data = data.timeline_data.map(item => item[1]);
        window.timelineChart.update('none'); // No animation for refresh
    }
    
    if (window.departmentChart && data.department_data) {
        window.departmentChart.data.labels = data.department_data.slice(0, 5).map(item => item[0] || 'Unknown');
        window.departmentChart.data.datasets[0].data = data.department_data.slice(0, 5).map(item => item[1]);
        window.departmentChart.update('none');
    }
}

function updateChartResponsiveness() {
    // Ensure charts are responsive after window resize
    Object.values(dashboardCharts).forEach(chart => {
        if (chart && chart.resize) {
            chart.resize();
        }
    });
}

// Action Handlers
function handleFlagSender(sender) {
    const modal = new bootstrap.Modal(document.getElementById('flagSenderModal'));
    document.getElementById('flagSenderEmail').value = sender;
    modal.show();
}

function handleCreateCase(emailId) {
    const modal = new bootstrap.Modal(document.getElementById('createCaseModal'));
    document.getElementById('caseEmailId').value = emailId;
    modal.show();
}

function handleUpdateCaseStatus(caseId, status) {
    if (!confirm(`Are you sure you want to update this case status to "${status}"?`)) {
        return;
    }
    
    showLoadingIndicator();
    
    fetch('/api/update-case-status', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            case_id: caseId,
            status: status
        })
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            showNotification(result.message, 'success');
            // Reload the page or update the UI
            setTimeout(() => location.reload(), 1000);
        } else {
            showNotification(result.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error updating case status:', error);
        showNotification('An error occurred while updating the case status', 'error');
    })
    .finally(() => {
        hideLoadingIndicator();
    });
}

function handleAjaxFormSubmit(form) {
    const formData = new FormData(form);
    const submitBtn = form.querySelector('button[type="submit"]');
    const originalText = submitBtn.innerHTML;
    
    // Disable submit button and show loading
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Processing...';
    
    fetch(form.action, {
        method: form.method,
        body: formData
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            showNotification(result.message, 'success');
            form.reset();
            // Close modal if form is in a modal
            const modal = form.closest('.modal');
            if (modal) {
                bootstrap.Modal.getInstance(modal).hide();
            }
        } else {
            showNotification(result.error, 'error');
        }
    })
    .catch(error => {
        console.error('Form submission error:', error);
        showNotification('An error occurred while processing the form', 'error');
    })
    .finally(() => {
        submitBtn.disabled = false;
        submitBtn.innerHTML = originalText;
    });
}

// Utility Functions
function showNotification(message, type = 'info') {
    // Create and show Bootstrap alert
    const alertClass = type === 'error' ? 'alert-danger' : 
                      type === 'success' ? 'alert-success' : 
                      type === 'warning' ? 'alert-warning' : 'alert-info';
    
    const alertHtml = `
        <div class="alert ${alertClass} alert-dismissible fade show" role="alert">
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        </div>
    `;
    
    // Find or create notification container
    let container = document.querySelector('.notification-container');
    if (!container) {
        container = document.createElement('div');
        container.className = 'notification-container position-fixed top-0 end-0 p-3';
        container.style.zIndex = '1060';
        document.body.appendChild(container);
    }
    
    container.innerHTML = alertHtml;
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        const alert = container.querySelector('.alert');
        if (alert) {
            alert.remove();
        }
    }, 5000);
}

function showLoadingIndicator() {
    // Show global loading indicator
    let loader = document.querySelector('.global-loader');
    if (!loader) {
        loader = document.createElement('div');
        loader.className = 'global-loader position-fixed top-50 start-50 translate-middle';
        loader.innerHTML = '<div class="spinner-border text-primary" role="status"><span class="visually-hidden">Loading...</span></div>';
        loader.style.zIndex = '1070';
        document.body.appendChild(loader);
    }
    loader.style.display = 'block';
}

function hideLoadingIndicator() {
    const loader = document.querySelector('.global-loader');
    if (loader) {
        loader.style.display = 'none';
    }
}

function animateNumberChange(element, newValue) {
    const currentValue = parseInt(element.textContent.replace(/,/g, '')) || 0;
    const duration = 1000; // 1 second
    const startTime = Date.now();
    
    function updateNumber() {
        const elapsed = Date.now() - startTime;
        const progress = Math.min(elapsed / duration, 1);
        
        // Ease out animation
        const easeOut = 1 - Math.pow(1 - progress, 3);
        const current = Math.round(currentValue + (newValue - currentValue) * easeOut);
        
        element.textContent = new Intl.NumberFormat().format(current);
        
        if (progress < 1) {
            requestAnimationFrame(updateNumber);
        }
    }
    
    updateNumber();
}

function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Export functions for global use
window.EmailGuardian = {
    refreshDashboardData,
    showNotification,
    handleFlagSender,
    handleCreateCase,
    handleUpdateCaseStatus
};

// Cleanup on page unload
window.addEventListener('beforeunload', function() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
    }
});
