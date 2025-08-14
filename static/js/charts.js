// Chart.js configurations and utilities for Email Guardian
// Chart color schemes and default configurations

const chartColors = {
    primary: '#0d6efd',
    secondary: '#6c757d',
    success: '#198754',
    danger: '#dc3545',
    warning: '#ffc107',
    info: '#0dcaf0',
    light: '#f8f9fa',
    dark: '#212529'
};

const chartColorPalette = [
    '#0d6efd', '#198754', '#dc3545', '#ffc107', '#0dcaf0',
    '#6f42c1', '#fd7e14', '#20c997', '#e83e8c', '#6c757d'
];

// Default chart configuration
Chart.defaults.font.family = "'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif";
Chart.defaults.font.size = 12;
Chart.defaults.color = '#6c757d';
Chart.defaults.plugins.legend.labels.usePointStyle = true;

// Chart utility functions
const ChartUtils = {
    // Generate color with opacity
    colorWithOpacity(color, opacity) {
        const hex = color.replace('#', '');
        const r = parseInt(hex.substr(0, 2), 16);
        const g = parseInt(hex.substr(2, 2), 16);
        const b = parseInt(hex.substr(4, 2), 16);
        return `rgba(${r}, ${g}, ${b}, ${opacity})`;
    },
    
    // Generate gradient
    createGradient(ctx, color1, color2, vertical = true) {
        const gradient = vertical ? 
            ctx.createLinearGradient(0, 0, 0, ctx.canvas.height) :
            ctx.createLinearGradient(0, 0, ctx.canvas.width, 0);
        
        gradient.addColorStop(0, color1);
        gradient.addColorStop(1, color2);
        return gradient;
    },
    
    // Format numbers for display
    formatNumber(value) {
        if (value >= 1000000) {
            return (value / 1000000).toFixed(1) + 'M';
        } else if (value >= 1000) {
            return (value / 1000).toFixed(1) + 'K';
        }
        return value.toString();
    },
    
    // Generate time series labels
    generateTimeLabels(days = 30) {
        const labels = [];
        const now = new Date();
        for (let i = days - 1; i >= 0; i--) {
            const date = new Date(now.getTime() - i * 24 * 60 * 60 * 1000);
            labels.push(date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));
        }
        return labels;
    }
};

// Chart configurations
const ChartConfigs = {
    // Line chart for time series data
    timeSeriesLine: {
        type: 'line',
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    titleColor: '#fff',
                    bodyColor: '#fff',
                    borderColor: chartColors.primary,
                    borderWidth: 1
                }
            },
            scales: {
                x: {
                    display: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                },
                y: {
                    display: true,
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        callback: function(value) {
                            return ChartUtils.formatNumber(value);
                        }
                    }
                }
            },
            interaction: {
                mode: 'nearest',
                axis: 'x',
                intersect: false
            },
            animation: {
                duration: 1000,
                easing: 'easeInOutQuart'
            }
        }
    },
    
    // Doughnut chart for categorical data
    doughnut: {
        type: 'doughnut',
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 20,
                        usePointStyle: true
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    titleColor: '#fff',
                    bodyColor: '#fff',
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = context.parsed;
                            const total = context.dataset.data.reduce((sum, val) => sum + val, 0);
                            const percentage = ((value / total) * 100).toFixed(1);
                            return `${label}: ${value} (${percentage}%)`;
                        }
                    }
                }
            },
            cutout: '60%',
            animation: {
                animateRotate: true,
                animateScale: false,
                duration: 1000
            }
        }
    },
    
    // Bar chart for comparisons
    bar: {
        type: 'bar',
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    titleColor: '#fff',
                    bodyColor: '#fff'
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false
                    }
                },
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        callback: function(value) {
                            return ChartUtils.formatNumber(value);
                        }
                    }
                }
            },
            animation: {
                duration: 1000,
                easing: 'easeInOutQuart'
            }
        }
    },
    
    // Area chart for volume data
    area: {
        type: 'line',
        options: {
            responsive: true,
            maintainAspectRatio: false,
            fill: true,
            plugins: {
                legend: {
                    display: false
                },
                filler: {
                    propagate: false
                }
            },
            scales: {
                x: {
                    display: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                },
                y: {
                    display: true,
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                }
            },
            elements: {
                line: {
                    tension: 0.4
                },
                point: {
                    radius: 4,
                    hoverRadius: 6
                }
            },
            animation: {
                duration: 1000,
                easing: 'easeInOutQuart'
            }
        }
    }
};

// Chart creation functions
function createTimeSeriesChart(ctx, data, options = {}) {
    const config = { ...ChartConfigs.timeSeriesLine };
    config.data = {
        labels: data.labels,
        datasets: [{
            label: data.label || 'Data',
            data: data.values,
            borderColor: data.color || chartColors.primary,
            backgroundColor: ChartUtils.colorWithOpacity(data.color || chartColors.primary, 0.1),
            borderWidth: 2,
            fill: data.fill || false,
            tension: 0.4,
            pointBackgroundColor: data.color || chartColors.primary,
            pointBorderColor: '#fff',
            pointBorderWidth: 2,
            pointRadius: 3,
            pointHoverRadius: 6
        }]
    };
    
    // Merge custom options
    if (options.scales) {
        config.options.scales = { ...config.options.scales, ...options.scales };
    }
    
    return new Chart(ctx, config);
}

function createDoughnutChart(ctx, data, options = {}) {
    const config = { ...ChartConfigs.doughnut };
    config.data = {
        labels: data.labels,
        datasets: [{
            data: data.values,
            backgroundColor: data.colors || chartColorPalette.slice(0, data.values.length),
            borderColor: '#fff',
            borderWidth: 2,
            hoverBorderWidth: 3
        }]
    };
    
    return new Chart(ctx, config);
}

function createBarChart(ctx, data, options = {}) {
    const config = { ...ChartConfigs.bar };
    config.data = {
        labels: data.labels,
        datasets: [{
            label: data.label || 'Data',
            data: data.values,
            backgroundColor: data.colors || chartColorPalette.slice(0, data.values.length),
            borderColor: data.colors || chartColorPalette.slice(0, data.values.length),
            borderWidth: 1,
            borderRadius: 4,
            borderSkipped: false
        }]
    };
    
    return new Chart(ctx, config);
}

function createAreaChart(ctx, data, options = {}) {
    const config = { ...ChartConfigs.area };
    config.data = {
        labels: data.labels,
        datasets: [{
            label: data.label || 'Data',
            data: data.values,
            borderColor: data.color || chartColors.primary,
            backgroundColor: ChartUtils.createGradient(ctx, 
                ChartUtils.colorWithOpacity(data.color || chartColors.primary, 0.3),
                ChartUtils.colorWithOpacity(data.color || chartColors.primary, 0.05)
            ),
            borderWidth: 2,
            pointBackgroundColor: data.color || chartColors.primary,
            pointBorderColor: '#fff',
            pointBorderWidth: 2,
            pointRadius: 3,
            pointHoverRadius: 6
        }]
    };
    
    return new Chart(ctx, config);
}

// Risk analysis chart
function createRiskAnalysisChart(ctx, riskData) {
    const config = {
        type: 'bar',
        data: {
            labels: ['High Risk', 'Medium Risk', 'Low Risk', 'Unknown'],
            datasets: [{
                label: 'Email Count',
                data: [
                    riskData.high || 0,
                    riskData.medium || 0,
                    riskData.low || 0,
                    riskData.unknown || 0
                ],
                backgroundColor: [
                    chartColors.danger,
                    chartColors.warning,
                    chartColors.success,
                    chartColors.secondary
                ],
                borderRadius: 6,
                borderSkipped: false
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    titleColor: '#fff',
                    bodyColor: '#fff'
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false
                    }
                },
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                }
            }
        }
    };
    
    return new Chart(ctx, config);
}

// Department comparison chart
function createDepartmentChart(ctx, departmentData) {
    const sortedData = departmentData.sort((a, b) => b.count - a.count).slice(0, 8);
    
    const config = {
        type: 'horizontalBar',
        data: {
            labels: sortedData.map(dept => dept.name),
            datasets: [{
                label: 'Email Volume',
                data: sortedData.map(dept => dept.count),
                backgroundColor: chartColorPalette.slice(0, sortedData.length),
                borderRadius: 4,
                borderSkipped: false
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                },
                y: {
                    grid: {
                        display: false
                    }
                }
            }
        }
    };
    
    return new Chart(ctx, config);
}

// Export chart utilities
window.ChartUtils = ChartUtils;
window.ChartConfigs = ChartConfigs;
window.createTimeSeriesChart = createTimeSeriesChart;
window.createDoughnutChart = createDoughnutChart;
window.createBarChart = createBarChart;
window.createAreaChart = createAreaChart;
window.createRiskAnalysisChart = createRiskAnalysisChart;
window.createDepartmentChart = createDepartmentChart;

// Common chart data processing functions
function processTimeSeriesData(rawData, dateField = '_time', valueField = 'count') {
    const processedData = {};
    
    rawData.forEach(record => {
        const date = new Date(record[dateField]).toISOString().split('T')[0];
        processedData[date] = (processedData[date] || 0) + (record[valueField] || 1);
    });
    
    const labels = Object.keys(processedData).sort();
    const values = labels.map(label => processedData[label]);
    
    return { labels, values };
}

function processCategoryData(rawData, categoryField, valueField = null) {
    const categoryCount = {};
    
    rawData.forEach(record => {
        const category = record[categoryField] || 'Unknown';
        if (valueField) {
            categoryCount[category] = (categoryCount[category] || 0) + (record[valueField] || 0);
        } else {
            categoryCount[category] = (categoryCount[category] || 0) + 1;
        }
    });
    
    const labels = Object.keys(categoryCount);
    const values = labels.map(label => categoryCount[label]);
    
    return { labels, values };
}

// Export data processing functions
window.processTimeSeriesData = processTimeSeriesData;
window.processCategoryData = processCategoryData;
