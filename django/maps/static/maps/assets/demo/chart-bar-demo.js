// Set new default font family and font color to mimic Bootstrap's default styling
Chart.defaults.global.defaultFontFamily = '-apple-system,system-ui,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif';
Chart.defaults.global.defaultFontColor = '#292b2c';

// Bar Chart Example
var ctx = document.getElementById("myBarChart");
var myBarChart = new Chart(ctx, {
    type: 'bar',
    data: {
        labels: {{ categories_data.labels|safe }},
        datasets: [{
            label: "Number of Events",
            backgroundColor: "rgba(255, 99, 132, 0.2)",  // 배경색 설정
            borderColor: "rgba(255, 99, 132, 1)",       // 테두리 색 설정
            borderWidth: 1,
            data: {{ categories_data.datasets.0.data|safe }},
        }],
    },
    options: {
        scales: {
            xAxes: [{
                ticks: {
                    maxTicksLimit: 6
                }
            }],
            yAxes: [{
                ticks: {
                    min: 0,
                    max: 15000,
                    maxTicksLimit: 5
                }
            }],
        },
        legend: {
            display: false
        }
    }
});
