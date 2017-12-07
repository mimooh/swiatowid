var ctx = document.getElementById('swiatowid');
var myChart = new Chart(ctx, {
    type: 'bar',
    data: {
        labels: [ "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy", "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy", "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" , "Krasuski", "Kreński", "Łazowy" ],
        datasets: [{
            label: 'wyniki',
            data: [ 21,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3 , 21,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3 , 21,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3, 12,  3 ],
            backgroundColor: [
                'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)', 'rgba(255, 99, 132, 0.2)'
            ],
            borderColor: [
                'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)', 'rgba(255,99,132,1)'
            ],
            borderWidth: 1
        }]
    },
    options: {
        scales: {
            yAxes: [{
                ticks: {
                    beginAtZero:true
                }
            }]
        }
    }
});

