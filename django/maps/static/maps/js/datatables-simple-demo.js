//datatablesSimple class를 가진 table은 simpleDatatables을 만드는 함수
window.addEventListener('DOMContentLoaded', event => {
    const datatableElements = document.querySelectorAll('table.datatablesSimple');

    datatableElements.forEach(element => {
        new simpleDatatables.DataTable(element);
    });
});
