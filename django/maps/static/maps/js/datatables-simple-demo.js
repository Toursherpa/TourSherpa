window.addEventListener('DOMContentLoaded', event => {
    const datatableElements = document.querySelectorAll('table.datatablesSimple');

    datatableElements.forEach(element => {
        new simpleDatatables.DataTable(element);
    });
});
