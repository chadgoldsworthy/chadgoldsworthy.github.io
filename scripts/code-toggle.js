function toggleCode() {
    const codeContainer = document.getElementById('code-container');
    const button = document.querySelector('.dropdown-btn');

    if (codeContainer.style.display === 'none' || codeContainer.style.display === '') {
        codeContainer.style.display = 'block';
        button.textContent = 'Hide Python Code';
        Prism.highlightElement(document.getElementById('code-block')); // Apply syntax highlighting
    } else {
        codeContainer.style.display = 'none';
        button.textContent = 'Show Python Code';
    }
}