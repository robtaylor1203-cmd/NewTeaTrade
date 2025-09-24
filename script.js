document.addEventListener('DOMContentLoaded', () => {
    // --- Mobile Menu Toggle Logic ---
    const menuToggle = document.querySelector('.mobile-menu-toggle');
    const secondaryNav = document.querySelector('.secondary-nav');

    if (menuToggle && secondaryNav) {
        menuToggle.addEventListener('click', () => {
            secondaryNav.classList.toggle('is-active');
            const isOpen = secondaryNav.classList.contains('is-active');
            menuToggle.setAttribute('aria-expanded', isOpen);
            // Change the icon when open/closed (Hamburger to X)
            if (isOpen) {
                menuToggle.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg>`;
            } else {
                menuToggle.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="3" y1="12" x2="21" y2="12"></line><line x1="3" y1="6" x2="21" y2="6"></line><line x1="3" y1="18" x2="21" y2="18"></line></svg>`;
            }
        });
    }
});