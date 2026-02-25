        lucide.createIcons({ attrs: { 'stroke-width': 1.6 } });

        function updateLastUpdated() {
            const el = document.getElementById('last-updated');
            if (!el) return;
            const ts = new Date().toLocaleString('sv-SE', { timeZone: 'Asia/Shanghai' });
            el.innerText = ts;
            el.classList.remove('update-flash');
            void el.offsetWidth;
            el.classList.add('update-flash');
        }

        // Theme Handling
        const btnThemeToggle = document.getElementById('btn-theme-toggle');
        let isDarkMode = localStorage.getItem('theme') === 'dark';

        function applyTheme(dark) {
            if (dark) {
                document.documentElement.classList.add('dark-mode');
                document.body.classList.add('dark-mode');
                if (btnThemeToggle) btnThemeToggle.innerHTML = '<i data-lucide="sun" class="w-4 h-4"></i>';
            } else {
                document.documentElement.classList.remove('dark-mode');
                document.body.classList.remove('dark-mode');
                if (btnThemeToggle) btnThemeToggle.innerHTML = '<i data-lucide="moon" class="w-4 h-4"></i>';
            }
            lucide.createIcons();
        }

        if (btnThemeToggle) {
            btnThemeToggle.addEventListener('click', () => {
                isDarkMode = !isDarkMode;
                localStorage.setItem('theme', isDarkMode ? 'dark' : 'light');
                applyTheme(isDarkMode);
            });
        }

        // Initialize Theme
        applyTheme(isDarkMode);

        updateLastUpdated();
