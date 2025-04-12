document.addEventListener("DOMContentLoaded", function () {
    var navbarObject = document.querySelector(".navbar"); // Get the <object> element

    navbarObject.onload = function () {
        // Access the content inside the object tag
        var navbarDocument = navbarObject.contentDocument || navbarObject.contentWindow.document;

        if (navbarDocument) {
            var currentPage = window.location.pathname.split("/").pop();
            
            var pageMap = {
                "portfolio.html": "portfolio",
                "portfolio-python.html": "portfolio-python",
                "portfolio-azure.html": "portfolio-azure",
                "portfolio-sql.html": "portfolio-sql",
                "portfolio-powerbi.html": "portfolio-powerbi",
                "about-me.html": "about-me",
                "contact.html": "contact",
                "education-experience.html": "education-experience",
            };

            // // Log all IDs inside the navbar
            // var allElements = navbarDocument.querySelectorAll("[id]");
            // allElements.forEach(function (element) {
            //     console.log(`ID: ${element.id}, Content: ${element.innerText}`);
            // });

            // Highlight the active link
            if (pageMap[currentPage]) {
                var activeLink = navbarDocument.getElementById(pageMap[currentPage]);
                if (activeLink) {
                    activeLink.classList.add("active");
                } 
                // else {
                //     console.warn("Element with ID not found:", pageMap[currentPage]);
                // }
            }
        } 
        // else {
        //     console.error("Failed to access navbar content.");
        // }
    };
});
