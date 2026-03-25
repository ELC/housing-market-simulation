var sourceClass = "cm-s-default"
var themeDarkClass = "cm-s-dracula"
var themeLightClass = "cm-s-friendly"
var themeAttribute = "data-theme"

function switchTheme(element, theme) {
    if (theme === "light") {
        element.classList.remove(sourceClass)
        element.classList.remove(themeDarkClass)
        element.classList.add(themeLightClass)
        return
    }
    if (theme === "dark") {
        element.classList.remove(sourceClass)
        element.classList.remove(themeLightClass)
        element.classList.add(themeDarkClass)
        return
    }

}

function updateCodeCellThemes() {
    var selector = `.${sourceClass}, .${themeDarkClass}, .${themeLightClass}`
    var codeCells = Array.from(document.querySelectorAll(selector))
    var rootElement = document.getElementsByTagName("html")[0]

    var theme = rootElement.getAttribute(themeAttribute)

    codeCells.forEach(element => switchTheme(element, theme))
}

function reactToThemeChanges() {
    var observer = new MutationObserver(function (mutations) {
        mutations.forEach(function (mutation) {
            if (mutation.type !== "attributes" && mutation.attributeName !== themeAttribute) {
                return;
            }

            updateCodeCellThemes()
        });
    });

    var rootElement = document.getElementsByTagName("html")[0]
    observer.observe(rootElement, {
        attributes: true
    });
}


function updateGallery() {
    var options = {
        showCloseButton: true
    };

    var images = document.querySelectorAll(".bd-article-container img")

    Array.from(images).forEach(img => {
        var source = img.getAttribute("src")
        var link = `<a href="${source}" target="_blank" class="gallery">${img.outerHTML}</a>`;
        img.outerHTML = link
    })
    new LuminousGallery(document.querySelectorAll(".gallery"), options);
}

function overwriteLaunchButton() {
    var launchButton = document.querySelector('[aria-label="Launch interactive content"]')

    launchButton?.querySelector("svg")?.setAttribute("data-icon", "play")

    launchButton?.addEventListener("click", function () {
        initThebeSBT()
        setTimeout(updateCodeCellThemes, 2000)
    })
}

function cleanupHTML() {
    document.querySelectorAll(".dropdown-menu")[0].remove()
}

function overwriteRunButton(){
    var thebeButtons = Array.from(document.querySelectorAll("thebelab-button"));
    thebeButtons.forEach(button => button.addEventListener("click", function() {
        setTimeout(updateGallery, 5000)
    } ))
}

function setup() {
    reactToThemeChanges()
    cleanupHTML()
    overwriteLaunchButton()
    updateGallery()
}

window.addEventListener("load", setup)
