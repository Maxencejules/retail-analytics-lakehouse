(function () {
  "use strict";

  function normalizeGitHubRepoUrl(value) {
    if (!value) {
      return null;
    }
    var match = value.match(/github\.com\/([^/]+)\/([^/.]+)/i);
    if (!match) {
      return null;
    }
    return { owner: match[1], repo: match[2] };
  }

  function inferRepoFromPagesHost() {
    var host = window.location.hostname.toLowerCase();
    if (!host.endsWith(".github.io")) {
      return null;
    }
    var owner = host.split(".")[0];
    var parts = window.location.pathname.split("/").filter(Boolean);
    if (parts.length < 1) {
      return null;
    }
    return { owner: owner, repo: parts[0] };
  }

  function resolveRepoInfo() {
    var body = document.body;
    return (
      normalizeGitHubRepoUrl(body.dataset.repositoryUrl) || inferRepoFromPagesHost()
    );
  }

  function setDrawioLinks() {
    var viewerLink = document.getElementById("drawio-viewer-link");
    var sourceLink = document.getElementById("drawio-source-link");
    var embed = document.getElementById("drawio-embed");
    if (!viewerLink || !sourceLink) {
      return;
    }

    var repoInfo = resolveRepoInfo();
    if (!repoInfo) {
      viewerLink.style.display = "none";
      sourceLink.style.display = "none";
      var missingNote = document.getElementById("drawio-link-note");
      if (missingNote) {
        missingNote.classList.add("warn");
        missingNote.textContent =
          "Set docs-site/_config.yml repository_url to enable interactive Draw.io links.";
      }
      return;
    }

    var branch = document.body.dataset.defaultBranch || "main";
    var drawioPath = document.body.dataset.drawioPath;
    var rawUrl =
      "https://raw.githubusercontent.com/" +
      repoInfo.owner +
      "/" +
      repoInfo.repo +
      "/" +
      branch +
      "/" +
      drawioPath;
    var viewerUrl =
      "https://viewer.diagrams.net/?lightbox=1&highlight=0A2239&edit=_blank&nav=1&title=lakehouse-architecture.drawio#U" +
      encodeURIComponent(rawUrl);

    viewerLink.href = viewerUrl;
    sourceLink.href = rawUrl;
    if (embed) {
      embed.src = viewerUrl;
    }
  }

  function setLiveDemoLinks() {
    var links = document.querySelectorAll("[data-live-demo-link]");
    var badges = document.querySelectorAll("[data-live-demo-status]");
    if (!links.length) {
      return;
    }

    var params = new URLSearchParams(window.location.search);
    var queryDemo = params.get("demo");
    if (queryDemo) {
      localStorage.setItem("retail_lakehouse_demo_url", queryDemo);
    }

    var configured = document.body.dataset.liveDemoUrl || "";
    var stored = localStorage.getItem("retail_lakehouse_demo_url") || "";
    var url = queryDemo || stored || configured;
    var label = document.body.dataset.liveDemoLabel || "Live Demo";
    var region = document.body.dataset.liveDemoRegion || "us-east-1";
    var isSet = Boolean(url);

    links.forEach(function (link) {
      if (isSet) {
        link.href = url;
        link.target = "_blank";
        link.rel = "noopener";
        link.textContent = label;
      } else {
        link.removeAttribute("href");
        link.classList.add("secondary");
        link.textContent = "Demo URL Not Configured";
      }
    });

    badges.forEach(function (badge) {
      if (isSet) {
        badge.textContent = "Active demo URL is configured for region " + region + ".";
      } else {
        badge.textContent =
          "No demo URL configured. Set live_demo_url in docs-site/_config.yml, or append ?demo=<url>.";
        badge.classList.add("warn");
      }
    });
  }

  function enableApiFilter() {
    var input = document.getElementById("api-filter");
    if (!input) {
      return;
    }
    var rows = Array.from(document.querySelectorAll("[data-api-row]"));
    var empty = document.getElementById("api-filter-empty");

    function apply() {
      var q = input.value.trim().toLowerCase();
      var shown = 0;
      rows.forEach(function (row) {
        var haystack = row.dataset.search || "";
        var visible = !q || haystack.indexOf(q) !== -1;
        row.style.display = visible ? "" : "none";
        if (visible) {
          shown += 1;
        }
      });
      if (empty) {
        empty.style.display = shown === 0 ? "block" : "none";
      }
    }

    input.addEventListener("input", apply);
    apply();
  }

  function setFooterYear() {
    var footer = document.getElementById("footer-year");
    if (!footer) {
      return;
    }
    footer.textContent = "Updated " + new Date().toISOString().slice(0, 10);
  }

  setFooterYear();
  setLiveDemoLinks();
  setDrawioLinks();
  enableApiFilter();
})();
