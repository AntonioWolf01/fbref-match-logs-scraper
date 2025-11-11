# Fbref Top 5 League Match Log Scraper (2025/26 Season)

---

## 📊 Project Overview

This project provides a robust R script designed to scrape comprehensive, match-level statistics for the **2025/26 season** from the top 5 European football leagues. It navigates the Fbref.com website using browser automation, bypasses anti-scraping measures, and systematically downloads, cleans, and aggregates data.

The final output is a single, match-centric CSV file (`data/europa26.csv`) combining base fixture results with advanced metrics from four key statistical categories:

* Shooting
* Passing
* Goalkeeping
* Goal and Shot Creation (GCA)

---

## ⚙️ Technical Implementation Summary

* **Browser Automation & Stealth:** The script leverages the `{selenider}` and `{chromote}` packages to run a visible Chrome browser instance. This is essential for bypassing Cloudflare's bot detection. A custom `apply_stealth` function modifies the browser's `navigator.webdriver` property and user agent to appear as a standard user, preventing blocks.

* **Robust Batch Scraping:** All scraping calls are wrapped in a `safe_scrape_batch` function. This custom wrapper uses a `tryCatch` block to implement a retry-on-failure mechanism (up to 5 attempts with a 30-second wait), ensuring the long-running script can recover from intermittent network errors or page load failures.

* **Advanced Data Cleaning & Reshaping:** Raw data is processed using `{dplyr}`, `{tidyverse}`, and `{janitor}`. This includes:
    * Handling dynamic **column shifts** in Fbref's tables (e.g., for Venue and Opponent columns) using `coalesce()`.
    * Standardizing team names across different datasets using a dedicated `name_mapping` vector and league-specific normalization logic.
    * Reshaping team-centric "For" and "Against" logs into a single, **match-centric row** (HomeTeam/AwayTeam) by merging the data on a composite `MatchID`.

---

## 📦 Data Specifications

### Leagues Covered
* 🏴󠁧󠁢󠁥󠁮󠁧󠁿 **Premier League** (ENG)
* 🇪🇸 **La Liga** (ESP)
* 🇮🇹 **Serie A** (ITA)
* 🇩🇪 **Bundesliga** (GER)
* 🇫🇷 **Ligue 1** (FRA)

### Season
* **2025/2026**

### Key Metric Categories
The final dataset includes home and away stats for the following advanced metrics, scraped from their respective log pages:

* **Fixtures & Results:**
    * `HomeGoals` / `AwayGoals`
    * `Home_xG` / `Away_xG` (Expected Goals)

* **Shooting:**
    * `Sh_Standard` (Total Shots)
    * `SoT_Standard` (Shots on Target)
    * `Dist_Standard` (Average Shot Distance)
    * `PK_Standard` (Penalty Kicks Taken)

* **Passing:**
    * `Cmp_percent_Total` (Pass Completion %)
    * `xA` (Expected Assists)
    * `KP` (Key Passes)
    * `PrgP` (Progressive Passes)
    * `PPA` (Passes into Penalty Area)

* **Goalkeeping:**
    * `PSxG_Performance` (Post-Shot Expected Goals minus Goals Allowed)
    * `Stp_percent_Crosses` (Crosses Stopped %)

* **Creation:**
    * `SCA_SCA_Types` (Shot-Creating Actions)
    * `GCA_GCA_Types` (Goal-Creating Actions)

---

## 🚀 How to Run

1.  **Install Dependencies:** Ensure all required libraries listed at the top of the script are installed. You will also need a working local installation of Google Chrome.
    ```R
    install.packages(c("worldfootballR", "dplyr", "stringi", "RSelenium", "netstat", "xml2", "purrr", "binman", "chromote", "rvest", "selenider", "magrittr", "tidyverse", "glue", "progress", "janitor"))
    ```

2.  **Run the Script:** Open the `Data 25-25 Robust Scraper.R` file in your R environment (like RStudio) and source the entire script.
    ```R
    source("Data 25-25 Robust Scraper.R")
    ```

3.  **Monitor the Browser:** By default (`headless = FALSE`), a new Chrome browser window will open. **Do not close this window.** The script will automatically navigate pages.

4.  **Be Patient:** This process is extremely time-consuming. The script scrapes 4 log types for over 90 teams and then scrapes 5 full league fixture lists. This will likely take **several hours** to complete due to the number of pages and the built-in `Sys.sleep()` pauses designed to avoid rate-limiting.

5.  **Find the Output:** Once the script finishes, a `data` folder will be created in your working directory containing the final `europa26.csv` file.
